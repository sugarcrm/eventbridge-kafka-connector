/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.batch;

import static ch.qos.logback.classic.Level.TRACE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static software.amazon.event.kafkaconnector.TestUtils.toFixedGitCommitId;
import static software.amazon.event.kafkaconnector.batch.DefaultEventBridgeBatching.getSize;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.eventbridge.model.PutPartnerEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutPartnerEventsRequestEntry.Builder;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

class DefaultEventBridgeBatchingTest {

  private static final EventBridgeBatchingStrategy strategy = new DefaultEventBridgeBatching();

  private static List<ILoggingEvent> loggingEvents;

  @BeforeAll
  public static void setup() {
    var appender = new ListAppender<ILoggingEvent>();
    appender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
    appender.start();
    var logger = (Logger) LoggerFactory.getLogger(DefaultEventBridgeBatching.class);
    logger.setLevel(TRACE);
    logger.addAppender(appender);
    loggingEvents = appender.list;
  }

  @AfterEach
  public void clearLoggingEvents() {
    loggingEvents.clear();
  }

  @Nested
  @DisplayName("getSize(PutPartnerEventsRequestEntry)")
  class Size {

    @Test
    @DisplayName("requires attribute source")
    public void requiresAttributeSource() {
      assertThrows(
          NullPointerException.class,
          () -> getSize(PutPartnerEventsRequestEntry.builder().detailType("detailType").build()));
      assertThat(
              getSize(
                  PutPartnerEventsRequestEntry.builder()
                      .source("source")
                      .detailType("detailType")
                      .build()))
          .isGreaterThan(0);
    }

    @Test
    @DisplayName("requires attribute detailType")
    public void requiresAttributeDetailType() {
      assertThrows(
          NullPointerException.class,
          () -> getSize(PutPartnerEventsRequestEntry.builder().source("source").build()));
      assertThat(
              getSize(
                  PutPartnerEventsRequestEntry.builder()
                      .source("source")
                      .detailType("detailType")
                      .build()))
          .isGreaterThan(0);
    }

    @Test
    @DisplayName("attribute time contributes 14 (bytes)")
    public void attributeTime() {
      assertThat(
              getSize(
                  PutPartnerEventsRequestEntry.builder()
                      .time(Instant.now())
                      .source("")
                      .detailType("")
                      .build()))
          .isEqualTo(14);
    }

    @ParameterizedTest(name = "attribute: {0} with «{1}»")
    @DisplayName("attribute contributes with used UTF-8 bytes")
    @MethodSource(
        "software.amazon.event.kafkaconnector.batch.DefaultEventBridgeBatchingTest#attributeContributionArguments")
    public void attributeContribution(String attribute, String value) {

      final Builder entry = PutPartnerEventsRequestEntry.builder().source("").detailType("");
      switch (attribute) {
        case "source":
          entry.source(value);
          break;
        case "detailType":
          entry.detailType(value);
          break;
        case "detail":
          entry.detail(value);
          break;
        case "resource":
          entry.resources(value);
          break;
      }

      assertThat(getSize(entry.build())).isEqualTo(value.getBytes(UTF_8).length);
    }

    @ParameterizedTest(name = "{0} times")
    @DisplayName("each resource contributes with it's size")
    @ValueSource(ints = {0, 1, 2, 4, 8, 16})
    public void multipleResource(int size) {
      assertThat(
              getSize(
                  PutPartnerEventsRequestEntry.builder()
                      .source("")
                      .detailType("")
                      .resources(
                          Stream.generate(() -> "\uD83D\uDC4D").limit(size).collect(toList()))
                      .build()))
          .isEqualTo(size * "\uD83D\uDC4D".getBytes(UTF_8).length);
    }

    @ParameterizedTest(name = "of {0} bytes")
    @DisplayName("generated test 'PutPartnerEventsRequestEntry' object should have expected size")
    @ValueSource(ints = {1024, 10 * 1024, 100 * 1024, 256 * 1024})
    public void shouldHaveExpectedSize(int size) {
      assertThat(getSize(createEntryOfByteSize(size, source("any")))).isEqualTo(size);
    }
  }

  @ParameterizedTest(name = "expected batches {1}")
  @DisplayName("should generate batches either up to 256kb or maximum 10 items")
  @MethodSource("batchingArguments")
  public void shouldGenerateBatches(
      Stream<MappedSinkRecord<PutPartnerEventsRequestEntry>> records,
      Iterable<List<String>> expected) {
    assertThat(strategy.apply(records))
        .extracting(it -> it.stream().map(e -> e.getValue().source()).collect(toList()))
        .asList()
        .containsExactlyElementsOf(expected);
  }

  @ParameterizedTest(name = "with entry sizes {1} and expected batches {2}")
  @DisplayName(
      "should generate batches with single record for the item wich exceeds maximum size of 256kb")
  @MethodSource("sizeExceedingBatchingArguments")
  public void shouldGenerateBatchesWithSingleRecord(
      Stream<MappedSinkRecord<PutPartnerEventsRequestEntry>> records,
      String title,
      Iterable<List<String>> expected) {
    assertThat(strategy.apply(records))
        .extracting(it -> it.stream().map(e -> e.getValue().source()).collect(toList()))
        .asList()
        .containsExactlyElementsOf(expected);
  }

  @Test
  @DisplayName("should log WARN message if single record exceeds maximum size of 256kb")
  public void shouldLogWarnMessage() {
    var sinkRecord = new SinkRecord("topic", 0, STRING_SCHEMA, "key", null, "", 0);
    final Stream<MappedSinkRecord<PutPartnerEventsRequestEntry>> records =
        Stream.of(
            new MappedSinkRecord<>(sinkRecord, createEntryOfByteSize(256 * 1024 + 1, source("b"))));

    assertThat(strategy.apply(records)).isNotEmpty();
    assertThat(loggingEvents)
        .extracting(Object::toString)
        .map(toFixedGitCommitId)
        .contains(
            "[WARN] [GitCommitId] Item for SinkRecord with topic='topic', partition=0 and offset=0 exceeds EventBridge size limit. Size is 262145 bytes.");
  }

  @Test
  @DisplayName("stream must not be parallel")
  public void shouldNotBeParallel() {
    final Stream<MappedSinkRecord<PutPartnerEventsRequestEntry>> records =
        Stream.of(
                new MappedSinkRecord<>(null, createEntryOfByteSize(32, source("a"))),
                new MappedSinkRecord<>(null, createEntryOfByteSize(32, source("b"))))
            .parallel();

    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> strategy.apply(records));
    assertThat(exception).hasMessage("Stream must not be parallel.");
  }

  public static Stream<Arguments> attributeContributionArguments() {
    return Stream.of("source", "detailType", "detail", "resource")
        .flatMap(
            attribute ->
                Stream.of("", "!", "µ", "Ⅲ", "\uD83D\uDC4D")
                    .map(value -> Arguments.of(attribute, value)));
  }

  public static Stream<Arguments> batchingArguments() {
    return Stream.of(
        Arguments.of(Stream.empty(), emptyList()),
        Arguments.of(
            Stream.of(new MappedSinkRecord<>(null, createEntryOfByteSize(1024, source("a")))),
            List.of(List.of("a"))),
        Arguments.of(
            Stream.generate(
                    () -> new MappedSinkRecord<>(null, createEntryOfByteSize(1024, source("a"))))
                .limit(11),
            List.of(List.of("a", "a", "a", "a", "a", "a", "a", "a", "a", "a"), List.of("a"))),
        Arguments.of(
            Stream.of(
                new MappedSinkRecord<>(null, createEntryOfByteSize(255 * 1024, source("a"))),
                new MappedSinkRecord<>(null, createEntryOfByteSize(1024, source("b"))),
                new MappedSinkRecord<>(null, createEntryOfByteSize(1024, source("c")))),
            List.of(List.of("a", "b"), List.of("c"))),
        Arguments.of(
            Stream.of(
                new MappedSinkRecord<>(null, createEntryOfByteSize(256 * 1024, source("a"))),
                new MappedSinkRecord<>(null, createEntryOfByteSize(1024, source("b"))),
                new MappedSinkRecord<>(null, createEntryOfByteSize(1024, source("c"))),
                new MappedSinkRecord<>(null, createEntryOfByteSize(256 * 1024, source("d")))),
            List.of(List.of("a"), List.of("b", "c"), List.of("d"))));
  }

  public static Stream<Arguments> sizeExceedingBatchingArguments() {
    var sinkRecord = new SinkRecord("topic", 0, STRING_SCHEMA, "key", null, "", 0);
    return Stream.of(
        Arguments.of(
            Stream.of(
                new MappedSinkRecord<>(
                    sinkRecord, createEntryOfByteSize(256 * 1024 + 1, source("a"))),
                new MappedSinkRecord<>(null, createEntryOfByteSize(1024, source("b"))),
                new MappedSinkRecord<>(null, createEntryOfByteSize(1024, source("c")))),
            "✗✓✓",
            List.of(List.of("a"), List.of("b", "c"))),
        Arguments.of(
            Stream.of(
                new MappedSinkRecord<>(null, createEntryOfByteSize(1024, source("a"))),
                new MappedSinkRecord<>(
                    sinkRecord, createEntryOfByteSize(256 * 1024 + 1, source("b"))),
                new MappedSinkRecord<>(null, createEntryOfByteSize(1024, source("c")))),
            "✓✗✓",
            List.of(List.of("a"), List.of("b"), List.of("c"))),
        Arguments.of(
            Stream.of(
                new MappedSinkRecord<>(null, createEntryOfByteSize(1024, source("a"))),
                new MappedSinkRecord<>(null, createEntryOfByteSize(1024, source("b"))),
                new MappedSinkRecord<>(
                    sinkRecord, createEntryOfByteSize(256 * 1024 + 1, source("c")))),
            "✓✓✗",
            List.of(List.of("a", "b"), List.of("c"))));
  }

  static String source(String value) {
    return value;
  }

  static PutPartnerEventsRequestEntry createEntryOfByteSize(int size, String source) {
    if (size < 31) throw new IllegalArgumentException("size must be >= 30");
    return PutPartnerEventsRequestEntry.builder()
        .time(Instant.now())
        .source(source)
        .detailType("detailType")
        .detail(
            "0"
                .repeat(
                    size
                        - 14 /*time*/
                        - source.length() /*UTF-8 bytes of source*/
                        - 10 /* UTF-8 bytes of detailType*/))
        .build();
  }
}

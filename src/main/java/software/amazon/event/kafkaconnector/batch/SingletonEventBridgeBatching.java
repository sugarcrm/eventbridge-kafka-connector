/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.batch;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import software.amazon.awssdk.services.eventbridge.model.PutPartnerEventsRequestEntry;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

public class SingletonEventBridgeBatching implements EventBridgeBatchingStrategy {
  @Override
  public Stream<List<MappedSinkRecord<PutPartnerEventsRequestEntry>>> apply(
      Stream<MappedSinkRecord<PutPartnerEventsRequestEntry>> records) {
    return records.map(Collections::singletonList);
  }
}

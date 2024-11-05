/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.offloading;

import java.util.List;
import software.amazon.awssdk.services.eventbridge.model.PutPartnerEventsRequestEntry;
import software.amazon.event.kafkaconnector.mapping.EventBridgeMappingResult;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

@FunctionalInterface
public interface EventBridgeEventDetailValueOffloadingStrategy {

  EventBridgeMappingResult apply(
      List<MappedSinkRecord<PutPartnerEventsRequestEntry>> putEventsRequestEntries);
}

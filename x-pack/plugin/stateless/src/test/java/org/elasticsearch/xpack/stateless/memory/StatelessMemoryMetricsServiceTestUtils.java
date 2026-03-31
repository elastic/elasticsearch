/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.common.unit.ByteSizeValue;

public class StatelessMemoryMetricsServiceTestUtils {

    private StatelessMemoryMetricsServiceTestUtils() {}

    public static ByteSizeValue getFixedShardMemoryOverhead(StatelessMemoryMetricsService service) {
        return service.getFixedShardMemoryOverhead();
    }

    public static long getLastMaxTotalPostingsInMemoryBytes(StatelessMemoryMetricsService service) {
        return service.getLastMaxTotalPostingsInMemoryBytes();
    }

    public static StatelessMemoryMetricsService.ShardMemoryMetrics newUninitialisedShardMemoryMetrics(
        StatelessMemoryMetricsService service,
        long updateTimestampNanos
    ) {
        return service.newUninitialisedShardMemoryMetrics(updateTimestampNanos);
    }
}

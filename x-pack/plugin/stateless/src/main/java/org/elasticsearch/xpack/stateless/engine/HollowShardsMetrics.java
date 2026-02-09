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

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.function.LongSupplier;

public record HollowShardsMetrics(
    MeterRegistry meterRegistry,
    LongCounter hollowSuccessCounter,
    LongCounter unhollowSuccessCounter,
    LongHistogram hollowTimeMs,
    LongHistogram unhollowTimeMs,
    LongUpDownCounter hollowShards
) {

    public static final HollowShardsMetrics NOOP = HollowShardsMetrics.from(MeterRegistry.NOOP, () -> 0L);
    public static final String HOLLOW_SUCCESS_TOTAL = "es.hollow_shards_metrics.hollow.success.total";
    public static final String HOLLOW_TIME_MILLIS = "es.hollow_shards_metrics.hollow.time";
    public static final String UNHOLLOW_SUCCESS_TOTAL = "es.hollow_shards_metrics.unhollow.success.total";
    public static final String UNHOLLOW_TIME_MILLIS = "es.hollow_shards_metrics.unhollow.time";
    public static final String HOLLOW_SHARDS_TOTAL = "es.hollow_shards_metrics.hollow_shards.total";
    public static final String HOLLOWABLE_SHARDS_TOTAL = "es.hollow_shards_metrics.hollowable_shards.total";

    public static HollowShardsMetrics from(MeterRegistry meterRegistry, LongSupplier amountOfHollowableShardsSupplier) {
        var hollowSuccessCounter = meterRegistry.registerLongCounter(
            HOLLOW_SUCCESS_TOTAL,
            "The number of times a shard was succesfully hollowed",
            "count"
        );
        var unhollowSuccessCounter = meterRegistry.registerLongCounter(
            UNHOLLOW_SUCCESS_TOTAL,
            "The number of times a shard was succesfully unhollowed",
            "count"
        );
        var hollowTime = meterRegistry.registerLongHistogram(HOLLOW_TIME_MILLIS, "Time spent on hollowing shards", "ms");
        var unhollowTime = meterRegistry.registerLongHistogram(UNHOLLOW_TIME_MILLIS, "Time spent on unhollowing shards", "ms");
        var hollowShards = meterRegistry.registerLongUpDownCounter(
            HOLLOW_SHARDS_TOTAL,
            "Amount of the current hollow shards on the node",
            "count"
        );
        meterRegistry.registerLongGauge(
            HOLLOWABLE_SHARDS_TOTAL,
            "Amount of the current hollowable shards on the node",
            "count",
            () -> new LongWithAttributes(amountOfHollowableShardsSupplier.getAsLong())
        );
        return new HollowShardsMetrics(meterRegistry, hollowSuccessCounter, unhollowSuccessCounter, hollowTime, unhollowTime, hollowShards);
    }

    public void incrementHollowShardCount() {
        hollowShards.add(1);
    }

    public void decrementHollowShardCount() {
        hollowShards.add(-1);
    }
}

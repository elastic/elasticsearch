/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams.autosharding;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataStats;
import org.elasticsearch.cluster.metadata.IndexWriteLoad;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexingStats;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.elasticsearch.action.datastreams.autosharding.AutoShardingResult.NOT_APPLICABLE_RESULT;

/**
 * Calculates the optimal number of shards the data stream write index should have based on the indexing load.
 */
public class DataStreamAutoShardingService {

    private static final Logger logger = LogManager.getLogger(DataStreamAutoShardingService.class);
    public static final String DATA_STREAMS_AUTO_SHARDING_ENABLED = "data_streams.auto_sharding.enabled";

    public static final Setting<List<String>> DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING = Setting.listSetting(
        "data_streams.auto_sharding.excludes",
        List.of(),
        Function.identity(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Represents the minimum amount of time between two scaling events if the next event will increase the number of shards.
     * We've chosen a value of 4.5minutes by default, just lower than the data stream lifecycle poll interval so we can increase shards with
     * every DSL run, but we don't want it to be lower/0 as data stream lifecycle might run more often than the poll interval in case of
     * a master failover.
     */
    public static final Setting<TimeValue> DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_COOLDOWN = Setting.timeSetting(
        "data_streams.auto_sharding.increase_shards.cooldown",
        TimeValue.timeValueSeconds(270),
        TimeValue.timeValueSeconds(0),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Represents the minimum amount of time between two scaling events if the next event will reduce the number of shards.
     */
    public static final Setting<TimeValue> DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_COOLDOWN = Setting.timeSetting(
        "data_streams.auto_sharding.decrease_shards.cooldown",
        TimeValue.timeValueDays(3),
        TimeValue.timeValueSeconds(0),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Represents the minimum number of write threads we expect a node to have in the environments where auto sharding will be enabled.
     */
    public static final Setting<Integer> CLUSTER_AUTO_SHARDING_MIN_WRITE_THREADS = Setting.intSetting(
        "cluster.auto_sharding.min_write_threads",
        2,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Represents the maximum number of write threads we expect a node to have in the environments where auto sharding will be enabled.
     */
    public static final Setting<Integer> CLUSTER_AUTO_SHARDING_MAX_WRITE_THREADS = Setting.intSetting(
        "cluster.auto_sharding.max_write_threads",
        32,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Enumerates the different ways of measuring write load which we can choose between to use in the auto-sharding calculations.
     */
    public enum WriteLoadMetric {

        /**
         * An unweighted average of the load across the whole time since each shard started (see
         * {@link IndexingStats.Stats#getWriteLoad()}).
         */
        ALL_TIME,

        /**
         * A weighted average of the load favoring recent load (see {@link IndexingStats.Stats#getRecentWriteLoad()}).
         */
        RECENT,

        /**
         * A measure of the peak value observed for the {@link #RECENT} metric (see {@link IndexingStats.Stats#getPeakWriteLoad()}).
         */
        PEAK
    }

    /**
     * Represents which write load metric should be used for the calculation when considering increasing shards.
     */
    public static final Setting<WriteLoadMetric> DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_LOAD_METRIC = Setting.enumSetting(
        WriteLoadMetric.class,
        "data_streams.auto_sharding.increase_shards.load_metric",
        WriteLoadMetric.ALL_TIME,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Represents which write load metric should be used for the calculation when considering decreasing shards.
     */
    public static final Setting<WriteLoadMetric> DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_LOAD_METRIC = Setting.enumSetting(
        WriteLoadMetric.class,
        "data_streams.auto_sharding.decrease_shards.load_metric",
        WriteLoadMetric.ALL_TIME,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final ClusterService clusterService;
    private final boolean isAutoShardingEnabled;
    private final LongSupplier nowSupplier;
    private volatile TimeValue increaseShardsCooldown;
    private volatile TimeValue reduceShardsCooldown;
    private volatile int minWriteThreads;
    private volatile int maxWriteThreads;
    private volatile List<String> dataStreamExcludePatterns;
    private volatile WriteLoadMetric increaseShardsMetric;
    private volatile WriteLoadMetric decreaseShardsMetric;

    public DataStreamAutoShardingService(Settings settings, ClusterService clusterService, LongSupplier nowSupplier) {
        this.clusterService = clusterService;
        this.isAutoShardingEnabled = settings.getAsBoolean(DATA_STREAMS_AUTO_SHARDING_ENABLED, false);
        this.increaseShardsCooldown = DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_COOLDOWN.get(settings);
        this.reduceShardsCooldown = DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_COOLDOWN.get(settings);
        this.minWriteThreads = CLUSTER_AUTO_SHARDING_MIN_WRITE_THREADS.get(settings);
        this.maxWriteThreads = CLUSTER_AUTO_SHARDING_MAX_WRITE_THREADS.get(settings);
        this.dataStreamExcludePatterns = DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING.get(settings);
        this.increaseShardsMetric = DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_LOAD_METRIC.get(settings);
        this.decreaseShardsMetric = DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_LOAD_METRIC.get(settings);
        this.nowSupplier = nowSupplier;
    }

    public void init() {
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_COOLDOWN, this::updateIncreaseShardsCooldown);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_COOLDOWN, this::updateReduceShardsCooldown);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLUSTER_AUTO_SHARDING_MIN_WRITE_THREADS, this::updateMinWriteThreads);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLUSTER_AUTO_SHARDING_MAX_WRITE_THREADS, this::updateMaxWriteThreads);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING, this::updateDataStreamExcludePatterns);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_LOAD_METRIC, this::updateIncreaseShardsMetric);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_LOAD_METRIC, this::updateDecreaseShardsMetric);
    }

    /**
     * Computes the optimal number of shards for the provided data stream according to the write index's indexing load (to check if we must
     * increase the number of shards, whilst the heuristics for decreasing the number of shards _might_ use the provided write indexing
     * load).
     *
     * <p>The result type will indicate the recommendation of the auto sharding service:
     * <ul>
     * <li>not applicable if the data stream is excluded from auto sharding as configured by
     * {@link #DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING} or if the auto sharding functionality is disabled according to
     * {@link #DATA_STREAMS_AUTO_SHARDING_ENABLED}, or if the write index's indexing load is not available
     * <li>increase number of shards if the optimal number of shards it deems necessary for the provided data stream is GT the current
     * number of shards
     * <li>decrease the number of shards if the optimal number of shards it deems necessary for the provided data stream is LT the current
     * number of shards
     * </ul>
     *
     * <p>If the recommendation is to INCREASE/DECREASE shards the reported cooldown period will be TimeValue.ZERO.
     * If the auto sharding service thinks the number of shards must be changed but it can't recommend a change due to the cooldown
     * period not lapsing, the result will be of type {@link AutoShardingType#COOLDOWN_PREVENTED_INCREASE} or
     * {@link AutoShardingType#COOLDOWN_PREVENTED_INCREASE} with the remaining cooldown configured and the number of shards that should
     * be configured for the data stream once the remaining cooldown lapses as the target number of shards.
     *
     * <p>The NOT_APPLICABLE type result will report a cooldown period of TimeValue.MAX_VALUE.
     *
     * <p>The NO_CHANGE_REQUIRED type will potentially report the remaining cooldown always report a cool down period of TimeValue.ZERO (as
     * there'll be no new auto sharding event)
     */
    public AutoShardingResult calculate(ProjectState state, DataStream dataStream, @Nullable IndexStats writeIndexStats) {
        if (isAutoShardingEnabled == false) {
            logger.debug("Data stream auto-sharding service is not enabled.");
            return NOT_APPLICABLE_RESULT;
        }

        if (dataStreamExcludePatterns.stream().anyMatch(pattern -> Regex.simpleMatch(pattern, dataStream.getName()))) {
            logger.debug(
                "Data stream [{}] is excluded from auto-sharding via the [{}] setting",
                dataStream.getName(),
                DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING.getKey()
            );
            return NOT_APPLICABLE_RESULT;
        }

        if (writeIndexStats == null) {
            logger.debug(
                "Data stream auto-sharding service cannot compute the optimal number of shards for data stream [{}] as the write index "
                    + "stats are not available",
                dataStream.getName()
            );
            return NOT_APPLICABLE_RESULT;
        }

        double writeIndexLoad = sumLoadMetrics(writeIndexStats, IndexingStats.Stats::getWriteLoad);
        double writeIndexRecentLoad = sumLoadMetrics(writeIndexStats, IndexingStats.Stats::getRecentWriteLoad);
        double writeIndexPeakLoad = sumLoadMetrics(writeIndexStats, IndexingStats.Stats::getPeakWriteLoad);
        double writeIndexLoadForIncrease = pickMetric(increaseShardsMetric, writeIndexLoad, writeIndexRecentLoad, writeIndexPeakLoad);
        double writeIndexLoadForDecrease = pickMetric(decreaseShardsMetric, writeIndexLoad, writeIndexRecentLoad, writeIndexPeakLoad);

        logger.trace(
            "Data stream auto-sharding service calculating recommendation with all-time load {}, recent load {}, peak load {}, "
                + "using {} for increase and {} for decrease",
            writeIndexLoad,
            writeIndexRecentLoad,
            writeIndexPeakLoad,
            increaseShardsMetric,
            decreaseShardsMetric
        );

        return innerCalculate(state.metadata(), dataStream, writeIndexLoadForIncrease, writeIndexLoadForDecrease, nowSupplier);
    }

    private static double sumLoadMetrics(IndexStats stats, Function<IndexingStats.Stats, Double> loadMetric) {
        return Arrays.stream(stats.getShards())
            .filter(shardStats -> shardStats.getStats().indexing != null)
            // only take primaries into account as in stateful the replicas also index data
            .filter(shardStats -> shardStats.getShardRouting().primary())
            .map(shardStats -> shardStats.getStats().indexing.getTotal())
            .map(loadMetric)
            .reduce(0.0, Double::sum);
    }

    private AutoShardingResult innerCalculate(
        ProjectMetadata project,
        DataStream dataStream,
        double writeIndexLoadForIncrease,
        double writeIndexLoadForDecrease,
        LongSupplier nowSupplier
    ) {
        // increasing the number of shards is calculated solely based on the index load of the write index
        IndexMetadata writeIndex = project.index(dataStream.getWriteIndex());
        assert writeIndex != null : "the data stream write index must exist in the provided cluster metadata";
        AutoShardingResult increaseShardsResult = getIncreaseShardsResult(dataStream, writeIndexLoadForIncrease, nowSupplier, writeIndex);
        return Objects.requireNonNullElseGet(
            increaseShardsResult,
            () -> getDecreaseShardsResult(
                project,
                dataStream,
                writeIndexLoadForDecrease,
                nowSupplier,
                writeIndex,
                getRemainingDecreaseShardsCooldown(project, dataStream)
            )
        );

    }

    @Nullable
    private AutoShardingResult getIncreaseShardsResult(
        DataStream dataStream,
        double writeIndexLoadForIncrease,
        LongSupplier nowSupplier,
        IndexMetadata writeIndex
    ) {
        // increasing the number of shards is calculated solely based on the index load of the write index
        long optimalShardCount = computeOptimalNumberOfShards(minWriteThreads, maxWriteThreads, writeIndexLoadForIncrease);
        logger.trace(
            "Calculated the optimal number of shards for a potential increase in number of shards for data stream [{}] as [{}]"
                + " with the {} indexing load [{}] for the write index assuming [{}-{}] threads per shard",
            dataStream.getName(),
            optimalShardCount,
            increaseShardsMetric,
            writeIndexLoadForIncrease,
            minWriteThreads,
            maxWriteThreads
        );
        if (optimalShardCount > writeIndex.getNumberOfShards()) {
            TimeValue timeSinceLastAutoShardingEvent = dataStream.getAutoShardingEvent() != null
                ? dataStream.getAutoShardingEvent().getTimeSinceLastAutoShardingEvent(nowSupplier)
                : TimeValue.MAX_VALUE;

            TimeValue coolDownRemaining = TimeValue.timeValueMillis(
                Math.max(0L, increaseShardsCooldown.millis() - timeSinceLastAutoShardingEvent.millis())
            );
            logger.debug(
                "Data stream auto-sharding service recommends increasing the number of shards from [{}] to [{}] after [{}] cooldown for "
                    + "data stream [{}]",
                writeIndex.getNumberOfShards(),
                optimalShardCount,
                coolDownRemaining,
                dataStream.getName()
            );
            return new AutoShardingResult(
                coolDownRemaining.equals(TimeValue.ZERO) ? AutoShardingType.INCREASE_SHARDS : AutoShardingType.COOLDOWN_PREVENTED_INCREASE,
                writeIndex.getNumberOfShards(),
                Math.toIntExact(optimalShardCount),
                coolDownRemaining,
                writeIndexLoadForIncrease
            );
        }
        return null;
    }

    /**
     * Calculates the amount of time remaining before we can consider reducing the number of shards.
     * This reference for the remaining time math is either the time since the last auto sharding event (if available) or otherwise the
     * oldest index in the data stream.
     */
    private TimeValue getRemainingDecreaseShardsCooldown(ProjectMetadata project, DataStream dataStream) {
        Index oldestBackingIndex = dataStream.getIndices().get(0);
        IndexMetadata oldestIndexMeta = project.getIndexSafe(oldestBackingIndex);

        return dataStream.getAutoShardingEvent() == null
            // without a pre-existing auto sharding event we wait until the oldest index has been created longer than the decrease_shards
            // cool down period "ago" so we don't immediately reduce the number of shards after a data stream is created
            ? TimeValue.timeValueMillis(
                Math.max(0L, oldestIndexMeta.getCreationDate() + reduceShardsCooldown.millis() - nowSupplier.getAsLong())
            )
            : TimeValue.timeValueMillis(
                Math.max(
                    0L,
                    reduceShardsCooldown.millis() - dataStream.getAutoShardingEvent()
                        .getTimeSinceLastAutoShardingEvent(nowSupplier)
                        .millis()
                )
            );
    }

    private AutoShardingResult getDecreaseShardsResult(
        ProjectMetadata project,
        DataStream dataStream,
        double writeIndexLoadForDecrease,
        LongSupplier nowSupplier,
        IndexMetadata writeIndex,
        TimeValue remainingReduceShardsCooldown
    ) {
        double maxIndexLoadWithinCoolingPeriod = getMaxIndexLoadWithinCoolingPeriod(
            project,
            dataStream,
            writeIndexLoadForDecrease,
            reduceShardsCooldown,
            nowSupplier,
            decreaseShardsMetric
        );

        long optimalShardCount = computeOptimalNumberOfShards(minWriteThreads, maxWriteThreads, maxIndexLoadWithinCoolingPeriod);
        logger.trace(
            "Calculated the optimal number of shards for a potential decrease in number of shards for data stream [{}] as [{}]"
                + " shards, using a max {} indexing load [{}] over the cool down period [{}] assuming [{}-{}] threads per shard",
            dataStream.getName(),
            optimalShardCount,
            decreaseShardsMetric,
            maxIndexLoadWithinCoolingPeriod,
            reduceShardsCooldown,
            minWriteThreads,
            maxWriteThreads
        );
        if (optimalShardCount < writeIndex.getNumberOfShards()) {
            logger.debug(
                "data stream auto-sharding service recommends decreasing the number of shards from [{}] to [{}] after [{}] cooldown for "
                    + "data stream [{}]",
                writeIndex.getNumberOfShards(),
                optimalShardCount,
                remainingReduceShardsCooldown,
                dataStream.getName()
            );

            // we should reduce the number of shards
            return new AutoShardingResult(
                remainingReduceShardsCooldown.equals(TimeValue.ZERO)
                    ? AutoShardingType.DECREASE_SHARDS
                    : AutoShardingType.COOLDOWN_PREVENTED_DECREASE,
                writeIndex.getNumberOfShards(),
                Math.toIntExact(optimalShardCount),
                remainingReduceShardsCooldown,
                maxIndexLoadWithinCoolingPeriod
            );
        }

        logger.trace(
            "data stream auto-sharding service recommends maintaining the number of shards [{}] for data stream [{}]",
            writeIndex.getNumberOfShards(),
            dataStream.getName()
        );
        return new AutoShardingResult(
            AutoShardingType.NO_CHANGE_REQUIRED,
            writeIndex.getNumberOfShards(),
            writeIndex.getNumberOfShards(),
            TimeValue.ZERO,
            maxIndexLoadWithinCoolingPeriod
        );
    }

    // Visible for testing
    static long computeOptimalNumberOfShards(int minNumberWriteThreads, int maxNumberWriteThreads, double indexingLoad) {
        /*
         * Define:
         *  - shardsByMaxThreads = number of shards required to ensure no more than 50% utilization with max number of threads per shard
         *  - shardsByMinThreads = number of shards required to ensure no more than 50% utilization with min number of threads per shard
         * Note that shardsByMaxThreads <= shardsByMinThreads.
         * This returns:
         *  - shardsByMaxThreads if shardsByMaxThreads > 3
         *  - 3 if shardsByMaxThreads <= 3 and shardsByMinThreads > 3
         *  - shardsByMinThreads if 0 < shardsByMinThreads <= 3
         *  - 1 if shardsByMinThreads == 0
         */
        return Math.max(
            Math.max(
                Math.min(roundUp(indexingLoad / (minNumberWriteThreads / 2.0)), 3),
                roundUp(indexingLoad / (maxNumberWriteThreads / 2.0))
            ),
            1 // we don't want to go lower than 1 shard
        );
    }

    private static long roundUp(double value) {
        return (long) Math.ceil(value);
    }

    // Visible for testing
    /**
     * Calculates the maximum write index load observed for the provided data stream across all the backing indices that were created
     * during the provide {@param coolingPeriod} (note: to cover the entire cooling period, the backing index created before the cooling
     * period is also considered).
     */
    static double getMaxIndexLoadWithinCoolingPeriod(
        ProjectMetadata project,
        DataStream dataStream,
        double writeIndexLoadForDecrease,
        TimeValue coolingPeriod,
        LongSupplier nowSupplier,
        WriteLoadMetric decreaseShardsMetric
    ) {
        // for reducing the number of shards we look at more than just the write index
        List<IndexWriteLoad> writeLoadsWithinCoolingPeriod = DataStream.getIndicesWithinMaxAgeRange(
            dataStream,
            project::getIndexSafe,
            coolingPeriod,
            nowSupplier
        )
            .stream()
            .filter(index -> index.equals(dataStream.getWriteIndex()) == false)
            .map(project::index)
            .filter(Objects::nonNull)
            .map(IndexMetadata::getStats)
            .filter(Objects::nonNull)
            .map(IndexMetadataStats::writeLoad)
            .filter(Objects::nonNull)
            .toList();

        // assume the current write index load is the highest observed and look back to find the actual maximum
        double maxIndexLoadWithinCoolingPeriod = writeIndexLoadForDecrease;
        for (IndexWriteLoad writeLoad : writeLoadsWithinCoolingPeriod) {
            double totalIndexLoad = 0;
            for (int shardId = 0; shardId < writeLoad.numberOfShards(); shardId++) {
                Double writeLoadForShard = pickMetric(
                    decreaseShardsMetric,
                    optionalDoubleToNullable(writeLoad.getWriteLoadForShard(shardId)),
                    optionalDoubleToNullable(writeLoad.getRecentWriteLoadForShard(shardId)),
                    optionalDoubleToNullable(writeLoad.getPeakWriteLoadForShard(shardId))
                );
                if (writeLoadForShard != null) {
                    totalIndexLoad += writeLoadForShard;
                }
            }
            if (totalIndexLoad > maxIndexLoadWithinCoolingPeriod) {
                maxIndexLoadWithinCoolingPeriod = totalIndexLoad;
            }
        }
        return maxIndexLoadWithinCoolingPeriod;
    }

    void updateIncreaseShardsCooldown(TimeValue scaleUpCooldown) {
        this.increaseShardsCooldown = scaleUpCooldown;
    }

    void updateReduceShardsCooldown(TimeValue scaleDownCooldown) {
        this.reduceShardsCooldown = scaleDownCooldown;
    }

    void updateMinWriteThreads(int minNumberWriteThreads) {
        this.minWriteThreads = minNumberWriteThreads;
    }

    void updateMaxWriteThreads(int maxNumberWriteThreads) {
        this.maxWriteThreads = maxNumberWriteThreads;
    }

    private void updateDataStreamExcludePatterns(List<String> newExcludePatterns) {
        this.dataStreamExcludePatterns = newExcludePatterns;
    }

    private void updateIncreaseShardsMetric(WriteLoadMetric newMetric) {
        this.increaseShardsMetric = newMetric;
    }

    private void updateDecreaseShardsMetric(WriteLoadMetric newMetric) {
        this.decreaseShardsMetric = newMetric;
    }

    private static Double pickMetric(
        WriteLoadMetric metric,
        Double writeIndexLoad,
        Double writeIndexRecentLoad,
        Double writeIndexPeakLoad
    ) {
        return switch (metric) {
            case ALL_TIME -> writeIndexLoad;
            case RECENT -> writeIndexRecentLoad != null ? writeIndexRecentLoad : writeIndexLoad; // fall-back to all-time metric if null
            case PEAK -> writeIndexPeakLoad != null ? writeIndexPeakLoad : writeIndexLoad; // fall-back to all-time metric if null
        };
    }

    private static Double optionalDoubleToNullable(OptionalDouble optional) {
        return optional.isPresent() ? optional.getAsDouble() : null;
    }
}

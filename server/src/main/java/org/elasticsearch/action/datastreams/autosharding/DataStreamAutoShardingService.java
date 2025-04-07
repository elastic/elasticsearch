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
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexWriteLoad;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexingStats;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toMap;
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
    private final Consumer<Decision> decisionLogger;
    private volatile TimeValue increaseShardsCooldown;
    private volatile TimeValue decreaseShardsCooldown;
    private volatile int minWriteThreads;
    private volatile int maxWriteThreads;
    private volatile List<String> dataStreamExcludePatterns;
    private volatile WriteLoadMetric increaseShardsMetric;
    private volatile WriteLoadMetric decreaseShardsMetric;

    public DataStreamAutoShardingService(Settings settings, ClusterService clusterService, LongSupplier nowSupplier) {
        this(settings, clusterService, nowSupplier, createPeriodicLoggingDecisionConsumer(nowSupplier));
    }

    private static Consumer<Decision> createPeriodicLoggingDecisionConsumer(LongSupplier nowSupplier) {
        PeriodicDecisionLogger periodicDecisionLogger = new PeriodicDecisionLogger(nowSupplier);
        return periodicDecisionLogger::maybeLogDecision;
    }

    // Exists to allow a fake decision logger to be injected in tests
    DataStreamAutoShardingService(
        Settings settings,
        ClusterService clusterService,
        LongSupplier nowSupplier,
        Consumer<Decision> decisionLogger
    ) {
        this.clusterService = clusterService;
        this.isAutoShardingEnabled = settings.getAsBoolean(DATA_STREAMS_AUTO_SHARDING_ENABLED, false);
        this.increaseShardsCooldown = DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_COOLDOWN.get(settings);
        this.decreaseShardsCooldown = DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_COOLDOWN.get(settings);
        this.minWriteThreads = CLUSTER_AUTO_SHARDING_MIN_WRITE_THREADS.get(settings);
        this.maxWriteThreads = CLUSTER_AUTO_SHARDING_MAX_WRITE_THREADS.get(settings);
        this.dataStreamExcludePatterns = DATA_STREAMS_AUTO_SHARDING_EXCLUDES_SETTING.get(settings);
        this.increaseShardsMetric = DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_LOAD_METRIC.get(settings);
        this.decreaseShardsMetric = DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_LOAD_METRIC.get(settings);
        this.nowSupplier = nowSupplier;
        this.decisionLogger = decisionLogger;
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

    // package-private for testing

    /**
     * Contains all the information relating to a decision made by this service: the inputs, the calculations made, and the resulting
     * recommendation.
     *
     * @param inputs The inputs into the decision
     * @param increaseCalculation The results of the calculation to determine whether to increase the number of shards
     * @param decreaseCalculation The results of the calculation to determine whether to decrease the number of shards, or null of the
     *     service already decided to increase the number (the decrease calculation is skipped in that case)
     * @param result The resulting recommendation to be returned from the service
     */
    record Decision(
        Inputs inputs,
        IncreaseCalculation increaseCalculation,
        @Nullable DecreaseCalculation decreaseCalculation,
        AutoShardingResult result
    ) {

        /**
         * Contains the inputs to a decision.
         *
         * @param increaseShardsCooldown The time since the last auto-sharding to wait before increasing the shards
         *     (see the {@link #DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_COOLDOWN} cluster setting)
         * @param decreaseShardsCooldown The time since the last auto-sharding to wait before decreasing the shards - or, if there was no
         *     previous auto-sharding, the time since the first backing index was created
         *     (see the {@link #DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_COOLDOWN} cluster setting)
         * @param minWriteThreads The minimum number of write threads per shard (see the {@link #CLUSTER_AUTO_SHARDING_MIN_WRITE_THREADS}
         *     cluster setting)
         * @param maxWriteThreads The maximum number of write threads per shard (see the {@link #CLUSTER_AUTO_SHARDING_MAX_WRITE_THREADS}
         *     cluster setting)
         * @param increaseShardsMetric Which load metric to use for the increase shards calculation
         *     (see the {@link #DATA_STREAMS_AUTO_SHARDING_INCREASE_SHARDS_LOAD_METRIC} cluster setting)
         * @param decreaseShardsMetric Which load metric to use for the decrease shards calculation
         *     (see the {@link #DATA_STREAMS_AUTO_SHARDING_DECREASE_SHARDS_LOAD_METRIC} cluster setting)
         * @param dataStream The name of the data stream
         * @param writeIndex The name of the current write index
         * @param writeIndexAllTimeLoad The all-time load metric for the write index (see {@link IndexingStats.Stats#getWriteLoad()})
         * @param writeIndexRecentLoad The recent load metric for the current write index
         *     (see {@link IndexingStats.Stats#getRecentWriteLoad()})
         * @param writeIndexPeakLoad The peak load metric for the write index (see {@link IndexingStats.Stats#getPeakWriteLoad()})
         * @param currentNumberOfWriteIndexShards The number of shards for the current write index
         */
        record Inputs(
            TimeValue increaseShardsCooldown,
            TimeValue decreaseShardsCooldown,
            int minWriteThreads,
            int maxWriteThreads,
            DataStreamAutoShardingService.WriteLoadMetric increaseShardsMetric,
            DataStreamAutoShardingService.WriteLoadMetric decreaseShardsMetric,
            String dataStream,
            String writeIndex,
            double writeIndexAllTimeLoad,
            double writeIndexRecentLoad,
            double writeIndexPeakLoad,
            int currentNumberOfWriteIndexShards
        ) {}

        /**
         * Contains details from the increase shards calculation.
         *
         * @param writeIndexLoadForIncrease The load considered for the increase shards calculation (i.e. one of write index load metrics
         *     from the {@link Decision.Inputs}, as determined by {@link Decision.Inputs#increaseShardsMetric})
         * @param optimalShardCountForIncrease The optimal shard count determined based on the load
         * @param increaseResult If the optimal shard count is greater than the current shard count, a recommendation to increase the number
         *     of shards (possibly after a cooldown period); otherwise, null
         */
        record IncreaseCalculation(
            double writeIndexLoadForIncrease,
            int optimalShardCountForIncrease,
            @Nullable AutoShardingResult increaseResult
        ) {}

        /**
         * Contains details from the increase shards calculation.
         *
         * @param maxLoadWithinCooldown The load considered for the decrease shards calculation (i.e. a load metric determined by
         *     {@link Decision.Inputs#increaseShardsMetric}, either for the write index or an older one within the cooldown period) and a
         *     record of which index that corresponded to
         * @param optimalShardCountForDecrease The optimal shard count determined based on the load
         * @param decreaseResult If the optimal shard count is less than the current shard count, a recommendation to decrease the number
         *     of shards (possibly after a cooldown period); otherwise, null
         */
        record DecreaseCalculation(
            MaxLoadWithinCooldown maxLoadWithinCooldown,
            int optimalShardCountForDecrease,
            @Nullable AutoShardingResult decreaseResult
        ) {

            /**
             * Contains information about the backing index with the highest load, out of the current write index and all the older
             * indices created within the cooldown period.
             *
             * @param load The highest load
             * @param previousIndexWithMaxLoad If this load came from one of the previous backing indices, the name of that index; null if
             *    it came from the current write index
             */
            record MaxLoadWithinCooldown(double load, @Nullable String previousIndexWithMaxLoad) {}
        }

        @Override
        public String toString() {
            return Strings.format(
                "For data stream %s: %s based on [inc/dec cooldowns %s/%s, %d-%d threads, "
                    + "write index %s has all-time/recent/peak loads %g/%g/%g, current shards %d, "
                    + "increase calculation gives %d shards using %s load %g for write index%s]",
                inputs.dataStream,
                result,
                inputs.increaseShardsCooldown,
                inputs.decreaseShardsCooldown,
                inputs.minWriteThreads,
                inputs.maxWriteThreads,
                inputs.writeIndex,
                inputs.writeIndexAllTimeLoad,
                inputs.writeIndexRecentLoad,
                inputs.writeIndexPeakLoad,
                inputs.currentNumberOfWriteIndexShards,
                increaseCalculation.optimalShardCountForIncrease,
                inputs.increaseShardsMetric,
                increaseCalculation.writeIndexLoadForIncrease,
                decreaseCalculation == null
                    ? ""
                    : Strings.format(
                        ", decrease calculation gives %d shards using %s load %g for %s",
                        decreaseCalculation.optimalShardCountForDecrease,
                        inputs.decreaseShardsMetric,
                        decreaseCalculation.maxLoadWithinCooldown.load,
                        decreaseCalculation.maxLoadWithinCooldown.previousIndexWithMaxLoad != null
                            ? decreaseCalculation.maxLoadWithinCooldown.previousIndexWithMaxLoad
                            : "write index"
                    )
            );
        }
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

        double writeIndexAllTimeLoad = sumLoadMetrics(writeIndexStats, IndexingStats.Stats::getWriteLoad);
        double writeIndexRecentLoad = sumLoadMetrics(writeIndexStats, IndexingStats.Stats::getRecentWriteLoad);
        double writeIndexPeakLoad = sumLoadMetrics(writeIndexStats, IndexingStats.Stats::getPeakWriteLoad);
        IndexMetadata writeIndex = state.metadata().index(dataStream.getWriteIndex());
        assert writeIndex != null : "the data stream write index must exist in the provided cluster metadata";
        Decision.Inputs inputs = new Decision.Inputs(
            increaseShardsCooldown,
            decreaseShardsCooldown,
            minWriteThreads,
            maxWriteThreads,
            increaseShardsMetric,
            decreaseShardsMetric,
            dataStream.getName(),
            writeIndex.getIndex().getName(),
            writeIndexAllTimeLoad,
            writeIndexRecentLoad,
            writeIndexPeakLoad,
            writeIndex.getNumberOfShards()
        );
        Decision decision = innerCalculate(state.metadata(), dataStream, inputs);
        decisionLogger.accept(decision);
        return decision.result();
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

    private Decision innerCalculate(ProjectMetadata project, DataStream dataStream, Decision.Inputs inputs) {
        // See whether we recommend increasing the number of shards.
        Decision.IncreaseCalculation increaseCalculation = calculateIncreaseShardsDecision(dataStream, inputs);
        if (increaseCalculation.increaseResult() != null) {
            return new Decision(inputs, increaseCalculation, null, increaseCalculation.increaseResult());
        }
        // If not, see whether we recommend decreasing the number of shards.
        Decision.DecreaseCalculation decreaseCalculation = calculateDecreaseShardsDecision(project, dataStream, inputs);
        if (decreaseCalculation.decreaseResult() != null) {
            return new Decision(inputs, increaseCalculation, decreaseCalculation, decreaseCalculation.decreaseResult());
        }
        // If we don't recommend increasing or decreasing then we recommend no change.
        return new Decision(
            inputs,
            increaseCalculation,
            decreaseCalculation,
            new AutoShardingResult(
                AutoShardingType.NO_CHANGE_REQUIRED,
                inputs.currentNumberOfWriteIndexShards(),
                inputs.currentNumberOfWriteIndexShards(),
                TimeValue.ZERO
            )
        );
    }

    private Decision.IncreaseCalculation calculateIncreaseShardsDecision(DataStream dataStream, Decision.Inputs inputs) {
        // increasing the number of shards is calculated solely based on the index load of the write index
        double writeIndexLoadForIncrease = pickMetric(
            inputs.increaseShardsMetric(),
            inputs.writeIndexAllTimeLoad(),
            inputs.writeIndexRecentLoad(),
            inputs.writeIndexPeakLoad()
        );
        int optimalShardCountForIncrease = computeOptimalNumberOfShards(
            inputs.minWriteThreads(),
            inputs.maxWriteThreads(),
            writeIndexLoadForIncrease
        );
        if (optimalShardCountForIncrease > inputs.currentNumberOfWriteIndexShards()) {
            TimeValue timeSinceLastAutoShardingEvent = dataStream.getAutoShardingEvent() != null
                ? dataStream.getAutoShardingEvent().getTimeSinceLastAutoShardingEvent(nowSupplier)
                : TimeValue.MAX_VALUE;
            TimeValue coolDownRemaining = TimeValue.timeValueMillis(
                Math.max(0L, inputs.increaseShardsCooldown().millis() - timeSinceLastAutoShardingEvent.millis())
            );
            return new Decision.IncreaseCalculation(
                writeIndexLoadForIncrease,
                optimalShardCountForIncrease,
                new AutoShardingResult(
                    coolDownRemaining.equals(TimeValue.ZERO)
                        ? AutoShardingType.INCREASE_SHARDS
                        : AutoShardingType.COOLDOWN_PREVENTED_INCREASE,
                    inputs.currentNumberOfWriteIndexShards(),
                    optimalShardCountForIncrease,
                    coolDownRemaining
                )
            );
        }
        return new Decision.IncreaseCalculation(writeIndexLoadForIncrease, optimalShardCountForIncrease, null);
    }

    /**
     * Calculates the amount of time remaining before we can consider reducing the number of shards.
     * This reference for the remaining time math is either the time since the last auto sharding event (if available) or otherwise the
     * oldest index in the data stream.
     */
    private TimeValue getRemainingDecreaseShardsCooldown(ProjectMetadata project, DataStream dataStream, TimeValue decreaseShardsCooldown) {
        Index oldestBackingIndex = dataStream.getIndices().get(0);
        IndexMetadata oldestIndexMeta = project.getIndexSafe(oldestBackingIndex);

        return dataStream.getAutoShardingEvent() == null
            // without a pre-existing auto sharding event we wait until the oldest index has been created longer than the decrease_shards
            // cool down period "ago" so we don't immediately reduce the number of shards after a data stream is created
            ? TimeValue.timeValueMillis(
                Math.max(0L, oldestIndexMeta.getCreationDate() + decreaseShardsCooldown.millis() - nowSupplier.getAsLong())
            )
            : TimeValue.timeValueMillis(
                Math.max(
                    0L,
                    decreaseShardsCooldown.millis() - dataStream.getAutoShardingEvent()
                        .getTimeSinceLastAutoShardingEvent(nowSupplier)
                        .millis()
                )
            );
    }

    private Decision.DecreaseCalculation calculateDecreaseShardsDecision(
        ProjectMetadata project,
        DataStream dataStream,
        Decision.Inputs inputs
    ) {
        TimeValue remainingCooldownForDecrease = getRemainingDecreaseShardsCooldown(project, dataStream, inputs.decreaseShardsCooldown());
        Decision.DecreaseCalculation.MaxLoadWithinCooldown maxLoadWithinCooldownForDecrease = getMaxIndexLoadWithinCoolingPeriod(
            project,
            dataStream,
            inputs,
            nowSupplier
        );
        int optimalShardCountForDecrease = computeOptimalNumberOfShards(
            inputs.minWriteThreads(),
            inputs.maxWriteThreads(),
            maxLoadWithinCooldownForDecrease.load()
        );
        if (optimalShardCountForDecrease < inputs.currentNumberOfWriteIndexShards()) {
            return new Decision.DecreaseCalculation(
                maxLoadWithinCooldownForDecrease,
                optimalShardCountForDecrease,
                new AutoShardingResult(
                    remainingCooldownForDecrease.equals(TimeValue.ZERO)
                        ? AutoShardingType.DECREASE_SHARDS
                        : AutoShardingType.COOLDOWN_PREVENTED_DECREASE,
                    inputs.currentNumberOfWriteIndexShards,
                    optimalShardCountForDecrease,
                    remainingCooldownForDecrease
                )
            );
        }
        return new Decision.DecreaseCalculation(maxLoadWithinCooldownForDecrease, optimalShardCountForDecrease, null);
    }

    // Visible for testing
    static int computeOptimalNumberOfShards(int minNumberWriteThreads, int maxNumberWriteThreads, double indexingLoad) {
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
        return Math.toIntExact(
            Math.max(
                Math.max(
                    Math.min(roundUp(indexingLoad / (minNumberWriteThreads / 2.0)), 3),
                    roundUp(indexingLoad / (maxNumberWriteThreads / 2.0))
                ),
                1 // we don't want to go lower than 1 shard
            )
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
    static Decision.DecreaseCalculation.MaxLoadWithinCooldown getMaxIndexLoadWithinCoolingPeriod(
        ProjectMetadata project,
        DataStream dataStream,
        Decision.Inputs inputs,
        LongSupplier nowSupplier
    ) {
        // for reducing the number of shards we look at more than just the write index
        Map<String, IndexWriteLoad> writeLoadsWithinCoolingPeriod = DataStream.getIndicesWithinMaxAgeRange(
            dataStream,
            project::getIndexSafe,
            inputs.decreaseShardsCooldown(),
            nowSupplier
        )
            .stream()
            .filter(index -> index.equals(dataStream.getWriteIndex()) == false)
            .map(project::index)
            .filter(Objects::nonNull)
            .filter(metadata -> metadata.getStats() != null)
            .filter(metadata -> metadata.getStats().indexWriteLoad() != null)
            .collect(
                toMap(metadata -> metadata.getIndex().getName(), metadata -> metadata.getStats().indexWriteLoad(), (unused1, unused2) -> {
                    throw new IllegalStateException("Multiple indices with same name");
                }, LinkedHashMap::new)
            );

        // assume the current write index load is the highest observed and look back to find the actual maximum
        double maxLoadWithinCooldown = pickMetric(
            inputs.decreaseShardsMetric(),
            inputs.writeIndexAllTimeLoad(),
            inputs.writeIndexRecentLoad(),
            inputs.writeIndexPeakLoad()
        );
        String previousIndexInCooldownWithMaxLoad = null;
        for (Map.Entry<String, IndexWriteLoad> entry : writeLoadsWithinCoolingPeriod.entrySet()) {
            String indexName = entry.getKey();
            IndexWriteLoad writeLoad = entry.getValue();
            double totalIndexLoad = 0;
            for (int shardId = 0; shardId < writeLoad.numberOfShards(); shardId++) {
                Double writeLoadForShard = pickMetric(
                    inputs.decreaseShardsMetric(),
                    optionalDoubleToNullable(writeLoad.getWriteLoadForShard(shardId)),
                    optionalDoubleToNullable(writeLoad.getRecentWriteLoadForShard(shardId)),
                    optionalDoubleToNullable(writeLoad.getPeakWriteLoadForShard(shardId))
                );
                if (writeLoadForShard != null) {
                    totalIndexLoad += writeLoadForShard;
                }
            }
            if (totalIndexLoad > maxLoadWithinCooldown) {
                maxLoadWithinCooldown = totalIndexLoad;
                previousIndexInCooldownWithMaxLoad = indexName;
            }
        }
        return new Decision.DecreaseCalculation.MaxLoadWithinCooldown(maxLoadWithinCooldown, previousIndexInCooldownWithMaxLoad);
    }

    void updateIncreaseShardsCooldown(TimeValue scaleUpCooldown) {
        this.increaseShardsCooldown = scaleUpCooldown;
    }

    void updateReduceShardsCooldown(TimeValue scaleDownCooldown) {
        this.decreaseShardsCooldown = scaleDownCooldown;
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

    /**
     * A buffer which can be used to store a number of {@link Decision} instances, up to some fixed size limit, keeping the decisions with
     * the highest value according to some comparator.
     */
    private static class DecisionBuffer {

        private final Comparator<Decision> comparator;
        private final PriorityQueue<Decision> queue; // This is a Lucene PriorityQueue, which is bounded, unlike the JDK one.

        DecisionBuffer(int maxSize, Comparator<Decision> comparator) {
            this.comparator = comparator;
            this.queue = new PriorityQueue<>(maxSize) {
                @Override
                protected boolean lessThan(Decision decision1, Decision decision2) {
                    return comparator.compare(decision1, decision2) < 0;
                }
            };
        }

        /**
         * Inserts the decision into the buffer, unless the buffer is full and all the contents are of higher value according to the
         * comparator, in which case the decision is discarded.
         */
        synchronized void insert(Decision Decision) {
            queue.insertWithOverflow(Decision);
        }

        /**
         * Clears the buffer, returning a copy of the previous contents of the buffer. The returned values will be sorted from high to low
         * according to the comparator.
         */
        synchronized List<Decision> flush() {
            List<Decision> previousDecisions = StreamSupport.stream(queue.spliterator(), false).sorted(comparator.reversed()).toList();
            queue.clear();
            return previousDecisions;
        }
    }

    // Package-private for testing
    static class PeriodicDecisionLogger {

        static final int BUFFER_SIZE = 10;
        static final long FLUSH_INTERVAL_MILLIS = TimeValue.timeValueMinutes(5).millis();

        private static final Comparator<Decision> HIGHEST_LOAD_COMPARATOR = Comparator.comparing(
            d -> d.increaseCalculation().writeIndexLoadForIncrease()
        );

        private final LongSupplier nowSupplier;
        private final Consumer<FlushedDecisions> logConsumer;
        private final AtomicLong lastFlushMillis;
        private final DecisionBuffer highestLoadIncreaseDecisions;
        private final DecisionBuffer highestLoadNonIncreaseDecisions;

        PeriodicDecisionLogger(LongSupplier nowSupplier) {
            this(nowSupplier, PeriodicDecisionLogger::logFlushedDecision);
        }

        // Exists to allow a fake logger to be injected in tests
        PeriodicDecisionLogger(LongSupplier nowSupplier, Consumer<FlushedDecisions> logConsumer) {
            this.nowSupplier = nowSupplier;
            this.highestLoadIncreaseDecisions = new DecisionBuffer(BUFFER_SIZE, HIGHEST_LOAD_COMPARATOR);
            this.highestLoadNonIncreaseDecisions = new DecisionBuffer(BUFFER_SIZE, HIGHEST_LOAD_COMPARATOR);
            this.lastFlushMillis = new AtomicLong(nowSupplier.getAsLong());
            this.logConsumer = logConsumer;
        }

        record FlushedDecisions(List<Decision> highestLoadIncreaseDecisions, List<Decision> highestLoadNonIncreaseDecisions) {}

        void maybeLogDecision(Decision Decision) {
            assert Decision.result != null : "Attempting to log a decision with no result";
            logger.debug("Data stream auto-sharding result: {}", Decision);
            if (Decision.result.type() == AutoShardingType.INCREASE_SHARDS) {
                highestLoadIncreaseDecisions.insert(Decision);
            } else {
                highestLoadNonIncreaseDecisions.insert(Decision);
            }
            if (shouldFlush()) {
                FlushedDecisions flushedDecisions = new FlushedDecisions(
                    highestLoadIncreaseDecisions.flush(),
                    highestLoadNonIncreaseDecisions.flush()
                );
                logConsumer.accept(flushedDecisions);
            }
        }

        private boolean shouldFlush() {
            long now = nowSupplier.getAsLong();
            // Get the time of the last flush. If it is time for the next flush, update that with the current time; else leave it alone.
            long previous = lastFlushMillis.getAndUpdate(last -> ((now - last) >= FLUSH_INTERVAL_MILLIS) ? now : last);
            // Return whether it is time for the next flush.
            return (now - previous) >= FLUSH_INTERVAL_MILLIS;
        }

        private static void logFlushedDecision(FlushedDecisions decisions) {
            if (decisions.highestLoadIncreaseDecisions.isEmpty() == false) {
                logger.info(
                    "Data stream auto-sharding decisions in the last {} with highest load with an increase shards recommendation: \n{}",
                    TimeValue.timeValueMillis(FLUSH_INTERVAL_MILLIS),
                    decisions.highestLoadIncreaseDecisions.stream().map(d -> " - " + d).collect(Collectors.joining("\n"))
                );
            }
            if (decisions.highestLoadNonIncreaseDecisions.isEmpty() == false) {
                logger.info(
                    "Data stream auto-sharding decisions in the last {} with highest load without an increase shards recommendation: \n{}",
                    TimeValue.timeValueMillis(FLUSH_INTERVAL_MILLIS),
                    decisions.highestLoadNonIncreaseDecisions.stream().map(d -> " - " + d).collect(Collectors.joining("\n"))
                );
            }
        }
    }
}

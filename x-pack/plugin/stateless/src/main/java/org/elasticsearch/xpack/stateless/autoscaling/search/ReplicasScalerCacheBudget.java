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

package org.elasticsearch.xpack.stateless.autoscaling.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.stateless.autoscaling.DesiredTopologyContext;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.xpack.stateless.autoscaling.search.ReplicaRankingContext.DEFAULT_NUMBER_OF_REPLICAS;
import static org.elasticsearch.xpack.stateless.autoscaling.search.ReplicasLoadBalancingScaler.ReplicasLoadBalancingResult;

/**
 * Filters scale-up recommendations by the {@link ReplicasLoadBalancingScaler} that will exceed a certain amount of cache space (in theory,
 * based on the index's interactive size).
 */
public class ReplicasScalerCacheBudget {

    private static final Logger LOGGER = LogManager.getLogger(ReplicasScalerCacheBudget.class);

    /**
     * Controls the fraction of cache space budgeted for scaling replicas for load balancing. The rough idea is: if increasing the number
     * of replicas will require us to use more cache than is budgeted here (50% by default), then don't scale up.
     * Replicas for instant failover is budgeted separately, and therefore not restricted by this value. However, if replicas for instant
     * failover requires a smaller fraction of the cache than this value, then replicas for load balancing will only allow scale-ups until
     * the value is met.
     *
     * <p>Example 1: Current replica counts theoretically need 40% of the available cache space. Replicas for instant failover recommends
     * scaling up indices such that 55% of the cache space will be needed. Replicas for load balancing also recommends further scale ups.
     * -> In this case, we will scale up all instant failover recommendations, but no load balancing recommendations
     *
     * <p>Example 2: Current replica counts theoretically need 30% of the available cache space. Replicas for instant failover recommends
     * scaling up indices such that 45% of the cache space will be needed. Replicas for load balancing also recommends further scale ups.
     * -> In this case, we will scale up all instant failover recommendations, and only the load balancing recommendations that keep us
     * below 50% cache usage. Once exceeded, any additional load balancing recommendations will be ignored.
     */
    public static final Setting<Double> REPLICA_CACHE_BUDGET_RATIO = Setting.doubleSetting(
        "serverless.autoscaling.replica_cache_budget_ratio",
        0.5,
        0,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final Settings settings;
    private final DesiredTopologyContext desiredTopologyContext;
    private final ReplicasScaleDownState replicasScaleDownState;
    private final Integer scaledownRepetitionSetting;
    private volatile Double cacheBudgetRatio;

    public ReplicasScalerCacheBudget(
        Settings settings,
        DesiredTopologyContext desiredTopologyContext,
        ReplicasScaleDownState replicasScaleDownState,
        Integer scaledownRepetitionSetting
    ) {
        this.settings = settings;
        this.desiredTopologyContext = desiredTopologyContext;
        this.replicasScaleDownState = replicasScaleDownState;
        this.scaledownRepetitionSetting = scaledownRepetitionSetting;
    }

    void setCacheBudgetRatio(Double cacheBudgetRatio) {
        this.cacheBudgetRatio = cacheBudgetRatio;
    }

    /**
     * Creates a new ReplicasLoadBalancingResult with an altered map of desired replica counts. Indices are removed from this map if scaling
     * them up will exceed the cache budget. The cache budget is calculated using the
     * {@link ReplicasScalerCacheBudget#REPLICA_CACHE_BUDGET_RATIO} setting. When budgeting, we will prioritize indices with higher search
     * loads.
     *
     * <p>Note: this cache budget may only restrict load balancing recommendations. Instant failover recommendations may exceed this budget.
     *
     * @param rankingContext                the current state of indices and shards
     * @param instantFailoverReplicaChanges a map of all recommended replica changes made by the instant failover system
     * @param replicasLoadBalancingResult   a result object containing: a map of immediate scale downs and a map of all recommended replica
     *                                      changes made by the load balancing system
     * @param immediateScaleDown            whether we will immediately scale down all recommendations, rather than waiting for the required
     *                                      repeated signals
     * @return a new {@link ReplicasLoadBalancingResult}. It has the same {@link ReplicasLoadBalancingResult#immediateReplicaScaleDown()}
     * as the parameter. If some scale-ups exceeded the cache budget, {@link ReplicasLoadBalancingResult#desiredReplicasPerIndex()} will
     * have a subset of the entries, and {@link ReplicasLoadBalancingResult#indicesBlockedFromScaleUp()} will be larger, reflecting the
     * difference.
     */
    ReplicasLoadBalancingResult applyCacheBudgetConstraint(
        ReplicaRankingContext rankingContext,
        Map<String, Integer> instantFailoverReplicaChanges,
        ReplicasLoadBalancingResult replicasLoadBalancingResult,
        boolean immediateScaleDown
    ) {
        var cacheBudgetRatio = this.cacheBudgetRatio;
        var desiredClusterTopology = desiredTopologyContext.getDesiredClusterTopology();
        if (desiredClusterTopology == null) {
            return replicasLoadBalancingResult;
        }
        var searchTierTopology = desiredClusterTopology.getSearch();
        long desiredNodeFsSize = (long) (searchTierTopology.getMemory().getBytes() * searchTierTopology.getStorageRatio());
        // Calculate the cache budget we have for scaling up replicas for load balancing
        long cacheBudget = (long) (SharedBlobCacheService.calculateCacheSize(settings, desiredNodeFsSize) * searchTierTopology.getReplicas()
            * cacheBudgetRatio);

        // Calculate the cache needed to handle current replica counts
        long cacheNeededByCurrentReplicas = rankingContext.getSumOfReplicasInteractiveSizes();

        long cacheFreedByScaleDowns = getCacheFreedByScaleDowns(
            rankingContext,
            instantFailoverReplicaChanges,
            replicasLoadBalancingResult,
            immediateScaleDown
        );

        long cacheNeededForInstantFailoverScaleUps = getCacheNeededForInstantFailoverScaleUps(
            rankingContext,
            instantFailoverReplicaChanges
        );

        // Calculate the cache budget available for scale-ups recommended by the load balancing system - specifically those that are larger
        // recommendations than those given by the instant failover system.
        long cacheBudgetForLoadBalancingScaleUps = cacheBudget + cacheFreedByScaleDowns - cacheNeededByCurrentReplicas
            - cacheNeededForInstantFailoverScaleUps;

        LOGGER.debug(
            "Cache budget for replicas scaling calculation: cacheBudget={}, cacheNeededByCurrentReplicas={}, cacheFreedByScaleDowns={}, "
                + "cacheNeededForInstantFailoverScaleUps={}, cacheBudgetForLoadBalancingScaleUps={}",
            cacheBudget,
            cacheNeededByCurrentReplicas,
            cacheFreedByScaleDowns,
            cacheNeededForInstantFailoverScaleUps,
            cacheBudgetForLoadBalancingScaleUps
        );

        return budgetLoadBalancingScaleUps(
            rankingContext,
            instantFailoverReplicaChanges,
            replicasLoadBalancingResult,
            cacheBudgetForLoadBalancingScaleUps
        );
    }

    /**
     * Calculate the cache that will be freed by imminent scale-downs. These can happen in several ways:
     * <ol>
     *     <li>Entries in replicasLoadBalancingResult.immediateReplicaScaleDown</li>
     *     <li>
     *         Combined recommendations (Math.max) of instantFailoverReplicaChanges and replicasLoadBalancingResult.desiredReplicasPerIndex
     *         <ol>
     *             <li>If immediateScaleDown = true, all scale-downs will be immediate</li>
     *             <li>Else, only those that have a sufficient number of repeated signals will scale down</li>
     *         </ol>
     *     </li>
     * </ol>
     */
    long getCacheFreedByScaleDowns(
        ReplicaRankingContext rankingContext,
        Map<String, Integer> instantFailoverReplicaChanges,
        ReplicasLoadBalancingResult replicasLoadBalancingResult,
        boolean immediateScaleDown
    ) {
        long cacheFreedByScaleDowns = 0;
        for (Map.Entry<String, Integer> entry : replicasLoadBalancingResult.immediateReplicaScaleDown().entrySet()) {
            IndexReplicationRanker.IndexRankingProperties props = rankingContext.rankingProperties(entry.getKey());
            if (props == null) {
                LOGGER.error("Index [{}] listed for immediate replica scale-down is missing from context", entry.getKey());
                assert false : "Index [" + entry.getKey() + "] listed for immediate replica scale-down is missing from context";
                continue;
            }
            int currentReplicas = props.indexProperties().replicas();
            int targetReplicas = entry.getValue();
            assert targetReplicas < currentReplicas
                : "index marked for immediate scale down to must target a number of replicas smaller than the current number";
            cacheFreedByScaleDowns += props.interactiveSize() * (currentReplicas - targetReplicas);
        }
        for (IndexReplicationRanker.IndexRankingProperties props : rankingContext.properties()) {
            String indexName = props.indexProperties().name();
            int currentReplicas = props.indexProperties().replicas();
            int recommendedReplicas = Math.max(
                instantFailoverReplicaChanges.getOrDefault(indexName, DEFAULT_NUMBER_OF_REPLICAS),
                replicasLoadBalancingResult.desiredReplicasPerIndex().getOrDefault(indexName, DEFAULT_NUMBER_OF_REPLICAS)
            );

            if (recommendedReplicas < currentReplicas) {
                ReplicasScaleDownState.PerIndexState state = replicasScaleDownState.getState(indexName);
                if (immediateScaleDown) {
                    cacheFreedByScaleDowns += props.interactiveSize() * (currentReplicas - recommendedReplicas);
                } else if (state != null && state.signalCount() + 1 >= scaledownRepetitionSetting) {
                    int actualTargetReplicas = Math.max(recommendedReplicas, state.maxReplicasRecommended());
                    cacheFreedByScaleDowns += props.interactiveSize() * (currentReplicas - actualTargetReplicas);
                }
            }
        }
        return cacheFreedByScaleDowns;
    }

    /**
     * Calculate the cache needed for scale-ups recommended by the instant failover system.
     * These will not be budgeted, however the disk required will still factor in to the budgeting of the load balancing system's
     * recommendations.
     */
    static long getCacheNeededForInstantFailoverScaleUps(
        ReplicaRankingContext rankingContext,
        Map<String, Integer> instantFailoverReplicaChanges
    ) {
        long cacheNeededForInstantFailoverScaleUps = 0;
        for (Map.Entry<String, Integer> entry : instantFailoverReplicaChanges.entrySet()) {
            IndexReplicationRanker.IndexRankingProperties props = rankingContext.rankingProperties(entry.getKey());
            if (props == null) {
                LOGGER.error("Index [{}] with recommendation from instant failover system is missing from context", entry.getKey());
                assert false : "Index [" + entry.getKey() + "] with recommendation from instant failover system is missing from context";
                continue;
            }
            int currentReplicas = props.indexProperties().replicas();
            int targetReplicas = entry.getValue();
            if (targetReplicas > currentReplicas) {
                cacheNeededForInstantFailoverScaleUps += props.interactiveSize() * (targetReplicas - currentReplicas);
            }
        }
        return cacheNeededForInstantFailoverScaleUps;
    }

    /**
     * Given a cacheBudgetForLoadBalancingScaleUps, identify the indices for which the load balancing system has recommended a scale-up
     * to _more_ replicas than the instant failover system. We may ignore such recommendations if they exceed the cache budget.
     *
     * @return A response object based on the parameter of the same type.
     */
    static ReplicasLoadBalancingResult budgetLoadBalancingScaleUps(
        ReplicaRankingContext rankingContext,
        Map<String, Integer> instantFailoverReplicaChanges,
        ReplicasLoadBalancingResult replicasLoadBalancingResult,
        long cacheBudgetForLoadBalancingScaleUps
    ) {
        SortedMap<String, Integer> budgetedLoadBalancingReplicaChanges = new TreeMap<>(
            replicasLoadBalancingResult.desiredReplicasPerIndex().comparator()
        );
        int indicesBlockedFromScaleUp = replicasLoadBalancingResult.indicesBlockedFromScaleUp();

        long cacheNeededForScaleUps = 0;
        for (Map.Entry<String, Integer> entry : replicasLoadBalancingResult.desiredReplicasPerIndex().entrySet()) {
            String indexName = entry.getKey();
            IndexReplicationRanker.IndexRankingProperties props = rankingContext.rankingProperties(indexName);
            if (props == null) {
                LOGGER.error("Index [{}] with recommendation from load balancing system is missing from context", entry.getKey());
                assert false : "Index [" + entry.getKey() + "] with recommendation from load balancing system is missing from context";
                continue;
            }
            int currentReplicas = props.indexProperties().replicas();
            int targetReplicasForLoadBalancing = entry.getValue();
            if (targetReplicasForLoadBalancing > currentReplicas) {
                // Scale up: check budget
                int numberReplicasToBudget = targetReplicasForLoadBalancing - currentReplicas;
                Integer targetReplicasForInstantFailover = instantFailoverReplicaChanges.get(indexName);
                if (targetReplicasForInstantFailover != null && targetReplicasForInstantFailover > currentReplicas) {
                    numberReplicasToBudget = targetReplicasForLoadBalancing - targetReplicasForInstantFailover;
                }
                long additionalCacheNeeded = props.interactiveSize() * numberReplicasToBudget;
                if (cacheNeededForScaleUps + additionalCacheNeeded <= cacheBudgetForLoadBalancingScaleUps) {
                    cacheNeededForScaleUps += additionalCacheNeeded;
                    budgetedLoadBalancingReplicaChanges.put(indexName, targetReplicasForLoadBalancing);
                } else {
                    indicesBlockedFromScaleUp++;
                    LOGGER.debug("Replicas scaling: cache budget exceeded, will not scale up index [{}]", indexName);
                    budgetedLoadBalancingReplicaChanges.put(indexName, currentReplicas);
                }
            } else {
                // Scale-down: always include
                budgetedLoadBalancingReplicaChanges.put(entry.getKey(), entry.getValue());
            }
        }
        return new ReplicasLoadBalancingResult(
            replicasLoadBalancingResult.immediateReplicaScaleDown(),
            budgetedLoadBalancingReplicaChanges,
            indicesBlockedFromScaleUp
        );
    }
}

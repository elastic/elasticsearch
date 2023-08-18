/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.MaxPrimaryShardDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Waits for at least one rollover condition to be satisfied, using the Rollover API's dry_run option.
 */
public class WaitForRolloverReadyStep extends AsyncWaitStep {
    private static final Logger logger = LogManager.getLogger(WaitForRolloverReadyStep.class);

    public static final String NAME = "check-rollover-ready";

    public static final long MAX_PRIMARY_SHARD_DOCS = 200_000_000L;

    private final RolloverConditions conditions;

    public WaitForRolloverReadyStep(
        StepKey key,
        StepKey nextStepKey,
        Client client,
        ByteSizeValue maxSize,
        ByteSizeValue maxPrimaryShardSize,
        TimeValue maxAge,
        Long maxDocs,
        Long maxPrimaryShardDocs,
        ByteSizeValue minSize,
        ByteSizeValue minPrimaryShardSize,
        TimeValue minAge,
        Long minDocs,
        Long minPrimaryShardDocs
    ) {
        super(key, nextStepKey, client);
        this.conditions = RolloverConditions.newBuilder()
            .addMaxIndexSizeCondition(maxSize)
            .addMaxPrimaryShardSizeCondition(maxPrimaryShardSize)
            .addMaxIndexAgeCondition(maxAge)
            .addMaxIndexDocsCondition(maxDocs)
            .addMaxPrimaryShardDocsCondition(maxPrimaryShardDocs)
            .addMinIndexSizeCondition(minSize)
            .addMinPrimaryShardSizeCondition(minPrimaryShardSize)
            .addMinIndexAgeCondition(minAge)
            .addMinIndexDocsCondition(minDocs)
            .addMinPrimaryShardDocsCondition(minPrimaryShardDocs)
            .build();
    }

    public WaitForRolloverReadyStep(StepKey key, StepKey nextStepKey, Client client, RolloverConditions conditions) {
        super(key, nextStepKey, client);
        this.conditions = conditions;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void evaluateCondition(Metadata metadata, Index index, Listener listener, TimeValue masterTimeout) {
        IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(index.getName());
        assert indexAbstraction != null : "invalid cluster metadata. index [" + index.getName() + "] was not found";
        final String rolloverTarget;
        DataStream dataStream = indexAbstraction.getParentDataStream();
        if (dataStream != null) {
            assert dataStream.getWriteIndex() != null : "datastream " + dataStream.getName() + " has no write index";
            if (dataStream.getWriteIndex().equals(index) == false) {
                logger.warn(
                    "index [{}] is not the write index for data stream [{}]. skipping rollover for policy [{}]",
                    index.getName(),
                    dataStream.getName(),
                    metadata.index(index).getLifecyclePolicyName()
                );
                listener.onResponse(true, EmptyInfo.INSTANCE);
                return;
            }
            rolloverTarget = dataStream.getName();
        } else {
            IndexMetadata indexMetadata = metadata.index(index);
            String rolloverAlias = RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.get(indexMetadata.getSettings());

            if (Strings.isNullOrEmpty(rolloverAlias)) {
                listener.onFailure(
                    new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "setting [%s] for index [%s] is empty or not defined",
                            RolloverAction.LIFECYCLE_ROLLOVER_ALIAS,
                            index.getName()
                        )
                    )
                );
                return;
            }

            if (indexMetadata.getRolloverInfos().get(rolloverAlias) != null) {
                logger.info(
                    "index [{}] was already rolled over for alias [{}], not attempting to roll over again",
                    index.getName(),
                    rolloverAlias
                );
                listener.onResponse(true, EmptyInfo.INSTANCE);
                return;
            }

            // The order of the following checks is important in ways which may not be obvious.

            // First, figure out if 1) The configured alias points to this index, and if so,
            // whether this index is the write alias for this index
            boolean aliasPointsToThisIndex = indexMetadata.getAliases().containsKey(rolloverAlias);

            Boolean isWriteIndex = null;
            if (aliasPointsToThisIndex) {
                // The writeIndex() call returns a tri-state boolean:
                // true -> this index is the write index for this alias
                // false -> this index is not the write index for this alias
                // null -> this alias is a "classic-style" alias and does not have a write index configured, but only points to one index
                // and is thus the write index by default
                isWriteIndex = indexMetadata.getAliases().get(rolloverAlias).writeIndex();
            }

            boolean indexingComplete = LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.get(indexMetadata.getSettings());
            if (indexingComplete) {
                logger.trace(index + " has lifecycle complete set, skipping " + WaitForRolloverReadyStep.NAME);
                // If this index is still the write index for this alias, skipping rollover and continuing with the policy almost certainly
                // isn't what we want, as something likely still expects to be writing to this index.
                // If the alias doesn't point to this index, that's okay as that will be the result if this index is using a
                // "classic-style" alias and has already rolled over, and we want to continue with the policy.
                if (aliasPointsToThisIndex && Boolean.TRUE.equals(isWriteIndex)) {
                    listener.onFailure(
                        new IllegalStateException(
                            String.format(
                                Locale.ROOT,
                                "index [%s] has [%s] set to [true], but is still the write index for alias [%s]",
                                index.getName(),
                                LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE,
                                rolloverAlias
                            )
                        )
                    );
                    return;
                }

                listener.onResponse(true, EmptyInfo.INSTANCE);
                return;
            }

            // If indexing_complete is *not* set, and the alias does not point to this index, we can't roll over this index, so error out.
            if (aliasPointsToThisIndex == false) {
                listener.onFailure(
                    new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "%s [%s] does not point to index [%s]",
                            RolloverAction.LIFECYCLE_ROLLOVER_ALIAS,
                            rolloverAlias,
                            index.getName()
                        )
                    )
                );
                return;
            }

            // Similarly, if isWriteIndex is false (see note above on false vs. null), we can't roll over this index, so error out.
            if (Boolean.FALSE.equals(isWriteIndex)) {
                listener.onFailure(
                    new IllegalArgumentException(
                        String.format(Locale.ROOT, "index [%s] is not the write index for alias [%s]", index.getName(), rolloverAlias)
                    )
                );
                return;
            }

            rolloverTarget = rolloverAlias;
        }

        // if we should only rollover if not empty, *and* if neither an explicit min_docs nor an explicit min_primary_shard_docs
        // has been specified on this policy, then inject a default min_docs: 1 condition so that we do not rollover empty indices
        boolean rolloverOnlyIfHasDocuments = LifecycleSettings.LIFECYCLE_ROLLOVER_ONLY_IF_HAS_DOCUMENTS_SETTING.get(metadata.settings());
        RolloverRequest rolloverRequest = createRolloverRequest(rolloverTarget, masterTimeout, rolloverOnlyIfHasDocuments);

        getClient().admin()
            .indices()
            .rolloverIndex(
                rolloverRequest,
                ActionListener.wrap(
                    response -> listener.onResponse(
                        rolloverRequest.getConditions().areConditionsMet(response.getConditionStatus()),
                        EmptyInfo.INSTANCE
                    ),
                    listener::onFailure
                )
            );
    }

    /**
     * Builds a RolloverRequest that captures the various max_* and min_* conditions of this {@link WaitForRolloverReadyStep}.
     * <p>
     * To prevent empty indices from rolling over, a `min_docs: 1` condition will be injected if `rolloverOnlyIfHasDocuments` is true
     * and the request doesn't already have an associated min_docs or min_primary_shard_docs condition.
     *
     * @param rolloverTarget             the index to rollover
     * @param masterTimeout              the master timeout to use with the request
     * @param rolloverOnlyIfHasDocuments whether to inject a min_docs 1 condition if there is not already a min_docs
     *                                   (or min_primary_shard_docs) condition
     * @return A RolloverRequest suitable for passing to {@code rolloverIndex(...) }.
     */
    // visible for testing
    RolloverRequest createRolloverRequest(String rolloverTarget, TimeValue masterTimeout, boolean rolloverOnlyIfHasDocuments) {
        RolloverRequest rolloverRequest = new RolloverRequest(rolloverTarget, null).masterNodeTimeout(masterTimeout);
        rolloverRequest.dryRun(true);
        if (rolloverOnlyIfHasDocuments && (conditions.getMinDocs() == null && conditions.getMinPrimaryShardDocs() == null)) {
            rolloverRequest.setConditions(RolloverConditions.newBuilder(conditions).addMinIndexDocsCondition(1L).build());
        } else {
            rolloverRequest.setConditions(conditions);
        }
        long currentMaxPrimaryShardDocs = rolloverRequest.getConditions().getMaxPrimaryShardDocs() != null
            ? rolloverRequest.getConditions().getMaxPrimaryShardDocs()
            : Long.MAX_VALUE;
        if (currentMaxPrimaryShardDocs > MAX_PRIMARY_SHARD_DOCS) {
            Map<String, Condition<?>> conditions = new HashMap<>(rolloverRequest.getConditions().getConditions());
            conditions.put(MaxPrimaryShardDocsCondition.NAME, new MaxPrimaryShardDocsCondition(MAX_PRIMARY_SHARD_DOCS));
            rolloverRequest.setConditions(new RolloverConditions(conditions));
        }
        return rolloverRequest;
    }

    public RolloverConditions getConditions() {
        return conditions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), conditions);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        WaitForRolloverReadyStep other = (WaitForRolloverReadyStep) obj;
        return super.equals(obj) && Objects.equals(conditions, other.conditions);
    }

    // We currently have no information to provide for this AsyncWaitStep, so this is an empty object
    private static final class EmptyInfo implements ToXContentObject {

        static final EmptyInfo INSTANCE = new EmptyInfo();

        private EmptyInfo() {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) {
            return builder;
        }
    }
}

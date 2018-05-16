/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;

import java.io.IOException;
import java.util.Objects;

public class ReplicasAllocatedStep extends ClusterStateWaitStep {
    public static final String NAME = "enough-shards-allocated";
    private int numberReplicas;

    public ReplicasAllocatedStep(StepKey key, StepKey nextStepKey, int numberReplicas) {
        super(key, nextStepKey);
        this.numberReplicas = numberReplicas;
    }

    int getNumberReplicas() {
        return numberReplicas;
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        IndexMetaData idxMeta = clusterState.metaData().index(index);
        if (idxMeta == null) {
            throw new IndexNotFoundException("Index not found when executing " + getKey().getAction() + " lifecycle action.",
                    index.getName());
        }
        // We only want to make progress if the cluster state reflects the number of replicas change and all shards are active
        boolean allShardsActive = ActiveShardCount.ALL.enoughShardsActive(clusterState, index.getName());
        boolean isConditionMet = idxMeta.getNumberOfReplicas() == numberReplicas && allShardsActive;
        if (isConditionMet) {
            return new Result(true, null);
        } else {
            return new Result(false, new Info(numberReplicas, idxMeta.getNumberOfReplicas(), allShardsActive));
        }
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), numberReplicas);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ReplicasAllocatedStep other = (ReplicasAllocatedStep) obj;
        return super.equals(obj) &&
                Objects.equals(numberReplicas, other.numberReplicas);
    }
    
    public static final class Info implements ToXContentObject {

        private final long expectedReplicas;
        private final long actualReplicas;
        private final boolean allShardsActive;
        private final String message;

        static final ParseField EXPECTED_REPLICAS = new ParseField("expected_replicas");
        static final ParseField ACTUAL_REPLICAS = new ParseField("actual_replicas");
        static final ParseField ALL_SHARDS_ACTIVE = new ParseField("all_shards_active");
        static final ParseField MESSAGE = new ParseField("message");
        static final ConstructingObjectParser<Info, Void> PARSER = new ConstructingObjectParser<>("replicas_allocated_step_info",
                a -> new Info((long) a[0], (long) a[1], (boolean) a[2]));
        static {
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), EXPECTED_REPLICAS);
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), ACTUAL_REPLICAS);
            PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ALL_SHARDS_ACTIVE);
            PARSER.declareString((i, s) -> {}, MESSAGE);
        }

        public Info(long expectedReplicas, long actualReplicas, boolean allShardsActive) {
            this.expectedReplicas = expectedReplicas;
            this.actualReplicas = actualReplicas;
            this.allShardsActive = allShardsActive;
            if (actualReplicas != expectedReplicas) {
                message = "Waiting for " + IndexMetaData.SETTING_NUMBER_OF_REPLICAS + " to be updated to " + expectedReplicas;
            } else if (allShardsActive == false) {
                message = "Waiting for all shard copies to be active";
            } else {
                message = "";
            }
        }

        public long getExpectedReplicas() {
            return expectedReplicas;
        }
        
        public long getActualReplicas() {
            return actualReplicas;
        }
        
        public boolean allShardsActive() {
            return allShardsActive;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MESSAGE.getPreferredName(), message);
            builder.field(EXPECTED_REPLICAS.getPreferredName(), expectedReplicas);
            builder.field(ACTUAL_REPLICAS.getPreferredName(), actualReplicas);
            builder.field(ALL_SHARDS_ACTIVE.getPreferredName(), allShardsActive);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(expectedReplicas, actualReplicas, allShardsActive);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Info other = (Info) obj;
            return Objects.equals(expectedReplicas, other.expectedReplicas) &&
                    Objects.equals(actualReplicas, other.actualReplicas) &&
                    Objects.equals(allShardsActive, other.allShardsActive);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}

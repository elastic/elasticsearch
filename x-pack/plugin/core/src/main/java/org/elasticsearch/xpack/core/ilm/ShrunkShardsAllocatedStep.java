/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Objects;

/**
 * Checks whether all shards in a shrunken index have been successfully allocated.
 */
public class ShrunkShardsAllocatedStep extends ClusterStateWaitStep {
    public static final String NAME = "shrunk-shards-allocated";
    private String shrunkIndexPrefix;

    public ShrunkShardsAllocatedStep(StepKey key, StepKey nextStepKey, String shrunkIndexPrefix) {
        super(key, nextStepKey);
        this.shrunkIndexPrefix = shrunkIndexPrefix;
    }

    String getShrunkIndexPrefix() {
        return shrunkIndexPrefix;
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        // We only want to make progress if all shards of the shrunk index are
        // active
        boolean indexExists = clusterState.metaData().index(shrunkIndexPrefix + index.getName()) != null;
        if (indexExists == false) {
            return new Result(false, new Info(false, -1, false));
        }
        boolean allShardsActive = ActiveShardCount.ALL.enoughShardsActive(clusterState, shrunkIndexPrefix + index.getName());
        int numShrunkIndexShards = clusterState.metaData().index(shrunkIndexPrefix + index.getName()).getNumberOfShards();
        if (allShardsActive) {
            return new Result(true, null);
        } else {
            return new Result(false, new Info(true, numShrunkIndexShards, allShardsActive));
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), shrunkIndexPrefix);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ShrunkShardsAllocatedStep other = (ShrunkShardsAllocatedStep) obj;
        return super.equals(obj) && Objects.equals(shrunkIndexPrefix, other.shrunkIndexPrefix);
    }

    public static final class Info implements ToXContentObject {

        private final int actualShards;
        private final boolean shrunkIndexExists;
        private final boolean allShardsActive;
        private final String message;

        static final ParseField ACTUAL_SHARDS = new ParseField("actual_shards");
        static final ParseField SHRUNK_INDEX_EXISTS = new ParseField("shrunk_index_exists");
        static final ParseField ALL_SHARDS_ACTIVE = new ParseField("all_shards_active");
        static final ParseField MESSAGE = new ParseField("message");
        static final ConstructingObjectParser<Info, Void> PARSER = new ConstructingObjectParser<>("shrunk_shards_allocated_step_info",
                a -> new Info((boolean) a[0], (int) a[1], (boolean) a[2]));
        static {
            PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), SHRUNK_INDEX_EXISTS);
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), ACTUAL_SHARDS);
            PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ALL_SHARDS_ACTIVE);
            PARSER.declareString((i, s) -> {}, MESSAGE);
        }

        public Info(boolean shrunkIndexExists, int actualShards, boolean allShardsActive) {
            this.actualShards = actualShards;
            this.shrunkIndexExists = shrunkIndexExists;
            this.allShardsActive = allShardsActive;
            if (shrunkIndexExists == false) {
                message = "Waiting for shrunk index to be created";
            } else if (allShardsActive == false) {
                message = "Waiting for all shard copies to be active";
            } else {
                message = "";
            }
        }

        public int getActualShards() {
            return actualShards;
        }

        public boolean shrunkIndexExists() {
            return shrunkIndexExists;
        }

        public boolean allShardsActive() {
            return allShardsActive;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MESSAGE.getPreferredName(), message);
            builder.field(SHRUNK_INDEX_EXISTS.getPreferredName(), shrunkIndexExists);
            builder.field(ACTUAL_SHARDS.getPreferredName(), actualShards);
            builder.field(ALL_SHARDS_ACTIVE.getPreferredName(), allShardsActive);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(shrunkIndexExists, actualShards, allShardsActive);
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
            return Objects.equals(shrunkIndexExists, other.shrunkIndexExists) &&
                    Objects.equals(actualShards, other.actualShards) &&
                    Objects.equals(allShardsActive, other.allShardsActive);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}

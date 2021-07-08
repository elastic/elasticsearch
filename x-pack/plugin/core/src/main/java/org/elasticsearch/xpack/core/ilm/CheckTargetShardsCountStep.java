/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * This step checks whether the new shrunken index's shards count is a factor of the source index's shards count.
 */
public class CheckTargetShardsCountStep extends ClusterStateWaitStep {

    public static final String NAME = "check-target-shards-count";

    private final Integer numberOfShards;

    private static final Logger logger = LogManager.getLogger(CheckTargetShardsCountStep.class);

    CheckTargetShardsCountStep(StepKey key, StepKey nextStepKey, Integer numberOfShards) {
        super(key, nextStepKey);
        this.numberOfShards = numberOfShards;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    public Integer getNumberOfShards() {
        return numberOfShards;
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        IndexMetadata indexMetadata = clusterState.metadata().index(index);
        String indexName = indexMetadata.getIndex().getName();
        if (numberOfShards != null) {
            int sourceNumberOfShards = indexMetadata.getNumberOfShards();
            if (sourceNumberOfShards % numberOfShards != 0) {
                String errorMessage = String.format(Locale.ROOT, "the target shards count [%d] must be a factor of the source index [%s]" +
                    "'s shards count [%d]", numberOfShards, indexName, sourceNumberOfShards);
                logger.debug(errorMessage);
                return new Result(false, new Info(errorMessage));
            }
        }

        return new Result(true, null);
    }

    static final class Info implements ToXContentObject {

        private final String message;

        static final ParseField MESSAGE = new ParseField("message");

        Info(String message) {
            this.message = message;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MESSAGE.getPreferredName(), message);
            builder.endObject();
            return builder;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Info info = (Info) o;
            return Objects.equals(message, info.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(message);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Optionally derives the index name using the provided prefix (if any) and waits for the status of the index to be GREEN.
 */
public class WaitForGreenIndexHealthStep extends ClusterStateWaitStep {

    public static final String NAME = "wait-for-green-health";
    private static final Logger logger = LogManager.getLogger(WaitForGreenIndexHealthStep.class);

    private final String indexNamePrefix;

    WaitForGreenIndexHealthStep(StepKey key, StepKey nextStepKey, @Nullable String indexNamePrefix) {
        super(key, nextStepKey);
        this.indexNamePrefix = indexNamePrefix;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        String indexName = indexNamePrefix != null ? indexNamePrefix + index.getName() : index.getName();
        IndexMetaData indexMetaData = clusterState.metaData().index(index);

        if (indexMetaData == null) {
            String errorMessage = String.format(Locale.ROOT, "[%s] lifecycle action for index [%s] executed but index no longer exists",
                getKey().getAction(), indexName);
            // Index must have been since deleted
            logger.debug(errorMessage);
            return new Result(false, new Info(errorMessage));
        }

        IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexMetaData.getIndex());
        if (indexRoutingTable == null) {
            return new Result(false, new Info("routing table not available for index [" + indexName + "]"));
        }

        ClusterIndexHealth indexHealth = new ClusterIndexHealth(indexMetaData, indexRoutingTable);
        if (indexHealth.getStatus() == ClusterHealthStatus.GREEN) {
            return new Result(true, null);
        } else {
            return new Result(false, new Info("status for index [" + indexName + "] is [" + indexHealth.getStatus() + "]"));
        }
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

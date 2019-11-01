/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;

final class WaitForIndexingCompleteStep extends ClusterStateWaitStep {
    private static final Logger logger = LogManager.getLogger(WaitForIndexingCompleteStep.class);

    static final String NAME = "wait-for-indexing-complete";

    WaitForIndexingCompleteStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        IndexMetaData followerIndex = clusterState.metaData().index(index);
        if (followerIndex == null) {
            // Index must have been since deleted, ignore it
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().getAction(), index.getName());
            return new Result(false, null);
        }
        Map<String, String> customIndexMetadata = followerIndex.getCustomData(CCR_METADATA_KEY);
        if (customIndexMetadata == null) {
            return new Result(true, null);
        }

        boolean indexingComplete = LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.get(followerIndex.getSettings());
        if (indexingComplete) {
            return new Result(true, null);
        } else {
            return new Result(false, new IndexingNotCompleteInfo());
        }
    }

    static final class IndexingNotCompleteInfo implements ToXContentObject {

        static final ParseField MESSAGE_FIELD = new ParseField("message");
        static final ParseField INDEXING_COMPLETE = new ParseField(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE);

        private final String message;

        IndexingNotCompleteInfo() {
            this.message = "waiting for the [" + LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE +
                "] setting to be set to true on the leader index, it is currently [false]";
        }

        String getMessage() {
            return message;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MESSAGE_FIELD.getPreferredName(), message);
            builder.field(INDEXING_COMPLETE.getPreferredName(), false);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IndexingNotCompleteInfo info = (IndexingNotCompleteInfo) o;
            return Objects.equals(getMessage(), info.getMessage());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getMessage());
        }
    }
}

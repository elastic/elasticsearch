/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.indexlifecycle.UnfollowAction.CCR_METADATA_KEY;

final class WaitForIndexingCompleteStep extends ClusterStateWaitStep {

    static final String NAME = "wait-for-indexing-complete";

    WaitForIndexingCompleteStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        IndexMetaData followerIndex = clusterState.metaData().getIndexSafe(index);
        Map<String, String> customIndexMetadata = followerIndex.getCustomData(CCR_METADATA_KEY);
        if (customIndexMetadata == null) {
            return new Result(true, null);
        }

        boolean indexingComplete = LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.get(followerIndex.getSettings());
        if (indexingComplete) {
            return new Result(true, null);
        } else {
            return new Result(false, new Info(followerIndex.getSettings()));
        }
    }

    static final class Info implements ToXContentObject {

        static final ParseField MESSAGE_FIELD = new ParseField("message");
        static final ParseField INDEX_SETTINGS_FIELD = new ParseField("index_settings");

        private final String message;
        private final Settings indexSettings;

        Info(Settings indexSettings) {
            this.message = "the [" + LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE + "] setting has not been set to true";
            this.indexSettings = indexSettings;
        }

        String getMessage() {
            return message;
        }

        Settings getIndexSettings() {
            return indexSettings;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MESSAGE_FIELD.getPreferredName(), message);
            builder.startObject(INDEX_SETTINGS_FIELD.getPreferredName());
            indexSettings.toXContent(builder, params);
            builder.endObject();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Info info = (Info) o;
            return Objects.equals(indexSettings, info.indexSettings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexSettings);
        }
    }
}

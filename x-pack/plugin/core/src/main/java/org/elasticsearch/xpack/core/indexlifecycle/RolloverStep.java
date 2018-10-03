/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class RolloverStep extends AsyncWaitStep {
    public static final String NAME = "attempt_rollover";

    private ByteSizeValue maxSize;
    private TimeValue maxAge;
    private Long maxDocs;

    public RolloverStep(StepKey key, StepKey nextStepKey, Client client, ByteSizeValue maxSize, TimeValue maxAge,
            Long maxDocs) {
        super(key, nextStepKey, client);
        this.maxSize = maxSize;
        this.maxAge = maxAge;
        this.maxDocs = maxDocs;
    }

    @Override
    public void evaluateCondition(IndexMetaData indexMetaData, Listener listener) {
        String rolloverAlias = RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.get(indexMetaData.getSettings());

        if (Strings.isNullOrEmpty(rolloverAlias)) {
            listener.onFailure(new IllegalArgumentException(String.format(Locale.ROOT,
                "setting [%s] for index [%s] is empty or not defined", RolloverAction.LIFECYCLE_ROLLOVER_ALIAS,
                indexMetaData.getIndex().getName())));
            return;
        }

        RolloverRequest rolloverRequest = new RolloverRequest(rolloverAlias, null);
        if (maxAge != null) {
            rolloverRequest.addMaxIndexAgeCondition(maxAge);
        }
        if (maxSize != null) {
            rolloverRequest.addMaxIndexSizeCondition(maxSize);
        }
        if (maxDocs != null) {
            rolloverRequest.addMaxIndexDocsCondition(maxDocs);
        }
        getClient().admin().indices().rolloverIndex(rolloverRequest,
                ActionListener.wrap(response -> listener.onResponse(response.isRolledOver(), new EmptyInfo()), listener::onFailure));
    }

    ByteSizeValue getMaxSize() {
        return maxSize;
    }

    TimeValue getMaxAge() {
        return maxAge;
    }

    Long getMaxDocs() {
        return maxDocs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxSize, maxAge, maxDocs);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RolloverStep other = (RolloverStep) obj;
        return super.equals(obj) &&
                Objects.equals(maxSize, other.maxSize) &&
                Objects.equals(maxAge, other.maxAge) &&
                Objects.equals(maxDocs, other.maxDocs);
    }

    // We currently have no information to provide for this AsyncWaitStep, so this is an empty object
    private class EmptyInfo implements ToXContentObject {
        private EmptyInfo() {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }
    }
}

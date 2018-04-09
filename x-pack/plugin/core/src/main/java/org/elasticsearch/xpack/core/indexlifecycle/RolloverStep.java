/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;

import java.util.Objects;

public class RolloverStep extends AsyncActionStep {
    public static final String NAME = "attempt_rollover";

    private String alias;
    private ByteSizeValue maxSize;
    private TimeValue maxAge;
    private Long maxDocs;

    public RolloverStep(StepKey key, StepKey nextStepKey, Client client, String alias, ByteSizeValue maxSize, TimeValue maxAge,
            Long maxDocs) {
        super(key, nextStepKey, client);
        this.alias = alias;
        this.maxSize = maxSize;
        this.maxAge = maxAge;
        this.maxDocs = maxDocs;
    }

    @Override
    public void performAction(Index index, Listener listener) {
        RolloverRequest rolloverRequest = new RolloverRequest(alias, null);
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
                ActionListener.wrap(response -> listener.onResponse(response.isRolledOver()), listener::onFailure));
    }
    
    String getAlias() {
        return alias;
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
        return Objects.hash(super.hashCode(), alias, maxSize, maxAge, maxDocs);
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
                Objects.equals(alias, other.alias) &&
                Objects.equals(maxSize, other.maxSize) &&
                Objects.equals(maxAge, other.maxAge) &&
                Objects.equals(maxDocs, other.maxDocs);
    }

}

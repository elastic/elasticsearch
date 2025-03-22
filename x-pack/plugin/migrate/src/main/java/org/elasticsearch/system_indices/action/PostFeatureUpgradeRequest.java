/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;

/**
 * Request to begin an upgrade of system features
 */
public class PostFeatureUpgradeRequest extends MasterNodeRequest<PostFeatureUpgradeRequest> {

    public PostFeatureUpgradeRequest(TimeValue masterNodeTimeout) {
        super(masterNodeTimeout);
    }

    public PostFeatureUpgradeRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}

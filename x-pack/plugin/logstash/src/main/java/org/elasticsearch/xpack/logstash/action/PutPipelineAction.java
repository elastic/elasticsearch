/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.action.ActionType;

public class PutPipelineAction extends ActionType<PutPipelineResponse> {

    public static final String NAME = "cluster:admin/logstash/pipeline/put";
    public static final PutPipelineAction INSTANCE = new PutPipelineAction();

    private PutPipelineAction() {
        super(NAME, PutPipelineResponse::new);
    }
}

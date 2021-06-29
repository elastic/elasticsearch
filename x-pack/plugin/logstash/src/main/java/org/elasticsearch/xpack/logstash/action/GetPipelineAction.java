/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.action.ActionType;

public class GetPipelineAction extends ActionType<GetPipelineResponse> {

    public static final String NAME = "cluster:admin/logstash/pipeline/get";
    public static final GetPipelineAction INSTANCE = new GetPipelineAction();

    private GetPipelineAction() {
        super(NAME, GetPipelineResponse::new);
    }
}

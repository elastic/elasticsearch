/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.action.ActionType;

public class DeletePipelineAction extends ActionType<DeletePipelineResponse> {

    public static final String NAME = "cluster:admin/logstash/pipeline/delete";
    public static final DeletePipelineAction INSTANCE = new DeletePipelineAction();

    private DeletePipelineAction() {
        super(NAME, DeletePipelineResponse::new);
    }
}

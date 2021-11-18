/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.action.ActionType;

public class GetPipelineAction extends ActionType<GetPipelineResponse> {

    public static final GetPipelineAction INSTANCE = new GetPipelineAction();
    public static final String NAME = "cluster:admin/ingest/pipeline/get";

    public GetPipelineAction() {
        super(NAME, GetPipelineResponse::new);
    }
}

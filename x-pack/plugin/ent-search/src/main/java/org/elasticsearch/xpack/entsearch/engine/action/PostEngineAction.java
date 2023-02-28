/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.action.ActionType;

public class PostEngineAction extends ActionType<PutEngineAction.Response> {

    public static final PostEngineAction INSTANCE = new PostEngineAction();
    public static final String NAME = "indices:admin/engine/post";

    public PostEngineAction() {
        super(NAME, PutEngineAction.Response::new);
    }

}

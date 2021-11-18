/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.explain;

import org.elasticsearch.action.ActionType;

/**
 * Entry point for the explain feature.
 */
public class ExplainAction extends ActionType<ExplainResponse> {

    public static final ExplainAction INSTANCE = new ExplainAction();
    public static final String NAME = "indices:data/read/explain";

    private ExplainAction() {
        super(NAME, ExplainResponse::new);
    }

}

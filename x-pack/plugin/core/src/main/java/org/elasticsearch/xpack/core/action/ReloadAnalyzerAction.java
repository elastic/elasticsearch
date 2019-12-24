/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionType;

public class ReloadAnalyzerAction extends ActionType<ReloadAnalyzersResponse> {

    public static final ReloadAnalyzerAction INSTANCE = new ReloadAnalyzerAction();
    public static final String NAME = "indices:admin/reload_analyzers";

    private ReloadAnalyzerAction() {
        super(NAME, ReloadAnalyzersResponse::new);
    }
}

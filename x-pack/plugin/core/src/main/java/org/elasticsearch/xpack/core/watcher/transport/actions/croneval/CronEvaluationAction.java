/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.croneval;

import org.elasticsearch.action.Action;

public class CronEvaluationAction extends Action<CronEvaluationResponse> {

    public static final CronEvaluationAction INSTANCE = new CronEvaluationAction();
    public static final String NAME = "cluster:admin/xpack/watcher/watch/croneval";

    private CronEvaluationAction() {
        super(NAME);
    }

    @Override
    public CronEvaluationResponse newResponse() {
        return new CronEvaluationResponse();
    }
}

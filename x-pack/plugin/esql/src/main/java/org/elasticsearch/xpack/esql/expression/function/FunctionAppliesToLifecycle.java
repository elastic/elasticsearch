/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

public enum FunctionAppliesToLifecycle {
    PREVIEW(true),
    BETA(false),
    DEVELOPMENT(false),
    DEPRECATED(true),
    COMING(true),
    DISCONTINUED(false),
    UNAVAILABLE(false),
    GA(true);

    private final boolean serverless;

    FunctionAppliesToLifecycle(boolean serverless) {
        this.serverless = serverless;
    }

    public FunctionAppliesToLifecycle serverlessLifecycle() {
        return serverless ? GA : this;
    }
}

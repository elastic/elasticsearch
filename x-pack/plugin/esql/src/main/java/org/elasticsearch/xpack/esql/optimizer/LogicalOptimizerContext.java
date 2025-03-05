/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.Objects;

public class LogicalOptimizerContext {
    private final Configuration configuration;
    private final FoldContext foldCtx;

    public LogicalOptimizerContext(Configuration configuration, FoldContext foldCtx) {
        this.configuration = configuration;
        this.foldCtx = foldCtx;
    }

    public Configuration configuration() {
        return configuration;
    }

    public FoldContext foldCtx() {
        return foldCtx;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (LogicalOptimizerContext) obj;
        return this.configuration.equals(that.configuration) && this.foldCtx.equals(that.foldCtx);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configuration, foldCtx);
    }

    @Override
    public String toString() {
        return "LogicalOptimizerContext[configuration=" + configuration + ", foldCtx=" + foldCtx + ']';
    }

}

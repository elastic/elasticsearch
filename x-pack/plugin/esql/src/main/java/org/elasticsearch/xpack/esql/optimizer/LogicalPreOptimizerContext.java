/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.core.expression.FoldContext;

import java.util.Objects;

public class LogicalPreOptimizerContext {

    private final FoldContext foldCtx;

    public LogicalPreOptimizerContext(FoldContext foldCtx) {
        this.foldCtx = foldCtx;
    }

    public FoldContext foldCtx() {
        return foldCtx;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (LogicalPreOptimizerContext) obj;
        return this.foldCtx.equals(that.foldCtx);
    }

    @Override
    public int hashCode() {
        return Objects.hash(foldCtx);
    }

    @Override
    public String toString() {
        return "LogicalPreOptimizerContext[foldCtx=" + foldCtx + ']';
    }
}

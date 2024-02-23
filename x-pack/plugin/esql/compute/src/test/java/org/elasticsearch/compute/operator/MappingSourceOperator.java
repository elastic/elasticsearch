/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Page;

public abstract class MappingSourceOperator extends SourceOperator {
    private final SourceOperator delegate;

    public MappingSourceOperator(SourceOperator delegate) {
        this.delegate = delegate;
    }

    protected abstract Page map(Page page);

    @Override
    public void finish() {
        delegate.finish();
    }

    @Override
    public boolean isFinished() {
        return delegate.isFinished();
    }

    @Override
    public Page getOutput() {
        Page p = delegate.getOutput();
        if (p == null) {
            return p;
        }
        return map(p);
    }

    @Override
    public void close() {
        delegate.close();
    }
}

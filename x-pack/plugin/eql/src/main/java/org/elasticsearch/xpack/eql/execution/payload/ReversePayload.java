/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.payload;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.eql.session.Payload;
import org.elasticsearch.xpack.eql.session.Results.Type;

import java.util.Collections;
import java.util.List;

public class ReversePayload implements Payload {

    private final Payload delegate;

    public ReversePayload(Payload delegate) {
        this.delegate = delegate;
        Collections.reverse(delegate.values());
    }

    @Override
    public Type resultType() {
        return delegate.resultType();
    }

    @Override
    public boolean timedOut() {
        return delegate.timedOut();
    }

    @Override
    public TimeValue timeTook() {
        return delegate.timeTook();
    }

    @Override
    public <V> List<V> values() {
        return delegate.values();
    }
}

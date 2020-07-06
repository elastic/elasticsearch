/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.session;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.eql.session.Results.Type;

import java.util.List;

import static java.util.Collections.emptyList;

public class EmptyPayload implements Payload {

    private final Type type;

    public EmptyPayload(Type type) {
        this.type = type;
    }

    @Override
    public Type resultType() {
        return type;
    }

    @Override
    public boolean timedOut() {
        return false;
    }

    @Override
    public TimeValue timeTook() {
        return TimeValue.ZERO;
    }

    @Override
    public <V> List<V> values() {
        return emptyList();
    }
}

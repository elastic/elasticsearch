/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.condition;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class AlwaysCondition implements Condition {
    public static final String TYPE = "always";
    public static final Condition INSTANCE = new AlwaysCondition();

    protected AlwaysCondition() { }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof AlwaysCondition;
    }

    @Override
    public int hashCode() {
        // All instances has to produce the same hashCode because they are all equal
        return 0;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().endObject();
    }
}

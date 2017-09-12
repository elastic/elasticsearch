/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;
import java.util.function.Supplier;

public class SuppliedProcessor implements Processor {

    private final Supplier<Object> action;

    public SuppliedProcessor(Supplier<Object> action) {
        this.action = action;
    }

    @Override
    public String getWriteableName() {
        throw new SqlIllegalArgumentException("transient");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new SqlIllegalArgumentException("transient");
    }

    @Override
    public Object process(Object input) {
        return action.get();
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.logical;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;

import java.io.IOException;

public class NotProcessor implements Processor {
    
    public static final NotProcessor INSTANCE = new NotProcessor();

    public static final String NAME = "ln";

    private NotProcessor() {}

    public NotProcessor(StreamInput in) throws IOException {}

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public Object process(Object input) {
        return apply(input);
    }

    public static Boolean apply(Object input) {
        if (input == null) {
            return null;
        }
        
        if (!(input instanceof Boolean)) {
            throw new QlIllegalArgumentException("A boolean is required; received {}", input);
        }

        return ((Boolean) input).booleanValue() ? Boolean.FALSE : Boolean.TRUE;
    }

    @Override
    public int hashCode() {
        return NotProcessor.class.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        return obj == null || getClass() != obj.getClass();
    }
}
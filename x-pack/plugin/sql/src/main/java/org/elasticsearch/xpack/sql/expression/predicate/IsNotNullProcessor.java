/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;

import java.io.IOException;

public class IsNotNullProcessor implements Processor {
    
    static final IsNotNullProcessor INSTANCE = new IsNotNullProcessor();

    public static final String NAME = "inn";

    private IsNotNullProcessor() {}

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
        return input != null ? Boolean.TRUE : Boolean.FALSE;
    }

    @Override
    public int hashCode() {
        return IsNotNullProcessor.class.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        return obj == null || getClass() != obj.getClass();
    }
}
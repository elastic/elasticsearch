/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;

import java.io.IOException;

public class NotLogicProcessor implements Processor {

    private static final String NAME = "not";

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }

    @Override
    public Boolean process(Object input) {
        if (input == null) {
            return null;
        }
        return !((Boolean) input);
    }
}

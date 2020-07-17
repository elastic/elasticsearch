/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.expression.gen.pipeline;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.List;

public abstract class MultiPipe extends Pipe {

    protected MultiPipe(Source source, Expression expression, List<Pipe> children) {
        super(source, expression, children);
    }

    @Override
    public Processor asProcessor() {
        List<Processor> procs = new ArrayList<>();
        for (Pipe pipe : children()) {
            procs.add(pipe.asProcessor());
        }

        return asProcessor(procs);
    }

    public abstract Processor asProcessor(List<Processor> procs);
}

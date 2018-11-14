/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.MultiPipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.List;

public class CoalescePipe extends MultiPipe {

    public CoalescePipe(Location location, Expression expression, List<Pipe> children) {
        super(location, expression, children);
    }

    @Override
    protected NodeInfo<CoalescePipe> info() {
        return NodeInfo.create(this, CoalescePipe::new, expression(), children());
    }

    @Override
    public Pipe replaceChildren(List<Pipe> newChildren) {
        return new CoalescePipe(location(), expression(), newChildren);
    }

    @Override
    public Processor asProcessor(List<Processor> procs) {
        return new CoalesceProcessor(procs);
    }
}

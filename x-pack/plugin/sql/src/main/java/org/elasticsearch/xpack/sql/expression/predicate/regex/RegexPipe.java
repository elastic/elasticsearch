/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.regex;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.BinaryPipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

public class RegexPipe extends BinaryPipe {

    public RegexPipe(Location location, Expression expression, Pipe left, Pipe right) {
        super(location, expression, left, right);
    }

    @Override
    protected NodeInfo<RegexPipe> info() {
        return NodeInfo.create(this, RegexPipe::new, expression(), left(), right());
    }

    @Override
    protected BinaryPipe replaceChildren(Pipe left, Pipe right) {
        return new RegexPipe(location(), expression(), left, right);
    }

    @Override
    public RegexProcessor asProcessor() {
        return new RegexProcessor(left().asProcessor(), right().asProcessor());
    }
}

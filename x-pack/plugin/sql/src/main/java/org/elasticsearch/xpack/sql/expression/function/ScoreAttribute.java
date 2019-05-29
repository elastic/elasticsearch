/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.Nullability;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.ScorePipe;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import static org.elasticsearch.xpack.sql.expression.Nullability.FALSE;

/**
 * {@link Attribute} that represents Elasticsearch's {@code _score}.
 */
public class ScoreAttribute extends FunctionAttribute {
    /**
     * Constructor for normal use.
     */
    public ScoreAttribute(Source source) {
        this(source, "SCORE()", DataType.FLOAT, null, FALSE, null, false);
    }

    /**
     * Constructor for {@link #clone()}
     */
    private ScoreAttribute(Source source, String name, DataType dataType, String qualifier, Nullability nullability, ExpressionId id,
                           boolean synthetic) {
        super(source, name, dataType, qualifier, nullability, id, synthetic, "SCORE");
    }

    @Override
    protected NodeInfo<ScoreAttribute> info() {
        return NodeInfo.create(this);
    }

    @Override
    protected Attribute clone(Source source, String name, String qualifier, Nullability nullability,
                              ExpressionId id, boolean synthetic) {
        return new ScoreAttribute(source, name, dataType(), qualifier, nullability, id, synthetic);
    }

    @Override
    protected Pipe makePipe() {
        return new ScorePipe(source(), this);
    }

    @Override
    protected String label() {
        return "SCORE";
    }
}

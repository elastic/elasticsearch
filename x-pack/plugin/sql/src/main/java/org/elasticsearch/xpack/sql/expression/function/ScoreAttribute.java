/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.ScorePipe;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

/**
 * {@link Attribute} that represents Elasticsearch's {@code _score}.
 */
public class ScoreAttribute extends FunctionAttribute {
    /**
     * Constructor for normal use.
     */
    public ScoreAttribute(Location location) {
        this(location, "SCORE()", DataType.FLOAT, null, false, null, false);
    }

    /**
     * Constructor for {@link #clone()}
     */
    private ScoreAttribute(Location location, String name, DataType dataType, String qualifier, boolean nullable, ExpressionId id,
                           boolean synthetic) {
        super(location, name, dataType, qualifier, nullable, id, synthetic, "SCORE");
    }

    @Override
    protected NodeInfo<ScoreAttribute> info() {
        return NodeInfo.create(this);
    }

    @Override
    protected Attribute clone(Location location, String name, String qualifier, boolean nullable, ExpressionId id, boolean synthetic) {
        return new ScoreAttribute(location, name, dataType(), qualifier, nullable, id, synthetic);
    }

    @Override
    protected Pipe makePipe() {
        return new ScorePipe(location(), this);
    }

    @Override
    protected String label() {
        return "SCORE";
    }
}

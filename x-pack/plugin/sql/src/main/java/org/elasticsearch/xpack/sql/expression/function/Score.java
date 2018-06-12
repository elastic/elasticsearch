/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import static java.util.Collections.emptyList;

import java.util.List;

/**
 * Function referring to the {@code _score} in a search. Only available
 * in the search context, and only at the "root" so it can't be combined
 * with other function.
 */
public class Score extends Function {
    public Score(Location location) {
        super(location, emptyList());
    }

    @Override
    protected NodeInfo<Score> info() {
        return NodeInfo.create(this);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
    }

    @Override
    public DataType dataType() {
        return DataType.FLOAT;
    }

    @Override
    public Attribute toAttribute() {
        return new ScoreAttribute(location());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        Score other = (Score) obj;
        return location().equals(other.location());
    }

    @Override
    public int hashCode() {
        return location().hashCode();
    }
}

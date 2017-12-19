/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

import static java.util.Collections.emptyList;

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
    public DataType dataType() {
        return DataTypes.FLOAT;
    }

    @Override
    public Attribute toAttribute() {
        return new ScoreAttribute(location());
    }
}

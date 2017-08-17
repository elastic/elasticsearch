/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Foldables;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

import static java.util.Collections.singletonList;

public class Percentile extends NumericAggregate implements EnclosedAgg {

    private final Expression percent;

    public Percentile(Location location, Expression field, Expression percent) {
        super(location, field, singletonList(percent));
        this.percent = percent;
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = super.resolveType();

        if (TypeResolution.TYPE_RESOLVED.equals(resolution)) {
            resolution = percent().dataType().isNumeric() ? TypeResolution.TYPE_RESOLVED :
                    new TypeResolution("Percentile#percent argument cannot be non-numeric (type is'%s')", percent().dataType().esName());
        }
        return resolution;
    }

    public Expression percent() {
        return percent;
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }

    @Override
    public String innerName() {
        return "[" + Double.toString(Foldables.doubleValueOf(percent)) + "]";
    }
}
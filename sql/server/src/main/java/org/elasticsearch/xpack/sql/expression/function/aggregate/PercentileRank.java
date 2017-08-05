/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.Foldables;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

import java.util.Objects;

import static java.util.Collections.singletonList;

public class PercentileRank extends AggregateFunction implements EnclosedAgg {

    private final Expression value;

    public PercentileRank(Location location, Expression field, Expression value) {
        super(location, field, singletonList(value));
        this.value = value;
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = field().dataType().isNumeric() ? TypeResolution.TYPE_RESOLVED : 
                    new TypeResolution("Function '%s' cannot be applied on a non-numeric expression ('%s' of type '%s')", 
                            functionName(), Expressions.name(field()), field().dataType().esName());

        if (TypeResolution.TYPE_RESOLVED.equals(resolution)) {
            resolution = value.dataType().isNumeric() ? TypeResolution.TYPE_RESOLVED :
                    new TypeResolution("PercentileRank#value argument cannot be non-numeric (type is'%s')", value.dataType().esName());
        }
        return resolution;
    }

    public Expression value() {
        return value;
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }

    @Override
    public String innerName() {
        return "[" + Double.toString(Foldables.doubleValueOf(value)) + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        PercentileRank other = (PercentileRank) obj;
        return Objects.equals(field(), other.field())
                && Objects.equals(value, other.value);
    }
}
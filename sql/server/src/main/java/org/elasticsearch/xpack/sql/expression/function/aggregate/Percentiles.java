/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;

import java.util.List;
import java.util.Objects;

public class Percentiles extends CompoundNumericAggregate {

    private final List<Expression> percents;

    public Percentiles(Location location, Expression field, List<Expression> percents) {
        super(location, field, percents);
        this.percents = percents;
    }

    public List<Expression> percents() {
        return percents;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        Percentiles other = (Percentiles) obj;
        return Objects.equals(field(), other.field())
                && Objects.equals(percents, other.percents);
    }
}
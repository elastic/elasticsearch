/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import java.util.Arrays;
import java.util.Objects;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

// The range is a mix of gt(e) AND lt(e) - it mainly exists since ES supports Range natively 
// and thus any comparison ends up translated to one
public class Range extends Expression {

    private final Expression value, lower, upper;
    private final boolean includeLower, includeUpper;
    
    public Range(Location location, Expression value, Expression lower, boolean includeLower, Expression upper, boolean includeUpper) {
        super(location, Arrays.asList(value, lower, upper));
        
        this.value = value;
        this.lower = lower;
        this.upper = upper;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
    }
    
    public Expression value() {
        return value;
    }
    
    public Expression lower() {
        return lower;
    }
    
    public Expression upper() {
        return upper;
    }
    
    public boolean includeLower() {
        return includeLower;
    }
    
    public boolean includeUpper() {
        return includeUpper;
    }

    @Override
    public boolean foldable() {
        return value.foldable() && lower.foldable() && upper.foldable();
    }

    @Override
    public boolean nullable() {
        return value.nullable() && lower.nullable() && upper.nullable();
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    public int hashCode() {
        return Objects.hash(includeLower, includeUpper, value, lower, upper);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        Range other = (Range) obj;
        return Objects.equals(includeLower, other.includeLower) 
                && Objects.equals(includeUpper, other.includeUpper)
                && Objects.equals(value, other.value)
                && Objects.equals(lower, other.lower)
                && Objects.equals(upper, other.upper);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(lower);
        sb.append(includeLower ? " <= " : " < ");
        sb.append(value);
        sb.append(includeUpper ? " <= " : " < ");
        sb.append(upper);
        return sb.toString();
    }
}
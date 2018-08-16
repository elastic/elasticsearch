/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

// BETWEEN or range - is a mix of gt(e) AND lt(e)
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

    @Override
    protected NodeInfo<Range> info() {
        return NodeInfo.create(this, Range::new, value, lower, includeLower, upper, includeUpper);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 3) {
            throw new IllegalArgumentException("expected [3] children but received [" + newChildren.size() + "]");
        }
        return new Range(location(), newChildren.get(0), newChildren.get(1), includeLower, newChildren.get(2), includeUpper);
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
        if (lower.foldable() && upper.foldable()) {
            return areBoundariesInvalid() || value.foldable();
        }

        return false;
    }

    @Override
    public Object fold() {
        if (areBoundariesInvalid()) {
            return Boolean.FALSE;
        }

        Object val = value.fold();
        Integer lowerCompare = BinaryComparison.compare(lower.fold(), val);
        Integer upperCompare = BinaryComparison.compare(val, upper().fold());
        boolean lowerComparsion = lowerCompare == null ? false : (includeLower ? lowerCompare <= 0 : lowerCompare < 0);
        boolean upperComparsion = upperCompare == null ? false : (includeUpper ? upperCompare <= 0 : upperCompare < 0);
        return lowerComparsion && upperComparsion;
    }

    /**
     * Check whether the boundaries are invalid ( upper &lt; lower) or not.
     * If they do, the value does not have to be evaluate.
     */
    private boolean areBoundariesInvalid() {
        Integer compare = BinaryComparison.compare(lower.fold(), upper.fold());
        // upper < lower OR upper == lower and the range doesn't contain any equals
        return compare != null && (compare > 0 || (compare == 0 && (!includeLower || !includeUpper)));
    }

    @Override
    public boolean nullable() {
        return value.nullable() && lower.nullable() && upper.nullable();
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
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

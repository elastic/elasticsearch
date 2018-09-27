/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.Foldables;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.Params;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptWeaver;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.EsField;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

// BETWEEN or range - is a mix of gt(e) AND lt(e)
public class Range extends NamedExpression implements ScriptWeaver {

    private final Expression value, lower, upper;
    private final boolean includeLower, includeUpper;

    public Range(Location location, Expression value, Expression lower, boolean includeLower, Expression upper, boolean includeUpper) {
        this(location, null, value, lower, includeLower, upper, includeUpper);
    }

    public Range(Location location, String name, Expression value, Expression lower, boolean includeLower, Expression upper,
            boolean includeUpper) {
        super(location, name == null ? defaultName(value, lower, upper, includeLower, includeUpper) : name,
                Arrays.asList(value, lower, upper), null);

        this.value = value;
        this.lower = lower;
        this.upper = upper;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
    }

    @Override
    protected NodeInfo<Range> info() {
        return NodeInfo.create(this, Range::new, name(), value, lower, includeLower, upper, includeUpper);
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
    public ScriptTemplate asScript() {
        ScriptTemplate scriptTemplate = asScript(value);

        String template = formatTemplate(format(Locale.ROOT, "({} %s %s) && (%s %s {})",
                        includeLower() ? "<=" : "<",
                        scriptTemplate.template(),
                        scriptTemplate.template(),
                        includeUpper() ? "<=" : "<"));

        Params params = paramsBuilder().variable(Foldables.valueOf(lower))
                .script(scriptTemplate.params())
                .script(scriptTemplate.params())
                .variable(Foldables.valueOf(upper))
                .build();

        return new ScriptTemplate(template, params, DataType.BOOLEAN);
    }

    @Override
    protected Pipe makePipe() {
        throw new SqlIllegalArgumentException("Not supported yet");
    }

    @Override
    public Attribute toAttribute() {
        return new FieldAttribute(location(), "not yet implemented",
                new EsField("not yet implemented", DataType.UNSUPPORTED, emptyMap(), false));
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

    private static String defaultName(Expression value, Expression lower, Expression upper, boolean includeLower, boolean includeUpper) {
        StringBuilder sb = new StringBuilder();
        sb.append(lower);
        sb.append(includeLower ? " <= " : " < ");
        sb.append(value);
        sb.append(includeUpper ? " <= " : " < ");
        sb.append(upper);
        return sb.toString();
    }

    @Override
    public String toString() {
        return name();
    }
}
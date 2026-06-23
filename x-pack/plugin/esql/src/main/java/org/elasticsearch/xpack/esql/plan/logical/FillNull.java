/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Replaces null values in specified fields (or all fields) with a given fill value
 * or type-appropriate defaults. Expands into an {@link Eval} with {@link Coalesce} aliases,
 * wrapped in a {@link Project} to preserve the original column order.
 * <p>
 * Without an explicit fill value, only numeric, string and boolean columns receive a default;
 * columns of any other type and all-null ({@code NULL}-typed) columns are left unchanged.
 * <p>
 * A filled column becomes a reference attribute (as with {@code EVAL col = COALESCE(col, ...)}),
 * so full-text functions and Lucene filter pushdown no longer treat it as an indexed field.
 */
public class FillNull extends UnaryPlan implements SurrogateLogicalPlan, PostAnalysisVerificationAware, TelemetryAware {

    private final @Nullable Expression fillValue;
    private final List<Attribute> targetFields;

    private List<Alias> lazyAliases;
    private List<Attribute> lazyOutput;

    public FillNull(Source source, LogicalPlan child, @Nullable Expression fillValue, List<Attribute> targetFields) {
        super(source, child);
        this.fillValue = fillValue;
        this.targetFields = targetFields;
    }

    @Nullable
    public Expression fillValue() {
        return fillValue;
    }

    public List<Attribute> targetFields() {
        return targetFields;
    }

    @Override
    public List<Attribute> output() {
        if (expressionsResolved() == false) {
            return child().output();
        }
        if (lazyOutput == null) {
            computeSurrogateInfo();
        }
        return lazyOutput;
    }

    @Override
    public boolean expressionsResolved() {
        if (fillValue != null && fillValue.resolved() == false) {
            return false;
        }
        for (Attribute field : targetFields) {
            if (field.resolved() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public FillNull replaceChild(LogicalPlan newChild) {
        FillNull copy = new FillNull(source(), newChild, fillValue, targetFields);
        // Once this FillNull's output() has been observed (e.g. by Analyzer.resolveFork or any plan rewrite that
        // builds a Project on top of it), the assigned alias NameIds are baked into upstream plan nodes.
        // Recomputing them after a child substitution would allocate fresh NameIds and break those upstream
        // references. Propagate the cached surrogate state when the new child preserves the same output identity
        // (which is the contract of surrogate substitution and most child rewrites).
        if (lazyAliases != null && lazyOutput != null && child().outputSet().equals(newChild.outputSet())) {
            copy.lazyAliases = lazyAliases;
            copy.lazyOutput = lazyOutput;
        }
        return copy;
    }

    public FillNull withTargetFields(List<Attribute> newTargetFields) {
        return new FillNull(source(), child(), fillValue, newTargetFields);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(
            this,
            (source, child, fillValue, targetFields) -> new FillNull(source, child, fillValue, targetFields),
            child(),
            fillValue,
            targetFields
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("doesn't escape the coordinator node");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the coordinator node");
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fillValue, targetFields);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FillNull other = (FillNull) obj;
        return super.equals(obj) && Objects.equals(fillValue, other.fillValue) && Objects.equals(targetFields, other.targetFields);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (fillValue != null && targetFields.isEmpty() == false) {
            for (Attribute field : targetFields) {
                if (field.resolved() && DataType.areCompatible(fillValue.dataType(), field.dataType()) == false) {
                    failures.add(
                        fail(
                            field,
                            "[FILLNULL] fill value type [{}] is incompatible with field [{}] type [{}]",
                            fillValue.dataType().typeName(),
                            field.name(),
                            field.dataType().typeName()
                        )
                    );
                }
            }
        }
        Set<String> seen = new HashSet<>();
        for (Attribute field : targetFields) {
            if (seen.add(field.name()) == false) {
                failures.add(fail(field, "[FILLNULL] duplicate field [{}]", field.name()));
            }
        }
    }

    @Override
    public LogicalPlan surrogate() {
        computeSurrogateInfo();
        if (lazyAliases.isEmpty()) {
            return child();
        }
        Eval eval = new Eval(source(), child(), lazyAliases);
        return new Project(source(), eval, lazyOutput);
    }

    private void computeSurrogateInfo() {
        if (lazyAliases != null) {
            return;
        }
        List<Attribute> fieldsToFill = targetFields.isEmpty() ? child().output() : targetFields;
        Set<String> fillNames = new HashSet<>(fieldsToFill.size());
        for (Attribute a : fieldsToFill) {
            fillNames.add(a.name());
        }

        lazyAliases = new ArrayList<>(fieldsToFill.size());
        List<Attribute> output = new ArrayList<>(child().output().size());

        for (Attribute field : child().output()) {
            if (fillNames.contains(field.name())) {
                Expression defaultValue = resolveDefaultValue(field.dataType());
                if (defaultValue != null) {
                    Coalesce coalesce = new Coalesce(field.source(), field, List.of(defaultValue));
                    Alias alias = new Alias(field.source(), field.name(), coalesce);
                    lazyAliases.add(alias);
                    output.add(alias.toAttribute());
                    continue;
                }
            }
            output.add(field);
        }

        lazyOutput = output;
    }

    @Nullable
    private Expression resolveDefaultValue(DataType type) {
        // NULL-typed columns (e.g. unmapped fields surfaced under SET unmapped_fields="nullify",
        // or bare `null` literals in ROW) cannot be promoted to another type by FILLNULL: every
        // value is already null and the column type stays NULL. The verifier accepts a fill
        // literal here because areCompatible(KEYWORD, NULL) is true via the NULL escape clause,
        // but wrapping the column in Coalesce(col, fillLiteral) would either be a no-op (when
        // the fill is converted down to NULL) or change the column's declared type. Skipping
        // matches the existing `defaultForType(NULL)` behavior and keeps the column unchanged.
        if (DataType.isNull(type)) {
            return null;
        }
        if (fillValue != null) {
            // A null fill value (FILLNULL WITH null, or a parameter bound to null) would expand to
            // Coalesce(col, null) - a no-op that needlessly rewrites the column into a reference attribute.
            // Leave the column untouched regardless of the literal's declared type.
            if (fillValue instanceof Literal fillLiteral && fillLiteral.value() == null) {
                return null;
            }
            DataType fillType = fillValue.dataType();
            if (fillType == type) {
                return fillValue;
            }
            // Coalesce requires all branches to share an exact dataType. When the user-supplied
            // fill value is type-compatible with the target column but not exactly the same type
            // (e.g. INTEGER fill into a LONG column), convert the literal value once at plan time.
            if (DataType.areCompatible(fillType, type) && fillValue instanceof Literal lit) {
                Object converted = DataTypeConverter.convert(lit.value(), type);
                return new Literal(lit.source(), converted, type);
            }
            return null;
        }
        return defaultForType(type);
    }

    @Nullable
    static Expression defaultForType(DataType type) {
        if (type == DataType.INTEGER) {
            return new Literal(Source.EMPTY, 0, DataType.INTEGER);
        }
        if (type == DataType.LONG) {
            return new Literal(Source.EMPTY, 0L, DataType.LONG);
        }
        if (type == DataType.DOUBLE) {
            return new Literal(Source.EMPTY, 0.0, DataType.DOUBLE);
        }
        if (type == DataType.KEYWORD || type == DataType.TEXT) {
            return new Literal(Source.EMPTY, new BytesRef(""), DataType.KEYWORD);
        }
        if (type == DataType.BOOLEAN) {
            return new Literal(Source.EMPTY, false, DataType.BOOLEAN);
        }
        return null;
    }
}

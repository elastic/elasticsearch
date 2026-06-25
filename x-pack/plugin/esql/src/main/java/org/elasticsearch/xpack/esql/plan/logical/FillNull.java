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
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
 * <p>
 * The fill aliases are modeled exactly like {@link Eval#fields()}: they are materialized once
 * during analysis (see {@code Analyzer.ResolveRefs#resolveFillNull}) and stored as proper
 * {@link NodeInfo} state.
 */
public class FillNull extends UnaryPlan implements SurrogateLogicalPlan, PostAnalysisVerificationAware, TelemetryAware {

    private final @Nullable Expression fillValue;
    private final List<Attribute> targetFields;
    /**
     * The {@code col = COALESCE(col, default)} aliases produced by this command, or {@code null} until
     * they are materialized during analysis. Empty means there is nothing to fill (the command is a no-op).
     */
    private final @Nullable List<Alias> fields;

    private List<Attribute> lazyOutput;

    public FillNull(Source source, LogicalPlan child, @Nullable Expression fillValue, List<Attribute> targetFields) {
        this(source, child, fillValue, targetFields, null);
    }

    public FillNull(
        Source source,
        LogicalPlan child,
        @Nullable Expression fillValue,
        List<Attribute> targetFields,
        @Nullable List<Alias> fields
    ) {
        super(source, child);
        this.fillValue = fillValue;
        this.targetFields = targetFields;
        this.fields = fields;
    }

    @Nullable
    public Expression fillValue() {
        return fillValue;
    }

    public List<Attribute> targetFields() {
        return targetFields;
    }

    @Nullable
    public List<Alias> fields() {
        return fields;
    }

    @Override
    public List<Attribute> output() {
        if (fields == null) {
            // Not yet materialized (only happens transiently during analysis): the schema is unchanged.
            return child().output();
        }
        if (lazyOutput == null) {
            // Replace each filled column with its alias attribute in place, preserving the original column order.
            // (mergeOutputAttributes would move shadowed columns to the end, which FILLNULL must not do.)
            Map<String, Attribute> filled = new HashMap<>(fields.size());
            for (Alias field : fields) {
                filled.put(field.name(), field.toAttribute());
            }
            List<Attribute> childOutput = child().output();
            List<Attribute> output = new ArrayList<>(childOutput.size());
            for (Attribute attr : childOutput) {
                Attribute replacement = filled.get(attr.name());
                output.add(replacement != null ? replacement : attr);
            }
            lazyOutput = output;
        }
        return lazyOutput;
    }

    @Override
    protected AttributeSet computeReferences() {
        return fields == null ? AttributeSet.EMPTY : Eval.computeReferences(fields);
    }

    /**
     * Whether the command inputs (the fill value and target fields) are resolved. Distinct from
     * {@link #expressionsResolved()}, which additionally requires the fill aliases to be materialized.
     */
    public boolean inputsResolved() {
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
    public boolean expressionsResolved() {
        // Keep the node "unresolved" until the fill aliases are materialized, so that ResolveRefs (which skips
        // already-resolved nodes) is guaranteed to run resolveFillNull and build them - including for the
        // all-fields form `... | FILLNULL`, which has no unresolved target attributes to begin with.
        return inputsResolved() && fields != null;
    }

    @Override
    public FillNull replaceChild(LogicalPlan newChild) {
        return new FillNull(source(), newChild, fillValue, targetFields, fields);
    }

    public FillNull withTargetFields(List<Attribute> newTargetFields) {
        return new FillNull(source(), child(), fillValue, newTargetFields, fields);
    }

    /**
     * Builds the fill aliases against the given (resolved) child output and returns a copy carrying them.
     * Invoked once during analysis. The aliases reference the same child attributes that {@link #output()}
     * and {@link #surrogate()} build on, so later attribute rewrites stay consistent.
     */
    public FillNull materialize(List<Attribute> childOutput) {
        List<Attribute> fieldsToFill = targetFields.isEmpty() ? childOutput : targetFields;
        Set<String> fillNames = new HashSet<>(fieldsToFill.size());
        for (Attribute a : fieldsToFill) {
            fillNames.add(a.name());
        }

        List<Alias> built = new ArrayList<>(fieldsToFill.size());
        for (Attribute field : childOutput) {
            if (fillNames.contains(field.name())) {
                Expression defaultValue = resolveDefaultValue(field.dataType());
                if (defaultValue != null) {
                    Coalesce coalesce = new Coalesce(field.source(), field, List.of(defaultValue));
                    built.add(new Alias(field.source(), field.name(), coalesce));
                }
            }
        }
        return new FillNull(source(), child(), fillValue, targetFields, built);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, FillNull::new, child(), fillValue, targetFields, fields);
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
        return Objects.hash(super.hashCode(), fillValue, targetFields, fields);
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
        return super.equals(obj)
            && Objects.equals(fillValue, other.fillValue)
            && Objects.equals(targetFields, other.targetFields)
            && Objects.equals(fields, other.fields);
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
        if (fields == null || fields.isEmpty()) {
            // Nothing to fill (no fillable columns, or fields not materialized): drop the command.
            return child();
        }
        Eval eval = new Eval(source(), child(), fields);
        return new Project(source(), eval, output());
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
            // The fill value is type-compatible with the column but not the same type
            // (e.g. INTEGER fill into a LONG column), so convert the literal once at plan time.
            // Coalesce compares branch types via noText() and string literals are always KEYWORD,
            // so a TEXT column takes a KEYWORD literal (matching defaultForType).
            if (DataType.areCompatible(fillType, type) && fillValue instanceof Literal lit) {
                DataType literalType = type.noText();
                Object converted = DataTypeConverter.convert(lit.value(), literalType);
                return new Literal(lit.source(), converted, literalType);
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

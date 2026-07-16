/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

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
 * Replaces nulls in the given fields (or all fields) with a fill value or type-appropriate defaults, expanding into
 * a {@link Project} over an {@link Eval} of {@link Coalesce} aliases that preserves column order. The aliases are
 * materialized during analysis like {@link Eval#fields()}; see #148232.
 * <p>
 * A string {@code WITH} value is implicitly cast to the types the language casts string literals to (datetime,
 * date_nanos, ip, version, boolean), mirroring {@code Analyzer.ImplicitCasting}. A column whose type has no default
 * fill value and that receives no {@code WITH} value is left unchanged; {@code WarnUnfillableFillNull} then surfaces a
 * response-header warning for it.
 */
public class FillNull extends UnaryPlan implements SurrogateLogicalPlan, PostAnalysisVerificationAware, TelemetryAware {

    private final @Nullable Expression fillValue;
    private final List<Attribute> targetFields;
    /**
     * The {@code col = COALESCE(col, default)} aliases, or {@code null} until materialized during analysis;
     * empty means nothing to fill (no-op).
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
            // Replace each filled column in place; mergeOutputAttributes would move shadowed columns to the end.
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
        if (fields != null) {
            return Eval.computeReferences(fields);
        }
        // Before materialization (e.g. pre-analysis field-name collection) the inputs are the target fields plus the
        // fill value; they must be reported so field-caps requests them. The all-fields form uses the fallback instead.
        AttributeSet refs = Expressions.references(targetFields);
        return fillValue == null ? refs : refs.combine(fillValue.references());
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
        // Stay unresolved until the aliases are materialized so ResolveRefs (which skips resolved nodes) runs
        // resolveFillNull - including the all-fields form `... | FILLNULL`, which has no unresolved targets.
        if (inputsResolved() == false || fields == null) {
            return false;
        }
        // All-fields form: unmapped_fields="load" injects columns after the first pass, so stay unresolved while a
        // fillable column still lacks an alias and let ResolveRefs re-materialize (targeted form gates on inputsResolved()).
        if (targetFields.isEmpty() && childrenResolved() && allFillableColumnsCovered() == false) {
            return false;
        }
        return Resolvables.resolved(fields);
    }

    private boolean allFillableColumnsCovered() {
        Set<String> filled = new HashSet<>(fields.size());
        for (Alias a : fields) {
            filled.add(a.name());
        }
        for (Attribute attr : child().output()) {
            if (filled.contains(attr.name()) == false && resolveDefaultValue(attr.dataType(), null) != null) {
                return false;
            }
        }
        return true;
    }

    @Override
    public FillNull replaceChild(LogicalPlan newChild) {
        return new FillNull(source(), newChild, fillValue, targetFields, fields);
    }

    public FillNull withTargetFields(List<Attribute> newTargetFields) {
        return new FillNull(source(), child(), fillValue, newTargetFields, fields);
    }

    /**
     * Builds the fill aliases against the resolved child output and returns a copy carrying them.
     */
    public FillNull materialize(List<Attribute> childOutput, Configuration configuration) {
        List<Attribute> fieldsToFill = targetFields.isEmpty() ? childOutput : targetFields;
        Set<String> fillNames = new HashSet<>(fieldsToFill.size());
        for (Attribute a : fieldsToFill) {
            fillNames.add(a.name());
        }

        Map<String, Alias> existing;
        if (fields == null || fields.isEmpty()) {
            existing = Map.of();
        } else {
            existing = new HashMap<>(fields.size());
            for (Alias a : fields) {
                existing.put(a.name(), a);
            }
        }

        List<Alias> built = new ArrayList<>(fieldsToFill.size());
        for (Attribute field : childOutput) {
            if (fillNames.contains(field.name())) {
                Alias previous = existing.get(field.name());
                // Reuse the existing alias (keeping its id) only while valid: resolved and same type
                if (previous != null && previous.resolved() && previous.dataType() == field.dataType().noText()) {
                    built.add(previous);
                    continue;
                }
                Expression defaultValue = resolveDefaultValue(field.dataType(), configuration);
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
                if (field.resolved() == false) {
                    continue;
                }
                DataType fieldType = field.dataType();
                boolean stringImplicitCast = fillValue.dataType() == DataType.KEYWORD
                    && fillValue instanceof Literal
                    && EsqlDataTypeConverter.isStringImplicitlyCastableTo(fieldType.noText());
                if (stringImplicitCast == false && DataType.areCompatible(fillValue.dataType(), fieldType) == false) {
                    failures.add(
                        fail(
                            field,
                            "[FILLNULL] fill value type [{}] is incompatible with field [{}] type [{}]",
                            fillValue.dataType().typeName(),
                            field.name(),
                            fieldType.typeName()
                        )
                    );
                    continue;
                }

                if (fillValue instanceof Literal lit
                    && lit.value() != null
                    && fillValue.dataType() != fieldType
                    && DataType.isNull(fieldType) == false
                    && resolveDefaultValue(fieldType, null) == null) {
                    failures.add(
                        fail(
                            field,
                            "[FILLNULL] fill value [{}] does not fit field [{}] of type [{}]",
                            BytesRefs.toString(lit.value()),
                            field.name(),
                            fieldType.typeName()
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
            return child();
        }
        Eval eval = new Eval(source(), child(), fields);
        return new Project(source(), eval, output());
    }

    @Nullable
    private Expression resolveDefaultValue(DataType type, @Nullable Configuration configuration) {
        if (DataType.isNull(type)) {
            return null;
        }
        if (fillValue != null) {
            if (fillValue instanceof Literal fillLiteral && fillLiteral.value() == null) {
                return null;
            }
            DataType fillType = fillValue.dataType();
            if (fillType == type) {
                return fillValue;
            }
            // Type-compatible but different type (e.g. INTEGER fill into a LONG column): convert the literal once
            if (DataType.areCompatible(fillType, type) && fillValue instanceof Literal lit) {
                DataType literalType = type.noText();
                Object converted;
                try {
                    converted = DataTypeConverter.convert(lit.value(), literalType);
                } catch (InvalidArgumentException e) {
                    // Value does not fit the column type (e.g. LONG literal outside INTEGER range). All-fields targets are
                    // silently skipped here; explicitly targeted fields are already rejected by postAnalysisVerification.
                    return null;
                }
                return new Literal(lit.source(), converted, literalType);
            }

            if (fillType == DataType.KEYWORD
                && fillValue instanceof Literal lit
                && EsqlDataTypeConverter.isStringImplicitlyCastableTo(type.noText())) {
                DataType literalType = type.noText();
                Object converted;
                try {
                    converted = castStringLiteral(lit.value(), literalType, configuration);
                } catch (Exception e) {
                    // Unparsable value for the target type (e.g. "not-a-date" into datetime). All-fields targets are
                    // silently skipped here; explicitly targeted fields are rejected by postAnalysisVerification.
                    return null;
                }
                return new Literal(lit.source(), converted, literalType);
            }
            return null;
        }
        return defaultForType(type);
    }

    private static Object castStringLiteral(Object value, DataType type, @Nullable Configuration configuration) {
        if (configuration != null) {
            return EsqlDataTypeConverter.convert(value, type, configuration);
        }
        if (type == DataType.DATETIME) {
            return EsqlDataTypeConverter.dateTimeToLong(BytesRefs.toString(value));
        }
        if (type == DataType.DATE_NANOS) {
            return EsqlDataTypeConverter.dateNanosToLong(BytesRefs.toString(value));
        }
        return EsqlDataTypeConverter.convert(value, type, null);
    }

    /**
     * The explicitly-targeted fields (or, in the all-fields form, the child columns) that will not be filled because
     * their type has no default fill value and no WITH value was provided. Only meaningful once {@link #fields} has been
     * materialized.
     */
    public List<Attribute> unfillableTargets() {
        Set<String> filled = new HashSet<>();
        if (fields != null) {
            for (Alias a : fields) {
                filled.add(a.name());
            }
        }
        List<Attribute> consider = targetFields.isEmpty() ? child().output() : targetFields;
        List<Attribute> result = new ArrayList<>();
        for (Attribute attr : consider) {
            if (attr.resolved() && filled.contains(attr.name()) == false) {
                result.add(attr);
            }
        }
        return result;
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

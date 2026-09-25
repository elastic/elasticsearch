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
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToText;
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

/**
 * Replaces nulls in the targeted columns with a fill value or type-appropriate defaults, expanding into a
 * {@link Project} over an {@link Eval} of {@link Coalesce} aliases that preserves column order. The aliases are derived
 * during analysis like {@link Eval#fields()}.
 * <p>
 * Syntax is {@code FILLNULL <value> ON <fields>}. Each target resolves like {@code KEEP}, independently of the rest of
 * the list, so an unknown name or a pattern matching nothing is an error even alongside a {@code *}. A bare {@code *}
 * sweeps up every user column but no internals; naming a metadata or synthetic column explicitly still targets it.
 * <p>
 * <b>Filling never fails.</b> A column the value cannot be applied to is left exactly as it was - null or not - and
 * reported by {@code WarnUnfillableFillNull}. How the column was selected does not change the outcome or the message.
 * This covers an incompatible type, an out-of-range value, a string that will not parse into a date / ip / version, a
 * {@code null}-typed column, a type with no default under {@code DEFAULT}, and a multi-valued value. {@code DEFAULT}
 * uses a type-appropriate default and is represented as a {@code null} fill value; an explicit {@code NULL} means "do
 * not fill" and is the one form that reports nothing.
 * <p>
 * A column's type is never changed, only its null positions - which is why {@link #fillExpression} wraps a {@code text}
 * column's {@link Coalesce} in {@code TO_TEXT}: {@code Coalesce} normalizes its own type to {@code keyword}.
 */
public class FillNull extends UnaryPlan implements SurrogateLogicalPlan, TelemetryAware {

    private final @Nullable Expression fillValue;

    // The parsed targets - UnresolvedAttributes UnresolvedNamePattern wildcards.
    private final List<NamedExpression> targetFields;

    // Whether {@code *} appeared in the target list, selecting every user column.
    private final boolean allColumns;

    // The col = COALESCE(col, default) aliases, or null until derived during analysis; empty means nothing to fill (no-op).
    private final @Nullable List<Alias> fields;

    private List<Attribute> lazyOutput;

    // fields cannot be derived without the Configuration: converterFor reads the time zone from it for datetime/date_nanos.
    private final @Nullable Configuration configuration;

    public FillNull(Source source, LogicalPlan child, @Nullable Expression fillValue, List<NamedExpression> targetFields) {
        this(source, child, fillValue, targetFields, false);
    }

    public FillNull(
        Source source,
        LogicalPlan child,
        @Nullable Expression fillValue,
        List<NamedExpression> targetFields,
        boolean allColumns
    ) {
        this(source, child, fillValue, targetFields, allColumns, null);
    }

    public FillNull(
        Source source,
        LogicalPlan child,
        @Nullable Expression fillValue,
        List<NamedExpression> targetFields,
        boolean allColumns,
        @Nullable List<Alias> fields
    ) {
        this(source, child, fillValue, targetFields, allColumns, fields, null);
    }

    private FillNull(
        Source source,
        LogicalPlan child,
        @Nullable Expression fillValue,
        List<NamedExpression> targetFields,
        boolean allColumns,
        @Nullable List<Alias> fields,
        @Nullable Configuration configuration
    ) {
        super(source, child);
        this.fillValue = fillValue;
        this.targetFields = targetFields;
        this.allColumns = allColumns;
        this.fields = fields;
        this.configuration = configuration;
    }

    // rebuilds the node with fields re-derived from the child output
    private static FillNull rebuildFillNullWithFields(
        Source source,
        LogicalPlan child,
        @Nullable Expression fillValue,
        List<NamedExpression> targetFields,
        boolean allColumns,
        @Nullable Configuration configuration,
        @Nullable List<Alias> previousFields
    ) {
        // Before the configuration arrives, or while the child is still unresolved, there is nothing to derive from;
        // expressionsResolved() reports unresolved until then, so ResolveRefs comes back.
        List<Alias> derived = configuration == null || child.resolved() == false
            ? previousFields
            : buildFields(child.output(), fillValue, targetFields, allColumns, configuration, previousFields);
        return new FillNull(source, child, fillValue, targetFields, allColumns, derived, configuration);
    }

    public FillNull withConfiguration(Configuration newConfiguration) {
        return rebuildFillNullWithFields(source(), child(), fillValue, targetFields, allColumns, newConfiguration, fields);
    }

    // Whether {@code *} was among the targets, i.e. every column is filled and skipping is lenient
    public boolean allColumns() {
        return allColumns;
    }

    @Nullable
    public Expression fillValue() {
        return fillValue;
    }

    public List<NamedExpression> targetFields() {
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
        for (NamedExpression field : targetFields) {
            if (field.resolved() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean expressionsResolved() {
        // Stay unresolved until the aliases are derived so ResolveRefs (which skips resolved nodes) runs resolveFillNull
        // to supply the Configuration - including the all-fields form `... | FILLNULL <value> ON *`, which has no unresolved targets
        if (inputsResolved() == false || fields == null) {
            return false;
        }
        return Resolvables.resolved(fields);
    }

    @Override
    public FillNull replaceChild(LogicalPlan newChild) {
        return rebuildFillNullWithFields(source(), newChild, fillValue, targetFields, allColumns, configuration, fields);
    }

    public FillNull withTargetFields(List<NamedExpression> newTargetFields) {
        return rebuildFillNullWithFields(source(), child(), fillValue, newTargetFields, allColumns, configuration, fields);
    }

    private static List<Alias> buildFields(
        List<Attribute> childOutput,
        @Nullable Expression fillValue,
        List<NamedExpression> targetFields,
        boolean allColumns,
        Configuration configuration,
        @Nullable List<Alias> previousFields
    ) {
        // A null fillNames set marks the all-columns form (the ON * form); otherwise only the resolved target names are filled.
        Set<String> namedTargets = targetNames(targetFields);
        final Set<String> fillNames = (allColumns || targetFields.isEmpty()) ? null : namedTargets;

        Map<String, Alias> existing;
        if (previousFields == null || previousFields.isEmpty()) {
            existing = Map.of();
        } else {
            existing = new HashMap<>(previousFields.size());
            for (Alias a : previousFields) {
                existing.put(a.name(), a);
            }
        }

        List<Alias> built = new ArrayList<>(childOutput.size());
        for (Attribute field : childOutput) {
            // `*` sweeps up every user column, not the internals: metadata (_index, _score) and synthetic attributes such
            // as the `$$<field>$converted_to$<type>` union-type columns. Naming one explicitly still fills it, even
            // alongside a `*` - what a column does must not depend on whether a `*` happens to be in the same list.
            if (isInternal(field) && namedTargets.contains(field.name()) == false) {
                continue;
            }
            if (fillNames == null || fillNames.contains(field.name())) {
                Alias previous = existing.get(field.name());
                // Reuse the existing alias (keeping its id) only while valid: resolved, same type, and still wrapping the same attribute
                if (previous != null
                    && previous.resolved()
                    && previous.dataType() == field.dataType()
                    && unwrapCoalesce(previous.child()) instanceof Coalesce c
                    && c.children().get(0).equals(field)) {
                    built.add(previous);
                    continue;
                }
                Expression defaultValue = resolveDefaultValue(field.dataType(), fillValue, configuration);
                if (defaultValue != null) {
                    built.add(new Alias(field.source(), field.name(), fillExpression(field, defaultValue)));
                }
            }
        }
        return built;
    }

    /**
     * The fill expression for one column: {@code COALESCE(col, default)}, wrapped in {@code TO_TEXT} for a {@code text}
     * column. {@link Coalesce#resolveType()} normalizes its type with {@code noText()}, so without the wrapper filling a
     * {@code text} column would silently re-type it to {@code keyword} - and {@code FILLNULL} must not change a column's
     * type, only its null positions.
     */
    private static Expression fillExpression(Attribute field, Expression defaultValue) {
        Coalesce coalesce = new Coalesce(field.source(), field, List.of(defaultValue));
        return field.dataType() == DataType.TEXT ? new ToText(field.source(), coalesce) : coalesce;
    }

    /** The {@link Coalesce} inside a fill expression, looking through the {@code text} wrapper added by {@link #fillExpression}. */
    private static Expression unwrapCoalesce(Expression fillExpression) {
        return fillExpression instanceof ToText toText ? toText.field() : fillExpression;
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        // configuration is captured by the closure rather than being a property: it is an input, not something plan
        // transformations rewrite, and it must survive a property rebuild or replaceChild could not re-derive fields.
        return NodeInfo.create(
            this,
            (source, child, fillValue, targetFields, allColumns, fields) -> new FillNull(
                source,
                child,
                fillValue,
                targetFields,
                allColumns,
                fields,
                configuration
            ),
            child(),
            fillValue,
            targetFields,
            allColumns,
            fields
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
        return Objects.hash(super.hashCode(), fillValue, targetFields, allColumns, fields, configuration);
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
            && allColumns == other.allColumns
            && Objects.equals(fields, other.fields)
            && Objects.equals(configuration, other.configuration);
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
    private static Expression resolveDefaultValue(DataType type, @Nullable Expression fillValue, Configuration configuration) {
        if (DataType.isNull(type)) {
            return null;
        }
        if (fillValue != null) {
            if (fillValue instanceof Literal fillLiteral && fillLiteral.value() == null) {
                return null;
            }
            if (multiValuedFill(fillValue) != null) {
                // WarnUnfillableFillNull reports this; bail out here so the converters, which cast the literal value to
                // Number/String, are never handed a List.
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
                    // RATIONAL_TO_INT/LONG round via Math.round instead of failing, so 2.7 into an integer column would
                    // silently fill 3. Require the conversion to be exactly reversible; COALESCE rejects the pair outright.
                    if (Objects.equals(lit.value(), DataTypeConverter.convert(converted, fillType)) == false) {
                        return null;
                    }
                } catch (InvalidArgumentException e) {
                    // Value does not fit the column type, or does not survive the round trip (e.g. a long that is not
                    // exactly representable as a double). The column is left unchanged either way and reported by
                    // WarnUnfillableFillNull; FILLNULL never fails over a value it cannot apply.
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
                    converted = EsqlDataTypeConverter.convert(lit.value(), literalType, configuration);
                } catch (IllegalArgumentException | InvalidArgumentException e) {
                    // Unparsable or out-of-range for the target type ("not-a-date" into datetime; a pre-1970 instant into
                    // date_nanos, which DateUtils.toLong rejects with IllegalArgumentException)
                    return null;
                }
                return new Literal(lit.source(), converted, literalType);
            }
            return null;
        }
        return defaultForType(type);
    }

    /** The names written in the target list. Empty for a bare {@code ON *}, which names nothing. */
    private static Set<String> targetNames(List<NamedExpression> targetFields) {
        Set<String> names = new HashSet<>(targetFields.size());
        for (NamedExpression ne : targetFields) {
            names.add(ne.name());
        }
        return names;
    }

    /** Columns {@code *} does not sweep up: metadata ({@code _index}, {@code _score}) and synthetic attributes. */
    private static boolean isInternal(Attribute attr) {
        return attr instanceof MetadataAttribute || attr.synthetic();
    }

    @Nullable
    private static List<?> multiValuedFill(@Nullable Expression fillValue) {
        return fillValue instanceof Literal lit && lit.value() instanceof List<?> values ? values : null;
    }

    /**
     * The values of a multi-valued fill value, or {@code null} if it is not one. Only reachable through a list-valued
     * {@code ?param}; nothing can be filled with it, so {@code WarnUnfillableFillNull} reports it.
     */
    @Nullable
    public List<?> multiValuedFill() {
        return multiValuedFill(fillValue);
    }

    /**
     * Whether the value is an explicit {@code NULL}, i.e. "do not fill". Nothing is unexpectedly left unfilled in that
     * case, so it is the one form that warns about nothing.
     */
    public boolean isExplicitNullFill() {
        return fillValue instanceof Literal lit && lit.value() == null;
    }

    /**
     * The targeted columns that were left unchanged - whatever the reason: the type has no default under
     * {@code DEFAULT}, the value's type is incompatible, the value is out of range for the column, a string could not be
     * parsed into it, or the column is {@code null}-typed. {@code FILLNULL} never fails over any of these; they are all
     * reported by {@code WarnUnfillableFillNull}, so this list is what it warns about. Only meaningful once
     * {@link #fields} has been derived.
     */
    public List<Attribute> unfillableTargets() {
        Set<String> filled = new HashSet<>();
        if (fields != null) {
            for (Alias a : fields) {
                filled.add(a.name());
            }
        }
        Set<String> namedTargets = targetNames(targetFields);
        List<Attribute> result = new ArrayList<>();
        if (allColumns || targetFields.isEmpty()) {
            for (Attribute attr : child().output()) {
                // `*` never sweeps up the internals, so they were never in scope and are not "left unchanged". Naming
                // one explicitly does put it in scope, so it is reported like any other target. Mirrors buildFields.
                if (isInternal(attr) && namedTargets.contains(attr.name()) == false) {
                    continue;
                }
                if (attr.resolved() && filled.contains(attr.name()) == false) {
                    result.add(attr);
                }
            }
        } else {
            for (NamedExpression ne : targetFields) {
                // Every resolved target is an Attribute (resolveAgainstList only ever yields those); assert rather than
                // skip, so a future change that breaks that fails loudly instead of dropping a column from the warning.
                if (ne.resolved() == false) {
                    continue;
                }
                assert ne instanceof Attribute : "resolved FILLNULL target is not an Attribute: " + ne;
                if (ne instanceof Attribute attr && filled.contains(attr.name()) == false) {
                    result.add(attr);
                }
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
        if (type == DataType.UNSIGNED_LONG) {
            // Held as the unsigned-long encoding of 0, matching how UL literals are represented elsewhere.
            return new Literal(Source.EMPTY, NumericUtils.ZERO_AS_UNSIGNED_LONG, DataType.UNSIGNED_LONG);
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

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
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedPattern;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToText;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.WarnUnfillableFillNull;
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
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Replaces nulls in the targeted columns with a fill value or type-appropriate defaults, expanding into a
 * {@link Project} over an {@link Eval} of {@link Coalesce} aliases that preserves column order. Until then the output keeps
 * its child's ids, a filled column as a reference: the aliases and their new ids only come into being in {@code SubstituteFillNull}.
 * <p>
 * Syntax is {@code FILLNULL <value> ON <fields>}. Each target resolves like {@code KEEP}, independently of the rest of
 * the list, so an unknown name or a pattern matching nothing is an error even alongside a {@code *}. A bare {@code *}
 * sweeps up every user column but no internals; naming a metadata or synthetic column explicitly still targets it.
 * <p>
 * <b>Filling never fails.</b> A column the value cannot be applied to is left exactly as it was - null or not - and
 * reported by {@link WarnUnfillableFillNull}. How the column was selected does not change the outcome or the message.
 * This covers an incompatible type, an out-of-range value, a string that will not parse into a boolean / date / ip / version, a
 * {@code null}-typed column, a type with no default under {@code DEFAULT}, and a multi-valued value. {@code DEFAULT}
 * uses a type-appropriate default and is represented as a {@code null} fill value; an explicit {@code NULL} means "do
 * not fill" and is the one form that reports nothing.
 * <p>
 * A column's type is never changed, only its null positions - which is why {@link #fillExpression} wraps a {@code text}
 * column's {@link Coalesce} in {@code TO_TEXT}: {@code Coalesce} normalizes its own type to {@code keyword}.
 */
public class FillNull extends UnaryPlan implements TelemetryAware, PostAnalysisVerificationAware {

    private final @Nullable Expression fillValue;

    // The parsed targets - UnresolvedAttributes UnresolvedNamePattern wildcards.
    private final List<NamedExpression> targetFields;

    // Whether {@code *} appeared in the target list, selecting every user column.
    private final boolean allColumns;

    // Patterns that match no column yet. They must not keep the node unresolved: a command above may name the field the pattern is
    // meant to match, and unmapped_fields only loads it once that command resolves. Reported by postAnalysisVerification.
    private final List<UnresolvedPattern> unmatchedPatterns;

    private final @Nullable Function<List<Attribute>, List<NamedExpression>> targetResolver;

    // The aliases cannot be derived without the Configuration: converterFor reads the time zone from it for datetime/date_nanos.
    private final @Nullable Configuration configuration;

    private List<Attribute> lazyOutput;

    private Set<String> lazyFilledNames;

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
        this(source, child, fillValue, targetFields, allColumns, List.of(), null, null);
    }

    private FillNull(
        Source source,
        LogicalPlan child,
        @Nullable Expression fillValue,
        List<NamedExpression> targetFields,
        boolean allColumns,
        List<UnresolvedPattern> unmatchedPatterns,
        @Nullable Function<List<Attribute>, List<NamedExpression>> targetResolver,
        @Nullable Configuration configuration
    ) {
        super(source, child);
        this.fillValue = fillValue;
        this.targetFields = targetFields;
        this.allColumns = allColumns;
        this.unmatchedPatterns = unmatchedPatterns;
        this.targetResolver = targetResolver;
        this.configuration = configuration;
    }

    public FillNull withConfiguration(Configuration newConfiguration) {
        return new FillNull(source(), child(), fillValue, targetFields, allColumns, unmatchedPatterns, targetResolver, newConfiguration);
    }

    public FillNull withTargetResolver(Function<List<Attribute>, List<NamedExpression>> resolver) {
        return resolvedOver(child(), resolver);
    }

    private FillNull resolvedOver(LogicalPlan newChild, @Nullable Function<List<Attribute>, List<NamedExpression>> resolver) {
        if (resolver == null || newChild.resolved() == false) {
            return new FillNull(source(), newChild, fillValue, targetFields, allColumns, unmatchedPatterns, resolver, configuration);
        }
        // Like ResolvingProject, keep the automatic LOAD_ALL column out of the resolver's scope: a pattern must not match it.
        List<Attribute> input = new ArrayList<>(newChild.output().size());
        for (Attribute attr : newChild.output()) {
            if (attr instanceof UnmappedFieldsAttribute == false) {
                input.add(attr);
            }
        }
        List<NamedExpression> resolved = resolver.apply(input);
        List<NamedExpression> targets = new ArrayList<>(resolved.size());
        List<UnresolvedPattern> unmatched = new ArrayList<>();
        for (NamedExpression target : resolved) {
            if (target instanceof UnresolvedPattern pattern) {
                unmatched.add(pattern);
            } else {
                targets.add(target);
            }
        }
        return new FillNull(source(), newChild, fillValue, targets, allColumns, unmatched, resolver, configuration);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        for (UnresolvedPattern pattern : unmatchedPatterns) {
            failures.add(fail(pattern, pattern.unresolvedMessage()));
        }
    }

    @Nullable
    public Function<List<Attribute>, List<NamedExpression>> targetResolver() {
        return targetResolver;
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

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            Set<String> filled = filledNames();
            List<Attribute> childOutput = child().output();
            if (filled.isEmpty()) {
                lazyOutput = childOutput;
            } else {
                // A filled column is computed, so the analysis-time rules must not take it for the field below (e.g. ResolveUnionTypes
                // would re-load a field from the index under a CONVERT, skipping the fill); it keeps the field's id until the expansion.
                List<Attribute> output = new ArrayList<>(childOutput.size());
                for (Attribute attr : childOutput) {
                    output.add(
                        filled.contains(attr.name()) && attr instanceof ReferenceAttribute == false
                            ? new ReferenceAttribute(
                                attr.source(),
                                attr.qualifier(),
                                attr.name(),
                                attr.dataType(),
                                attr.nullable(),
                                attr.id(),
                                attr.synthetic()
                            )
                            : attr
                    );
                }
                lazyOutput = output;
            }
        }
        return lazyOutput;
    }

    @Override
    protected AttributeSet computeReferences() {
        // The inputs are the target fields plus the fill value; before analysis they must be reported so field-caps requests them. The
        // all-fields form uses the fallback instead.
        AttributeSet refs = Expressions.references(targetFields);
        return fillValue == null ? refs : refs.combine(fillValue.references());
    }

    /**
     * Whether the command inputs (the fill value and target fields) are resolved. Distinct from
     * {@link #expressionsResolved()}, which additionally requires the query {@link Configuration}.
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
        // Stay unresolved until the Configuration arrives so ResolveRefs (which skips resolved nodes) runs resolveFillNull - including
        // the all-fields form `... | FILLNULL <value> ON *`, which has no unresolved targets
        return inputsResolved() && configuration != null;
    }

    @Override
    public FillNull replaceChild(LogicalPlan newChild) {
        return resolvedOver(newChild, targetResolver);
    }

    public List<Alias> fillAliases() {
        if (configuration == null || inputsResolved() == false || child().resolved() == false) {
            return List.of();
        }
        Set<String> namedTargets = targetNames(targetFields);
        Set<String> fillNames = fillsAllColumns() ? null : namedTargets;
        List<Attribute> childOutput = child().output();
        List<Alias> aliases = new ArrayList<>(childOutput.size());
        for (Attribute field : childOutput) {
            // `*` sweeps up every user column, not the internals: metadata (_index, _score) and synthetic attributes such
            // as the `$$<field>$converted_to$<type>` union-type columns. Naming one explicitly still fills it, even
            // alongside a `*` - what a column does must not depend on whether a `*` happens to be in the same list.
            if (field.isInternal() && namedTargets.contains(field.name()) == false) {
                continue;
            }
            if (fillNames == null || fillNames.contains(field.name())) {
                Expression defaultValue = resolveDefaultValue(field.dataType(), fillValue, configuration);
                if (defaultValue != null) {
                    aliases.add(new Alias(field.source(), field.name(), fillExpression(field, defaultValue)));
                }
            }
        }
        return aliases;
    }

    public boolean fills(Attribute attribute) {
        return child().outputSet().contains(attribute) && filledNames().contains(attribute.name());
    }

    private Set<String> filledNames() {
        if (lazyFilledNames == null) {
            Set<String> filled = new HashSet<>();
            for (Alias alias : fillAliases()) {
                filled.add(alias.name());
            }
            lazyFilledNames = filled;
        }
        return lazyFilledNames;
    }

    public static LogicalPlan expandAll(LogicalPlan plan) {
        return plan.anyMatch(FillNull.class::isInstance) ? expandBelow(plan, new HashMap<>()) : plan;
    }

    private static LogicalPlan expandBelow(LogicalPlan plan, Map<NameId, Attribute> replacedInOutput) {
        List<LogicalPlan> children = plan.children();
        List<LogicalPlan> newChildren = new ArrayList<>(children.size());
        Map<NameId, Attribute> replaced = new HashMap<>();
        boolean childrenChanged = false;
        for (LogicalPlan child : children) {
            LogicalPlan newChild = expandBelow(child, replaced);
            childrenChanged |= newChild != child;
            newChildren.add(newChild);
        }
        LogicalPlan result = childrenChanged ? plan.replaceChildren(newChildren) : plan;
        if (replaced.isEmpty() == false) {
            result = result.transformExpressionsOnly(Attribute.class, a -> replaced.getOrDefault(a.id(), a));
        }
        if (result instanceof FillNull fillNull) {
            result = fillNull.expand();
        }
        // The parent can only reference this node's output, so the ids that changed there are exactly what it has to follow.
        // That includes nodes whose output is not just their child's, like INLINE STATS, which also outputs its Aggregate's input.
        // A column that leaves the output, because it is dropped, redefined, or behind a FORK whose output has ids of its own,
        // stops here, which also keeps FORK branches apart.
        if (result != plan) {
            List<Attribute> before = plan.output();
            List<Attribute> after = result.output();
            assert before.size() == after.size() && Expressions.names(before).equals(Expressions.names(after))
                : "FILLNULL expansion changed the output from " + before + " to " + after;
            assert before.stream().map(Attribute::dataType).toList().equals(after.stream().map(Attribute::dataType).toList())
                : "FILLNULL expansion changed the output types from " + before + " to " + after;
            for (int i = 0; i < before.size(); i++) {
                if (before.get(i).id().equals(after.get(i).id()) == false) {
                    replacedInOutput.put(before.get(i).id(), after.get(i));
                }
            }
        }
        return result;
    }

    private LogicalPlan expand() {
        List<Alias> aliases = fillAliases();
        if (aliases.isEmpty()) {
            return child();
        }
        // Replace each filled column in place; mergeOutputAttributes would move shadowed columns to the end.
        Map<String, Attribute> filled = new HashMap<>(aliases.size());
        for (Alias alias : aliases) {
            filled.put(alias.name(), alias.toAttribute());
        }
        List<Attribute> childOutput = child().output();
        List<Attribute> output = new ArrayList<>(childOutput.size());
        for (Attribute attr : childOutput) {
            Attribute replacement = filled.get(attr.name());
            output.add(replacement != null ? replacement : attr);
        }
        return new Project(source(), new Eval(source(), child(), aliases), output);
    }

    private boolean fillsAllColumns() {
        return allColumns || (targetFields.isEmpty() && unmatchedPatterns.isEmpty());
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

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        // The resolver and the configuration are captured by the closure rather than being properties: they are inputs, not something
        // plan transformations rewrite, and they must survive a property rebuild or replaceChild could not re-resolve the targets.
        return NodeInfo.create(
            this,
            (source, child, fillValue, targetFields, allColumns) -> new FillNull(
                source,
                child,
                fillValue,
                targetFields,
                allColumns,
                unmatchedPatterns,
                targetResolver,
                configuration
            ),
            child(),
            fillValue,
            targetFields,
            allColumns
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
        return Objects.hash(super.hashCode(), fillValue, targetFields, allColumns, unmatchedPatterns, configuration);
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
            && Objects.equals(unmatchedPatterns, other.unmatchedPatterns)
            && Objects.equals(configuration, other.configuration);
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
                if (literalType == DataType.BOOLEAN && isBooleanString(lit.value()) == false) {
                    return null;
                }
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

    private static boolean isBooleanString(Object value) {
        String s = BytesRefs.toString(value);
        return "true".equalsIgnoreCase(s) || "false".equalsIgnoreCase(s);
    }

    /** The names written in the target list. Empty for a bare {@code ON *}, which names nothing. */
    private static Set<String> targetNames(List<NamedExpression> targetFields) {
        Set<String> names = new HashSet<>(targetFields.size());
        for (NamedExpression ne : targetFields) {
            names.add(ne.name());
        }
        return names;
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
     * reported by {@code WarnUnfillableFillNull}, so this list is what it warns about. Only meaningful once the node is
     * resolved.
     */
    public List<Attribute> unfillableTargets() {
        Set<String> filled = new HashSet<>();
        for (Alias a : fillAliases()) {
            filled.add(a.name());
        }
        Set<String> namedTargets = targetNames(targetFields);
        List<Attribute> result = new ArrayList<>();
        if (fillsAllColumns()) {
            for (Attribute attr : child().output()) {
                // `*` never sweeps up the internals, so they were never in scope and are not "left unchanged". Naming
                // one explicitly does put it in scope, so it is reported like any other target. Mirrors fillAliases.
                if (attr.isInternal() && namedTargets.contains(attr.name()) == false) {
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

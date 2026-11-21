/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ml;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.ml.ColumnLineage.Computed;
import org.elasticsearch.xpack.esql.ml.ColumnLineage.Scope;
import org.elasticsearch.xpack.esql.ml.ColumnLineage.SourceField;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucket;

import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Finds the fixed-span time bucket a STATS groups by in the parsed, unresolved plan, and the output columns carrying it. */
final class TimeBucketResolver {

    private TimeBucketResolver() {}

    /** A bucket over {@code field}, a source field, or over a computed value when {@code computedField}; both unset for TBUCKET. */
    private record BucketSource(@Nullable String field, boolean computedField, TimeValue span) {}

    /**
     * The one fixed-span time bucket among the groupings of {@code aggregate}, or {@code null} if there is none or more than one.
     *
     * @param above the plans between the query root and {@code aggregate}, root first
     */
    @Nullable
    static TimeBucket timeBucket(Aggregate aggregate, List<LogicalPlan> above) {
        BucketSource bucket = null;
        String column = null;
        for (Expression grouping : aggregate.groupings()) {
            BucketSource source = bucketSource(Alias.unwrap(grouping), Scope.below(aggregate.child()));
            if (source != null) {
                if (bucket != null) {
                    return null;
                }
                bucket = source;
                column = ((NamedExpression) grouping).name();
            }
        }
        return bucket == null ? null : new TimeBucket(bucket.field(), bucket.computedField(), bucket.span(), outputColumns(column, above));
    }

    /**
     * The bucket {@code value} computes when it is itself a time function, or an attribute defined below as one through EVAL,
     * RENAME or a lower STATS grouping. Joins, merges and INLINE STATS end the search.
     */
    @Nullable
    private static BucketSource bucketSource(Expression value, Scope scope) {
        if (value instanceof UnresolvedFunction function) {
            return bucketSource(function, scope);
        }
        if (value instanceof UnresolvedAttribute attribute
            && ColumnLineage.origin(attribute.name(), scope, false) instanceof Computed computed
            && computed.expression() instanceof UnresolvedFunction function) {
            return bucketSource(function, computed.scope());
        }
        return null;
    }

    /** The columns of the query output that carry the value of {@code column}, following it up from the STATS. */
    private static Set<String> outputColumns(String column, List<LogicalPlan> above) {
        Set<String> columns = new LinkedHashSet<>();
        columns.add(column);
        for (LogicalPlan node : above.reversed()) {
            switch (node) {
                case Rename rename -> {
                    for (Alias renaming : rename.renamings()) {
                        boolean carries = renaming.child() instanceof UnresolvedAttribute source && columns.remove(source.name());
                        columns.remove(renaming.name());
                        if (carries) {
                            columns.add(renaming.name());
                        }
                    }
                }
                case Eval eval -> {
                    for (Alias alias : eval.fields()) {
                        if (derivesFromOneColumn(alias.child(), columns)) {
                            columns.add(alias.name());
                        } else {
                            columns.remove(alias.name());
                        }
                    }
                }
                case Keep keep -> columns.removeIf(name -> ColumnLineage.matchesAny(keep.projections(), name) == false);
                case Drop drop -> columns.removeIf(name -> ColumnLineage.matchesAny(drop.removals(), name));
                case InlineStats inlineStats -> {
                    // Aliased aggregates overwrite columns of the same name; unaliased groupings keep theirs.
                    if (inlineStats.child() instanceof Aggregate aggregate) {
                        aggregate.aggregates().stream().filter(Alias.class::isInstance).map(NamedExpression::name).forEach(columns::remove);
                    }
                }
                case Aggregate aggregate -> {
                    Set<String> grouped = new LinkedHashSet<>();
                    for (Expression grouping : aggregate.groupings()) {
                        if (Alias.unwrap(grouping) instanceof UnresolvedAttribute attribute && columns.contains(attribute.name())) {
                            grouped.add(((NamedExpression) grouping).name());
                        }
                    }
                    columns.clear();
                    columns.addAll(grouped);
                }
                default -> {
                }
            }
        }
        return columns;
    }

    /** Whether {@code expression} references exactly one of {@code columns} and otherwise only literals. */
    private static boolean derivesFromOneColumn(Expression expression, Set<String> columns) {
        boolean otherLeaf = expression.anyMatch(
            e -> e.children().isEmpty() && e instanceof Literal == false && e instanceof UnresolvedAttribute == false
        );
        if (otherLeaf) {
            return false;
        }
        Set<String> references = new HashSet<>();
        expression.forEachDown(UnresolvedAttribute.class, attribute -> references.add(attribute.name()));
        return references.size() == 1 && columns.containsAll(references);
    }

    @Nullable
    private static BucketSource bucketSource(UnresolvedFunction function, Scope scope) {
        List<Expression> args = function.arguments();
        return switch (function.name().toLowerCase(Locale.ROOT)) {
            case "bucket" -> args.size() >= 2 ? fixedBucket(args.get(0), args.get(1), scope) : null;
            case "date_trunc" -> args.size() == 2 ? fixedBucket(args.get(1), args.get(0), scope) : null;
            case "tbucket" -> args.size() == 1 ? fixedBucket(null, args.get(0), scope) : null;
            default -> null;
        };
    }

    /** A bucket with a fixed {@code span}, its {@code field} traced to a source field ({@code null} for TBUCKET), or {@code null}. */
    @Nullable
    private static BucketSource fixedBucket(@Nullable Expression field, Expression span, Scope scope) {
        TimeValue spanValue = span(span);
        if (spanValue == null) {
            return null;
        }
        if (field == null) {
            return new BucketSource(null, false, spanValue);
        }
        if (field instanceof UnresolvedAttribute attribute
            && ColumnLineage.origin(attribute.name(), scope, true) instanceof SourceField sourceField) {
            return new BucketSource(sourceField.name(), false, spanValue);
        }
        return new BucketSource(null, true, spanValue);
    }

    /** A fixed span from a time-duration literal ({@code 1 hour}) or a string ({@code "1 hour"}); calendar periods are not fixed. */
    @Nullable
    private static TimeValue span(Expression span) {
        if (span instanceof Literal == false) {
            return null;
        }
        Literal literal = (Literal) span;
        if (literal.dataType() == DataType.TIME_DURATION && literal.value() instanceof Duration duration) {
            return TimeValue.timeValueMillis(duration.toMillis());
        }
        if (literal.dataType() == DataType.KEYWORD && literal.value() != null) {
            // Parse as ES|QL does: "1d" is a calendar period there, not 24 hours.
            try {
                return EsqlDataTypeConverter.maybeParseTemporalAmount(BytesRefs.toString(literal.value())) instanceof Duration duration
                    ? TimeValue.timeValueMillis(duration.toMillis())
                    : null;
            } catch (ParsingException | InvalidArgumentException | ArithmeticException e) {
                return null;
            }
        }
        return null;
    }
}

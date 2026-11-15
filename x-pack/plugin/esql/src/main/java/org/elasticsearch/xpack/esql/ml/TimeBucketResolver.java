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
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
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

    private record BucketSource(@Nullable String field, TimeValue span) {}

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
            BucketSource source = bucketSource(grouping, aggregate.child());
            if (source != null) {
                if (bucket != null) {
                    return null;
                }
                bucket = source;
                column = ((NamedExpression) grouping).name();
            }
        }
        return bucket == null ? null : new TimeBucket(bucket.field(), bucket.span(), outputColumns(column, above));
    }

    /** The bucket {@code expression} computes when it is itself a time function, or an attribute defined below as one. */
    @Nullable
    private static BucketSource bucketSource(Expression expression, LogicalPlan below) {
        Expression value = Alias.unwrap(expression);
        if (value instanceof UnresolvedFunction function) {
            return bucketSource(function);
        }
        return value instanceof UnresolvedAttribute attribute ? lineage(attribute.name(), below) : null;
    }

    /** Follows {@code column} down through EVAL, RENAME and lower STATS groupings to the time function defining it. */
    @Nullable
    private static BucketSource lineage(String column, LogicalPlan plan) {
        LogicalPlan node = plan;
        while (true) {
            switch (node) {
                case Eval eval -> {
                    // The last alias of a name wins, and a later alias may refer to an earlier one of the same EVAL.
                    for (Alias alias : eval.fields().reversed()) {
                        if (alias.name().equals(column)) {
                            if (alias.child() instanceof UnresolvedFunction function) {
                                return bucketSource(function);
                            } else if (alias.child() instanceof UnresolvedAttribute attribute) {
                                column = attribute.name();
                            } else {
                                return null;
                            }
                        }
                    }
                    node = eval.child();
                }
                case Rename rename -> {
                    for (Alias renaming : rename.renamings().reversed()) {
                        if (renaming.name().equals(column) && renaming.child() instanceof UnresolvedAttribute attribute) {
                            column = attribute.name();
                        }
                    }
                    node = rename.child();
                }
                case InlineStats ignored -> {
                    return null;
                }
                case Aggregate aggregate -> {
                    Expression grouping = grouping(aggregate, column);
                    if (grouping == null) {
                        return null;
                    }
                    return bucketSource(grouping, aggregate.child());
                }
                case UnaryPlan unary -> node = unary.child();
                // Joins, merges (FORK, FROM with subqueries) and the source command end the lineage.
                default -> {
                    return null;
                }
            }
        }
    }

    @Nullable
    private static Expression grouping(Aggregate aggregate, String column) {
        for (Expression grouping : aggregate.groupings()) {
            if (grouping instanceof NamedExpression named && named.name().equals(column)) {
                return grouping;
            }
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
                case Keep keep -> columns.removeIf(name -> keep.projections().stream().noneMatch(projection -> matches(projection, name)));
                case Drop drop -> columns.removeIf(name -> drop.removals().stream().anyMatch(removal -> matches(removal, name)));
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

    private static boolean matches(NamedExpression pattern, String column) {
        return switch (pattern) {
            case UnresolvedStar ignored -> true;
            case UnresolvedNamePattern namePattern -> namePattern.match(column);
            case UnresolvedAttribute attribute -> attribute.name().equals(column);
            default -> false;
        };
    }

    @Nullable
    private static BucketSource bucketSource(UnresolvedFunction function) {
        List<Expression> args = function.arguments();
        return switch (function.name().toLowerCase(Locale.ROOT)) {
            case "bucket" -> args.size() >= 2 ? fixedBucket(args.get(0), args.get(1)) : null;
            case "date_trunc" -> args.size() == 2 ? fixedBucket(args.get(1), args.get(0)) : null;
            case "tbucket" -> args.size() == 1 ? fixedBucket(null, args.get(0)) : null;
            default -> null;
        };
    }

    @Nullable
    private static BucketSource fixedBucket(@Nullable Expression field, Expression span) {
        if (field != null && field instanceof UnresolvedAttribute == false) {
            return null;
        }
        TimeValue spanValue = span(span);
        if (spanValue == null) {
            return null;
        }
        return new BucketSource(field == null ? null : ((UnresolvedAttribute) field).name(), spanValue);
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

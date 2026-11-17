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
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TBucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.ml.ColumnLineage.Computed;
import org.elasticsearch.xpack.esql.ml.ColumnLineage.Scope;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucketing;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucketing.BucketSource;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucketing.BucketSource.ComputedValue;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucketing.BucketSource.ImplicitTimestamp;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucketing.FixedTimeBucket;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucketing.NoTimeBucket;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucketing.Reason;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucketing.UnsupportedTimeBucket;

import java.time.Duration;
import java.time.temporal.TemporalAmount;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Finds the fixed-span time bucket a STATS groups by in the parsed, unresolved plan, and the output columns carrying it. */
final class TimeBucketResolver {

    static final TimeBucketing NO_TIME_BUCKET = new NoTimeBucket();

    private final EsqlFunctionRegistry registry;

    TimeBucketResolver(EsqlFunctionRegistry registry) {
        this.registry = registry;
    }

    /** A time bucket of the grouping: fixed over a source, or one that cannot give a fixed interval. */
    private sealed interface Found {}

    private record Fixed(BucketSource source, TimeValue span) implements Found {}

    private record Unsupported(Reason reason) implements Found {}

    /** The span argument of a bucket: a fixed {@code value}, or the {@code reason} it is not fixed. */
    private record Span(@Nullable TimeValue value, @Nullable Reason reason) {}

    /**
     * How {@code aggregate} buckets time: by the one time bucket among its groupings, if any.
     *
     * @param above the plans between the query root and {@code aggregate}, root first
     */
    TimeBucketing resolve(Aggregate aggregate, List<LogicalPlan> above) {
        Found found = null;
        String column = null;
        for (Expression grouping : aggregate.groupings()) {
            Found candidate = found(Alias.unwrap(grouping), Scope.below(aggregate.child()));
            if (candidate != null) {
                if (found != null) {
                    return new UnsupportedTimeBucket(Reason.SEVERAL_BUCKETS);
                }
                found = candidate;
                column = ((NamedExpression) grouping).name();
            }
        }
        return switch (found) {
            case null -> NO_TIME_BUCKET;
            case Unsupported unsupported -> new UnsupportedTimeBucket(unsupported.reason());
            case Fixed fixed -> new FixedTimeBucket(fixed.span(), fixed.source(), outputColumns(column, above));
        };
    }

    /**
     * The bucket {@code value} computes when it is itself a time function, or an attribute defined below as one through EVAL,
     * RENAME or a lower STATS grouping. Joins, merges and INLINE STATS end the search.
     */
    @Nullable
    private Found found(Expression value, Scope scope) {
        if (value instanceof UnresolvedFunction function) {
            return found(function, scope);
        }
        if (value instanceof UnresolvedAttribute attribute
            && ColumnLineage.origin(attribute.name(), scope, false) instanceof Computed computed
            && computed.expression() instanceof UnresolvedFunction function) {
            return found(function, computed.scope());
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
    private Found found(UnresolvedFunction function, Scope scope) {
        // Resolve through the registry so that aliases such as BIN, and any later name, reach the same function class.
        String name = registry.resolveAlias(function.name());
        if (registry.functionExists(name) == false) {
            return null;
        }
        Class<?> type = registry.resolveFunction(name).clazz();
        List<Expression> args = function.arguments();
        if (type == Bucket.class) {
            return args.size() >= 2 ? bucket(args.get(0), args.get(1), args.size() > 2, scope) : null;
        } else if (type == DateTrunc.class) {
            return args.size() == 2 ? bucket(args.get(1), args.get(0), false, scope) : null;
        } else if (type == TBucket.class) {
            return args.size() == 1 ? bucket(null, args.get(0), false, scope) : null;
        }
        return null;
    }

    /** A bucket of {@code span} over {@code field}, which is {@code null} for TBUCKET; {@code null} if it is not a time bucket. */
    @Nullable
    private static Found bucket(@Nullable Expression field, Expression span, boolean rangeGiven, Scope scope) {
        Span parsed = span(span, rangeGiven);
        if (parsed == null) {
            return null;
        }
        if (parsed.reason() != null) {
            return new Unsupported(parsed.reason());
        }
        if (field == null) {
            return new Fixed(new ImplicitTimestamp(), parsed.value());
        }
        if (field instanceof UnresolvedAttribute attribute
            && ColumnLineage.origin(attribute.name(), scope, true) instanceof ColumnLineage.SourceField sourceField) {
            return new Fixed(new BucketSource.SourceField(sourceField.name()), parsed.value());
        }
        return new Fixed(new ComputedValue(), parsed.value());
    }

    /**
     * A fixed span from a time-duration literal ({@code 1 hour}) or a string ({@code "1 hour"}); calendar periods and a bucket
     * count with a range ({@code BUCKET(ts, 20, from, to)}) are not fixed. {@code null} for anything else, such as numeric buckets.
     */
    @Nullable
    private static Span span(Expression span, boolean rangeGiven) {
        if (span instanceof Literal == false) {
            return null;
        }
        Literal literal = (Literal) span;
        if (literal.dataType() == DataType.DATE_PERIOD) {
            return new Span(null, Reason.CALENDAR_SPAN);
        }
        if (literal.dataType() == DataType.TIME_DURATION && literal.value() instanceof Duration duration) {
            return new Span(TimeValue.timeValueMillis(duration.toMillis()), null);
        }
        if ((literal.dataType() == DataType.INTEGER || literal.dataType() == DataType.LONG) && rangeGiven) {
            return new Span(null, Reason.AUTO_SPAN);
        }
        if (literal.dataType() == DataType.KEYWORD && literal.value() != null) {
            // Parse as ES|QL does: "1d" is a calendar period there, not 24 hours.
            try {
                TemporalAmount amount = EsqlDataTypeConverter.maybeParseTemporalAmount(BytesRefs.toString(literal.value()));
                if (amount == null) {
                    return null;
                }
                return amount instanceof Duration duration
                    ? new Span(TimeValue.timeValueMillis(duration.toMillis()), null)
                    : new Span(null, Reason.CALENDAR_SPAN);
            } catch (ParsingException | InvalidArgumentException | ArithmeticException e) {
                return null;
            }
        }
        return null;
    }
}

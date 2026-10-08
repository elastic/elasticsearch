/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.pushdown.StringPrefixUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvGreater;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLess;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Walks filter predicates into {@link PartitionFilterHint}s for partition pruning during glob
 * expansion and split discovery.
 *
 * <p>{@link #extract} is the pre-analysis path: {@link UnresolvedAttribute} versus {@link Literal}
 * on {@code =}, {@code !=}, {@code >}, {@code >=}, {@code <}, {@code <=}, {@code IN}, intersected
 * across every occurrence of a path.
 *
 * <p>{@link #fromConjuncts} is the resolved scan-relist path. See that method for operators,
 * column gating, and {@link LiteralNormalizer}.
 */
public final class PartitionFilterHintExtractor {

    private PartitionFilterHintExtractor() {}

    public enum Operator {
        EQUALS,
        NOT_EQUALS,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        IN;

        public boolean canRewriteGlob() {
            return this == EQUALS || this == IN;
        }
    }

    public record PartitionFilterHint(String columnName, Operator operator, List<Object> values) {
        public PartitionFilterHint {
            if (columnName == null) {
                throw new IllegalArgumentException("columnName cannot be null");
            }
            if (operator == null) {
                throw new IllegalArgumentException("operator cannot be null");
            }
            values = values != null ? List.copyOf(values) : List.of();
        }

        public boolean isSingleValue() {
            return values.size() == 1;
        }
    }

    /**
     * Turns a bound {@link Literal} into the object stored on a hint.
     */
    @FunctionalInterface
    public interface LiteralNormalizer {
        Object apply(Literal literal);
    }

    /** BytesRef → String. DATETIME stays a Long of millis. */
    public static final LiteralNormalizer LISTING = PartitionFilterHintExtractor::listingValue;

    /**
     * DATETIME {@link Number} → {@link Instant} of millis; DATE_NANOS {@link Number} →
     * {@link Instant} of epoch-second plus nano-of-second. Other types match {@link #LISTING}.
     */
    public static final LiteralNormalizer TEMPORAL = PartitionFilterHintExtractor::temporalValue;

    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    public static Map<String, List<PartitionFilterHint>> extract(LogicalPlan unresolvedPlan) {
        Map<String, List<List<PartitionFilterHint>>> perOccurrence = new LinkedHashMap<>();
        collectHints(unresolvedPlan, List.of(), perOccurrence);

        // One listing serves every occurrence of a path, so a folder may be skipped only if EVERY occurrence excludes
        // it. `FROM ds | FORK (WHERE year == 2025) (WHERE ...)` reaches the same relation twice; letting one branch's
        // hint narrow the shared listing would starve the other. An unguarded occurrence contributes no hints and so
        // vetoes the rewrite outright.
        //
        // The intersection is by exact hint equality (column, operator, values), so it keeps only hints two branches
        // spell identically. It does not reason about subsumption: `year == 2025` in one branch and `year >= 2020` in
        // another share no common hint even though the first implies the second, so the rewrite is skipped and the full
        // set is listed. That is conservative — correct, only wider — and semantic subsumption is left as a follow-up.
        Map<String, List<PartitionFilterHint>> result = new LinkedHashMap<>();
        perOccurrence.forEach((path, occurrences) -> {
            List<PartitionFilterHint> common = new ArrayList<>(occurrences.get(0));
            for (int i = 1; i < occurrences.size(); i++) {
                common.retainAll(occurrences.get(i));
            }
            if (common.isEmpty() == false) {
                result.put(path, common);
            }
        });
        return result;
    }

    /**
     * The hints in a set of conjuncts already bound to one relation occurrence, for a caller that holds the
     * filters rather than the plan they came from.
     * <p>
     * {@link #extract} exists for the phase before analysis, where one listing serves every occurrence of a path
     * and hints must therefore be intersected across them. A caller discovering files for a single occurrence has
     * no such constraint: the filters it holds are that occurrence's, and narrowing to them starves nobody.
     * <p>
     * This overload reads <em>resolved</em> columns ({@link FieldAttribute}, {@link ReferenceAttribute},
     * {@link ExternalMetadataAttribute}) against literals. Unresolved names are ignored — {@link #extract} is the
     * pre-analysis path. Comparison, {@code IN}, prefix predicates ({@code STARTS_WITH}, case-sensitive
     * {@code LIKE 'lit*'}), {@code MV_IN_RANGE}, {@code MV_GREATER}, {@code MV_LESS}, and {@code Range} emit hints
     * only for requested {@code _file.*} columns or names in {@code partitionKeys}; a data column is not a listing
     * key and must not join the listing cache identity. Prefix predicates become a GTE/LT range.
     * {@code MV_IN_RANGE} is always a closed GTE+LTE (inclusivity is not read). {@code MV_GREATER}/{@code MV_LESS}
     * are GTE/LTE ({@code include_bound} is not read). {@code Range} keeps {@code includeLower}/{@code includeUpper}
     * as written. {@code RLIKE}, {@code NOT}, and {@code OR} emit nothing. Bounds use {@link #LISTING}.
     */
    public static List<PartitionFilterHint> fromConjuncts(
        List<Expression> conjuncts,
        Set<String> requestedMetadata,
        Set<String> partitionKeys
    ) {
        return fromConjuncts(conjuncts, requestedMetadata, partitionKeys, LISTING);
    }

    /**
     * {@link #fromConjuncts(List, Set, Set)} with a bound {@link LiteralNormalizer}.
     * {@link #LISTING} is listing-cache identity (DATETIME stays a Long).
     * {@link #TEMPORAL} maps date Numbers to {@link Instant} so a spec cannot treat datetime millis as unix-seconds.
     */
    public static List<PartitionFilterHint> fromConjuncts(
        List<Expression> conjuncts,
        Set<String> requestedMetadata,
        Set<String> partitionKeys,
        LiteralNormalizer normalizer
    ) {
        List<PartitionFilterHint> hints = new ArrayList<>();
        for (Expression conjunct : conjuncts) {
            extractResolvedFromExpression(conjunct, hints, requestedMetadata, partitionKeys, normalizer);
        }
        return hints;
    }

    /** Delegates to {@link #fromConjuncts(List, Set, Set)} with no partition keys. */
    public static List<PartitionFilterHint> fromConjuncts(List<Expression> conjuncts, Set<String> requestedMetadata) {
        return fromConjuncts(conjuncts, requestedMetadata, Set.of());
    }

    /**
     * Walks top-down (pipeline-last node first), carrying the conjuncts that guard the relations below.
     *
     * <p>Two things can disqualify a conjunct on the way down, and both are decided by {@link PartitionPruningRule}:
     *
     * <ul>
     *   <li>A node that changes the row count — a {@code LIMIT}, {@code STATS}, {@code SAMPLE}, a join. A {@code WHERE}
     *       above one of those does not commute with it, so nothing collected above may be used to narrow the listing
     *       below it. The accumulator resets.</li>
     *   <li>A node that redefines a name a conjunct depends on — {@code EVAL year = ...} or {@code RENAME x AS year}.
     *       That {@code year} is a row value, not the {@code year=2024/} folder the row came from, so a hint on it must
     *       be dropped before it can rewrite the glob.</li>
     * </ul>
     *
     * <p>Shadowing is judged at the moment the walk passes the redefining node, not accumulated and applied at the
     * relation. The direction matters and is easy to get backwards: a conjunct is only endangered by a node
     * <em>between</em> it and the relation, which — walking top-down — is a node visited <em>after</em> the conjunct was
     * collected. A generating node above the filter is irrelevant. Dropping at the pass-through moment gets both right,
     * and in particular keeps the hint in {@code WHERE year == 2025 | EVAL year = 9}, where the filter reads the
     * partition column and the {@code EVAL} only affects what comes after it.
     */
    private static void collectHints(
        LogicalPlan node,
        List<Expression> guardingConjuncts,
        Map<String, List<List<PartitionFilterHint>>> result
    ) {
        if (node instanceof UnresolvedExternalRelation rel) {
            String path = extractPath(rel);
            if (path != null) {
                Set<String> requestedMetadata = requestedMetadataNames(rel);
                List<PartitionFilterHint> hints = new ArrayList<>();
                for (Expression conjunct : guardingConjuncts) {
                    extractFromExpression(conjunct, hints, requestedMetadata);
                }
                // Registered even when empty: an occurrence with no usable hint must veto the rewrite, not be ignored.
                result.computeIfAbsent(path, k -> new ArrayList<>()).add(hints);
            }
            return;
        }

        List<Expression> conjuncts = PartitionPruningRule.hintTransparent(node) ? guardingConjuncts : List.of();

        Set<String> shadowed = PartitionPruningRule.shadowedNames(node);
        if (shadowed.isEmpty() == false && conjuncts.isEmpty() == false) {
            conjuncts = conjuncts.stream().filter(c -> referencesAny(c, shadowed) == false).toList();
        }

        if (node instanceof Filter filter) {
            List<Expression> extended = new ArrayList<>(conjuncts);
            extended.addAll(Predicates.splitAnd(filter.condition()));
            conjuncts = List.copyOf(extended);
        }

        for (LogicalPlan child : node.children()) {
            collectHints(child, conjuncts, result);
        }
    }

    /** Whether {@code expression} reads any of {@code names} — matched on the attribute name, all a hint has pre-resolution. */
    private static boolean referencesAny(Expression expression, Set<String> names) {
        return expression.anyMatch(e -> e instanceof Attribute attr && names.contains(attr.name()));
    }

    /**
     * Names from the relation's {@code METADATA} clause that listing may treat as engine values.
     */
    private static Set<String> requestedMetadataNames(UnresolvedExternalRelation rel) {
        Set<String> names = new LinkedHashSet<>();
        for (NamedExpression field : rel.metadataFields()) {
            names.add(MetadataAttribute.metadataName(field));
        }
        return names;
    }

    private static void extractFromExpression(Expression expr, List<PartitionFilterHint> hints, Set<String> requestedMetadata) {
        for (Expression conjunct : Predicates.splitAnd(expr)) {
            if (conjunct instanceof EsqlBinaryComparison comparison) {
                extractFromComparison(comparison, hints, requestedMetadata);
            } else if (conjunct instanceof In in) {
                extractFromIn(in, hints, requestedMetadata);
            }
        }
    }

    private static void extractFromComparison(
        EsqlBinaryComparison comparison,
        List<PartitionFilterHint> hints,
        Set<String> requestedMetadata
    ) {
        Expression left = comparison.left();
        Expression right = comparison.right();

        String columnName = null;
        Object literalValue = null;
        boolean reversed = false;

        if (left instanceof UnresolvedAttribute attr && right instanceof Literal lit) {
            columnName = attr.name();
            literalValue = lit.value();
        } else if (left instanceof Literal lit && right instanceof UnresolvedAttribute attr) {
            columnName = attr.name();
            literalValue = lit.value();
            reversed = true;
        }

        if (columnName == null) {
            return;
        }
        if (isUnrequestedFileMetadata(columnName, requestedMetadata)) {
            return;
        }

        Operator operator = toOperator(comparison, reversed);
        if (operator != null) {
            hints.add(new PartitionFilterHint(columnName, operator, List.of(normalizeValue(literalValue))));
        }
    }

    private static void extractFromIn(In in, List<PartitionFilterHint> hints, Set<String> requestedMetadata) {
        Expression value = in.value();
        if (value instanceof UnresolvedAttribute == false) {
            return;
        }
        UnresolvedAttribute attr = (UnresolvedAttribute) value;
        String columnName = attr.name();
        if (isUnrequestedFileMetadata(columnName, requestedMetadata)) {
            return;
        }

        List<Object> literalValues = new ArrayList<>();
        for (Expression listItem : in.list()) {
            if (listItem instanceof Literal lit) {
                literalValues.add(normalizeValue(lit.value()));
            } else {
                return;
            }
        }

        if (literalValues.isEmpty() == false) {
            hints.add(new PartitionFilterHint(columnName, Operator.IN, literalValues));
        }
    }

    /**
     * Resolved conjuncts for a scan re-list. Does not call {@link #extractFromComparison} —
     * that helper requires {@link UnresolvedAttribute}.
     */
    private static void extractResolvedFromExpression(
        Expression expr,
        List<PartitionFilterHint> hints,
        Set<String> requestedMetadata,
        Set<String> partitionKeys,
        LiteralNormalizer normalizer
    ) {
        for (Expression conjunct : Predicates.splitAnd(expr)) {
            if (conjunct instanceof EsqlBinaryComparison comparison) {
                extractResolvedFromComparison(comparison, hints, requestedMetadata, partitionKeys, normalizer);
            } else if (conjunct instanceof In in) {
                extractResolvedFromIn(in, hints, requestedMetadata, partitionKeys, normalizer);
            } else if (conjunct instanceof StartsWith startsWith) {
                extractResolvedPrefix(startsWith.str(), startsWith.prefix(), hints, requestedMetadata, partitionKeys, normalizer);
            } else if (conjunct instanceof WildcardLike like
                && like.caseInsensitive() == false
                && like.pattern().shape() instanceof WildcardPattern.Shape.Prefix prefix) {
                    extractResolvedPrefix(like.field(), prefix.literal(), hints, requestedMetadata, partitionKeys);
                } else if (conjunct instanceof MvInRange mvInRange) {
                    extractResolvedMvInRange(mvInRange, hints, requestedMetadata, partitionKeys, normalizer);
                } else if (conjunct instanceof MvGreater mvGreater) {
                    extractResolvedMvCompare(
                        mvGreater.field(),
                        mvGreater.bound(),
                        Operator.GREATER_THAN_OR_EQUAL,
                        hints,
                        requestedMetadata,
                        partitionKeys,
                        normalizer
                    );
                } else if (conjunct instanceof MvLess mvLess) {
                    extractResolvedMvCompare(
                        mvLess.field(),
                        mvLess.bound(),
                        Operator.LESS_THAN_OR_EQUAL,
                        hints,
                        requestedMetadata,
                        partitionKeys,
                        normalizer
                    );
                } else if (conjunct instanceof Range range) {
                    extractResolvedRange(range, hints, requestedMetadata, partitionKeys, normalizer);
                }
        }
    }

    private static void extractResolvedFromComparison(
        EsqlBinaryComparison comparison,
        List<PartitionFilterHint> hints,
        Set<String> requestedMetadata,
        Set<String> partitionKeys,
        LiteralNormalizer normalizer
    ) {
        Expression left = comparison.left();
        Expression right = comparison.right();

        String columnName = resolvedColumnName(left);
        Literal bound = null;
        boolean reversed = false;
        if (columnName != null && right instanceof Literal lit) {
            bound = lit;
        } else {
            columnName = resolvedColumnName(right);
            if (columnName != null && left instanceof Literal lit) {
                bound = lit;
                reversed = true;
            }
        }

        if (columnName == null || bound == null || bound.value() == null) {
            return;
        }
        if (isPrefixHintColumn(columnName, requestedMetadata, partitionKeys) == false) {
            return;
        }

        Operator operator = toOperator(comparison, reversed);
        if (operator != null) {
            hints.add(new PartitionFilterHint(columnName, operator, List.of(normalizer.apply(bound))));
        }
    }

    private static void extractResolvedFromIn(
        In in,
        List<PartitionFilterHint> hints,
        Set<String> requestedMetadata,
        Set<String> partitionKeys,
        LiteralNormalizer normalizer
    ) {
        String columnName = resolvedColumnName(in.value());
        if (columnName == null || isPrefixHintColumn(columnName, requestedMetadata, partitionKeys) == false) {
            return;
        }

        List<Object> literalValues = new ArrayList<>();
        for (Expression listItem : in.list()) {
            if (listItem instanceof Literal lit) {
                literalValues.add(normalizer.apply(lit));
            } else {
                return;
            }
        }

        if (literalValues.isEmpty() == false) {
            hints.add(new PartitionFilterHint(columnName, Operator.IN, literalValues));
        }
    }

    /**
     * Closed GTE+LTE. Inclusivity is not read: a closed interval is a superset of a
     * half-open one, so the inclusive bound prunes fewer folders and never drops a live one.
     */
    private static void extractResolvedMvInRange(
        MvInRange mvInRange,
        List<PartitionFilterHint> hints,
        Set<String> requestedMetadata,
        Set<String> partitionKeys,
        LiteralNormalizer normalizer
    ) {
        String columnName = resolvedColumnName(mvInRange.field());
        if (columnName == null || isPrefixHintColumn(columnName, requestedMetadata, partitionKeys) == false) {
            return;
        }
        Literal lower = scalarBound(mvInRange.lower());
        Literal upper = scalarBound(mvInRange.upper());
        if (lower == null || upper == null) {
            return;
        }
        hints.add(new PartitionFilterHint(columnName, Operator.GREATER_THAN_OR_EQUAL, List.of(normalizer.apply(lower))));
        hints.add(new PartitionFilterHint(columnName, Operator.LESS_THAN_OR_EQUAL, List.of(normalizer.apply(upper))));
    }

    /** GTE or LTE. {@code include_bound} is not read — same closed-superset stance as {@code MV_IN_RANGE}. */
    private static void extractResolvedMvCompare(
        Expression field,
        Expression bound,
        Operator operator,
        List<PartitionFilterHint> hints,
        Set<String> requestedMetadata,
        Set<String> partitionKeys,
        LiteralNormalizer normalizer
    ) {
        String columnName = resolvedColumnName(field);
        if (columnName == null || isPrefixHintColumn(columnName, requestedMetadata, partitionKeys) == false) {
            return;
        }
        Literal scalar = scalarBound(bound);
        if (scalar == null) {
            return;
        }
        hints.add(new PartitionFilterHint(columnName, operator, List.of(normalizer.apply(scalar))));
    }

    /** BETWEEN-shaped plans: inclusivity as written. */
    private static void extractResolvedRange(
        Range range,
        List<PartitionFilterHint> hints,
        Set<String> requestedMetadata,
        Set<String> partitionKeys,
        LiteralNormalizer normalizer
    ) {
        String columnName = resolvedColumnName(range.value());
        if (columnName == null || isPrefixHintColumn(columnName, requestedMetadata, partitionKeys) == false) {
            return;
        }
        Literal lower = scalarBound(range.lower());
        Literal upper = scalarBound(range.upper());
        if (lower == null || upper == null) {
            return;
        }
        hints.add(
            new PartitionFilterHint(
                columnName,
                range.includeLower() ? Operator.GREATER_THAN_OR_EQUAL : Operator.GREATER_THAN,
                List.of(normalizer.apply(lower))
            )
        );
        hints.add(
            new PartitionFilterHint(
                columnName,
                range.includeUpper() ? Operator.LESS_THAN_OR_EQUAL : Operator.LESS_THAN,
                List.of(normalizer.apply(upper))
            )
        );
    }

    /**
     * A single non-null value. An ordered bound may be a list literal of the field's type
     * ({@code mv_in_range}, {@code MvCompare}); a list is not a bound a folder comparison can use.
     */
    private static Literal scalarBound(Expression bound) {
        if (bound instanceof Literal lit && lit.value() != null && lit.value() instanceof List == false) {
            return lit;
        }
        return null;
    }

    private static void extractResolvedPrefix(
        Expression column,
        Expression prefixExpr,
        List<PartitionFilterHint> hints,
        Set<String> requestedMetadata,
        Set<String> partitionKeys,
        LiteralNormalizer normalizer
    ) {
        if (prefixExpr instanceof Literal lit && lit.value() != null) {
            Object normalized = normalizer.apply(lit);
            if (normalized instanceof String prefix) {
                extractResolvedPrefix(column, prefix, hints, requestedMetadata, partitionKeys);
            }
        }
    }

    private static void extractResolvedPrefix(
        Expression column,
        String prefix,
        List<PartitionFilterHint> hints,
        Set<String> requestedMetadata,
        Set<String> partitionKeys
    ) {
        String columnName = resolvedColumnName(column);
        if (columnName == null || prefix.isEmpty() || isPrefixHintColumn(columnName, requestedMetadata, partitionKeys) == false) {
            return;
        }
        hints.add(new PartitionFilterHint(columnName, Operator.GREATER_THAN_OR_EQUAL, List.of(prefix)));
        BytesRef upper = StringPrefixUtils.nextPrefixUpperBound(new BytesRef(prefix));
        if (upper != null) {
            hints.add(new PartitionFilterHint(columnName, Operator.LESS_THAN, List.of(upper.utf8ToString())));
        }
    }

    /**
     * Resolved column on one side of a listing predicate. External data and hive partitions are
     * {@link ReferenceAttribute}; {@code _file.*} is {@link ExternalMetadataAttribute}. Unresolved
     * names and aliases are ignored.
     */
    private static String resolvedColumnName(Expression expr) {
        return switch (expr) {
            case FieldAttribute fa -> fa.name();
            case ExternalMetadataAttribute ema -> ema.name();
            case ReferenceAttribute ra -> ra.name();
            default -> null;
        };
    }

    /** Prefix ranges are listing keys only: requested {@code _file.*}, or a known partition column. */
    private static boolean isPrefixHintColumn(String columnName, Set<String> requestedMetadata, Set<String> partitionKeys) {
        if (FileMetadataColumns.isFileMetadataColumn(columnName)) {
            return requestedMetadata.contains(columnName);
        }
        return partitionKeys.contains(columnName);
    }

    /**
     * A {@code _file.*} predicate is a listing hint only when that name is in the relation's
     * {@code METADATA} clause. Without the clause the name is an ordinary data column.
     */
    private static boolean isUnrequestedFileMetadata(String columnName, Set<String> requestedMetadata) {
        return FileMetadataColumns.isFileMetadataColumn(columnName) && requestedMetadata.contains(columnName) == false;
    }

    private static Object listingValue(Literal literal) {
        return normalizeValue(literal.value());
    }

    private static Object temporalValue(Literal literal) {
        Object value = literal.value();
        if (value instanceof Number n) {
            DataType type = literal.dataType();
            if (type == DataType.DATETIME) {
                return Instant.ofEpochMilli(n.longValue());
            }
            if (type == DataType.DATE_NANOS) {
                long nanos = n.longValue();
                return Instant.ofEpochSecond(Math.floorDiv(nanos, NANOS_PER_SECOND), Math.floorMod(nanos, NANOS_PER_SECOND));
            }
        }
        return listingValue(literal);
    }

    private static Object normalizeValue(Object value) {
        return value instanceof BytesRef br ? BytesRefs.toString(br) : value;
    }

    private static Operator toOperator(EsqlBinaryComparison comparison, boolean reversed) {
        return switch (comparison) {
            case Equals ignored -> Operator.EQUALS;
            case NotEquals ignored -> Operator.NOT_EQUALS;
            case GreaterThan ignored -> reversed ? Operator.LESS_THAN : Operator.GREATER_THAN;
            case GreaterThanOrEqual ignored -> reversed ? Operator.LESS_THAN_OR_EQUAL : Operator.GREATER_THAN_OR_EQUAL;
            case LessThan ignored -> reversed ? Operator.GREATER_THAN : Operator.LESS_THAN;
            case LessThanOrEqual ignored -> reversed ? Operator.GREATER_THAN_OR_EQUAL : Operator.LESS_THAN_OR_EQUAL;
            default -> null;
        };
    }

    private static String extractPath(UnresolvedExternalRelation rel) {
        Expression tablePath = rel.tablePath();
        if (tablePath instanceof Literal literal && literal.value() != null) {
            return BytesRefs.toString(literal.value());
        }
        return null;
    }
}

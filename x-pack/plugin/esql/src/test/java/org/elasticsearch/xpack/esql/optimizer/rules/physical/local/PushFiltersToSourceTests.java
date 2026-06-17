/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.FieldExtract;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Verifies that {@link PushFiltersToSource#resolveFormatName} delegates to
 * {@link FormatNameResolver#resolve}. Comprehensive resolution tests live in
 * {@link org.elasticsearch.xpack.esql.datasources.FormatNameResolverTests}.
 */
public class PushFiltersToSourceTests extends ESTestCase {

    public void testResolveFormatNameDelegatesToFormatNameResolver() {
        assertEquals(
            FormatNameResolver.resolve(Map.of("reader", "java"), "file.parquet"),
            PushFiltersToSource.resolveFormatName(Map.of("reader", "java"), "file.parquet")
        );
    }

    public void testResolveFormatNameFromExtension() {
        assertEquals("orc", PushFiltersToSource.resolveFormatName(null, "s3://bucket/data/file.orc"));
    }

    // -- referencesAnyColumn: partition/data conjunct split --

    public void testReferencesAnyColumnReturnsTrueForPartitionColumn() {
        Expression expr = new Equals(SRC, fieldAttr("lang"), intLiteral(3));
        assertTrue(PushFiltersToSource.referencesAnyColumn(expr, Set.of("lang")));
    }

    public void testReferencesAnyColumnReturnsFalseForDataColumn() {
        Expression expr = new Equals(SRC, fieldAttr("salary"), intLiteral(100));
        assertFalse(PushFiltersToSource.referencesAnyColumn(expr, Set.of("lang")));
    }

    public void testReferencesAnyColumnReturnsTrueForCompoundExpressionWithPartitionColumn() {
        // A conjunct spanning both a partition column and a data column must be kept in FilterExec,
        // not pushed to the format reader (which has no partition column in its payload).
        Expression mixed = new And(
            SRC,
            new Equals(SRC, fieldAttr("lang"), intLiteral(3)),
            new Equals(SRC, fieldAttr("salary"), intLiteral(100))
        );
        assertTrue(PushFiltersToSource.referencesAnyColumn(mixed, Set.of("lang")));
    }

    public void testReferencesAnyColumnReturnsFalseForEmptyColumnSet() {
        Expression expr = new Equals(SRC, fieldAttr("lang"), intLiteral(3));
        assertFalse(PushFiltersToSource.referencesAnyColumn(expr, Set.of()));
    }

    public void testReferencesAnyColumnReturnsFalseForLiteralWithNoReferences() {
        Expression lit = new Literal(SRC, 3, DataType.INTEGER);
        assertFalse(PushFiltersToSource.referencesAnyColumn(lit, Set.of("lang")));
    }

    /**
     * The happy path: a single conjunction of one {@code >=} and one {@code <=} on the same
     * {@code field_extract(root, "k")} folds into one inclusive {@code Range}. The plan run
     * by {@code PushFiltersToSource} must therefore land a {@code RangeQuery} on the keyed
     * sub-field, with no {@code FilterExec} left behind.
     */
    public void testCombinesGreaterThanOrEqualAndLessThanOrEqualOnFieldExtractIntoRange() {
        assumeQueryPushdownEnabled();
        FieldAttribute root = flattenedRoot("resource.attributes");
        FieldExtract fe = fieldExtract(root, "host.name");
        Expression lower = new GreaterThanOrEqual(SRC, fe, Literal.keyword(SRC, "node-a"));
        Expression upper = new LessThanOrEqual(SRC, fe, Literal.keyword(SRC, "node-z"));

        List<Expression> result = PushFiltersToSource.combineFieldExtractRangePairs(
            List.of(lower, upper),
            UNMAPPED_KEY_PREDICATES,
            AttributeMap.emptyAttributeMap()
        );

        assertThat(result, hasSize(1));
        assertThat(result.get(0), instanceOf(Range.class));
        Range range = (Range) result.get(0);
        assertThat(range.value(), equalTo(fe));
        assertThat(range.lower(), equalTo(Literal.keyword(SRC, "node-a")));
        assertThat(range.upper(), equalTo(Literal.keyword(SRC, "node-z")));
        assertThat(range.includeLower(), equalTo(true));
        assertThat(range.includeUpper(), equalTo(true));
    }

    /**
     * The combine must respect the strictness of each half independently: {@code >} folds to
     * an exclusive lower bound, {@code <} folds to an exclusive upper bound. Verifies the
     * boundary flags from the original BCs land on the produced {@code Range}.
     */
    public void testCombinesGreaterThanAndLessThanOnFieldExtractIntoExclusiveRange() {
        assumeQueryPushdownEnabled();
        FieldAttribute root = flattenedRoot("resource.attributes");
        FieldExtract fe = fieldExtract(root, "host.name");
        Expression lower = new GreaterThan(SRC, fe, Literal.keyword(SRC, "node-a"));
        Expression upper = new LessThan(SRC, fe, Literal.keyword(SRC, "node-z"));

        List<Expression> result = PushFiltersToSource.combineFieldExtractRangePairs(
            List.of(lower, upper),
            UNMAPPED_KEY_PREDICATES,
            AttributeMap.emptyAttributeMap()
        );

        assertThat(result, hasSize(1));
        Range range = (Range) result.get(0);
        assertThat(range.includeLower(), equalTo(false));
        assertThat(range.includeUpper(), equalTo(false));
    }

    /**
     * A lone {@code >=} or {@code <=} on {@code field_extract} must <em>not</em> be turned into
     * a {@code Range} by the combiner. Single-sided ranges now have their own direct pushdown
     * path on {@code EsqlBinaryComparison}, so the unpaired half stays as a binary comparison
     * and is translated against the keyed flattened mapper's single-sided range support. The
     * combiner only ever folds matching {@code (lower, upper)} pairs.
     */
    public void testDoesNotCombineSingleSidedRangeOnFieldExtract() {
        assumeQueryPushdownEnabled();
        FieldAttribute root = flattenedRoot("resource.attributes");
        FieldExtract fe = fieldExtract(root, "host.name");
        Expression lower = new GreaterThanOrEqual(SRC, fe, Literal.keyword(SRC, "node-a"));

        List<Expression> conjuncts = List.of(lower);
        List<Expression> result = PushFiltersToSource.combineFieldExtractRangePairs(
            conjuncts,
            UNMAPPED_KEY_PREDICATES,
            AttributeMap.emptyAttributeMap()
        );

        // Same reference: with no matching upper, the combiner short-circuits and returns the
        // original list rather than constructing a new one. The binary comparison itself is
        // independently pushable through the {@code FieldExtract} branch on
        // {@link EsqlBinaryComparison#translatable}, so the surrounding classification picks it
        // up directly.
        assertThat(result, equalTo(conjuncts));
    }

    /**
     * Pairs across different flattened roots or different literal keys must stay separate.
     * Two halves only fold together when {@code FieldExtract#tryAsKeyedSubfieldName} returns
     * the same synthetic data-node field name for both.
     */
    public void testDoesNotCombineWhenFieldExtractRootsDiffer() {
        assumeQueryPushdownEnabled();
        FieldAttribute root = flattenedRoot("resource.attributes");
        // Two field_extract calls on the same root but for different literal sub-keys produce
        // different synthetic field names (root.host.name vs root.agent.id), so the combiner
        // groups them into separate buckets and finds no pair.
        FieldExtract hostExtract = fieldExtract(root, "host.name");
        FieldExtract agentExtract = fieldExtract(root, "agent.id");
        Expression lower = new GreaterThanOrEqual(SRC, hostExtract, Literal.keyword(SRC, "node-a"));
        Expression upper = new LessThanOrEqual(SRC, agentExtract, Literal.keyword(SRC, "id-z"));

        List<Expression> conjuncts = List.of(lower, upper);
        List<Expression> result = PushFiltersToSource.combineFieldExtractRangePairs(
            conjuncts,
            UNMAPPED_KEY_PREDICATES,
            AttributeMap.emptyAttributeMap()
        );

        assertThat(result, equalTo(conjuncts));
    }

    /**
     * The combiner resolves {@code ReferenceAttribute}s through {@code aliasReplacedBy} before
     * the {@code FieldExtract}-shape check. The applicable shape is an EVAL or RENAME that
     * aliases the <em>flattened root</em> (e.g. {@code | EVAL my_root = real_root | WHERE
     * field_extract(my_root, "k") >= "a" AND field_extract(my_root, "k") <= "z"}); after
     * resolution the two halves point at the same underlying root and fold into a single
     * {@code Range}.
     */
    public void testCombinesFieldExtractRangePairsOnAliasResolvedConjuncts() {
        assumeQueryPushdownEnabled();
        FieldAttribute realRoot = flattenedRoot("real_root");
        // The alias map records a ReferenceAttribute "my_root" pointing at the real flattened
        // attribute. The conjuncts below address the alias; only after alias resolution does
        // the field_extract see a true FieldAttribute and become eligible for the keyed-name
        // lookup.
        ReferenceAttribute aliasAttr = new ReferenceAttribute(SRC, "my_root", DataType.FLATTENED);
        AttributeMap.Builder<Attribute> aliasBuilder = AttributeMap.builder();
        aliasBuilder.put(aliasAttr, realRoot);
        AttributeMap<Attribute> aliasReplacedBy = aliasBuilder.build();

        FieldExtract aliasedExtract = new FieldExtract(SRC, aliasAttr, Literal.keyword(SRC, "host.name"));
        Expression lower = new GreaterThanOrEqual(SRC, aliasedExtract, Literal.keyword(SRC, "node-a"));
        Expression upper = new LessThanOrEqual(SRC, aliasedExtract, Literal.keyword(SRC, "node-z"));

        List<Expression> result = PushFiltersToSource.combineFieldExtractRangePairs(
            List.of(lower, upper),
            UNMAPPED_KEY_PREDICATES,
            aliasReplacedBy
        );

        assertThat(result, hasSize(1));
        Range range = (Range) result.get(0);
        // The produced Range references the resolved field_extract (the one whose field is the
        // real FieldAttribute), not the original aliased one. The classification loop's later
        // alias-resolution pass would therefore be a no-op for this Range.
        Expression rangeValue = range.value();
        assertThat(rangeValue, instanceOf(FieldExtract.class));
        assertThat(((FieldExtract) rangeValue).children().get(0), equalTo(realRoot));
    }

    /**
     * Combining must not affect {@link FieldAttribute} LHS: those are still handled by the
     * existing post-classification {@code combineEligiblePushableToRange} step. This test
     * pins down that the new pre-pass is additive, not a replacement.
     */
    public void testFieldAttributeRangePairsAreNotCombinedByPrePass() {
        FieldAttribute keyword = new FieldAttribute(
            SRC,
            "host.name",
            new EsField("host.name", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Expression lower = new GreaterThanOrEqual(SRC, keyword, Literal.keyword(SRC, "node-a"));
        Expression upper = new LessThanOrEqual(SRC, keyword, Literal.keyword(SRC, "node-z"));

        List<Expression> conjuncts = List.of(lower, upper);
        List<Expression> result = PushFiltersToSource.combineFieldExtractRangePairs(
            conjuncts,
            UNMAPPED_KEY_PREDICATES,
            AttributeMap.emptyAttributeMap()
        );

        // FieldAttribute LHS never enters the field_extract bucket. The pre-pass is a no-op
        // and returns the original list; the post-classification combineEligiblePushableToRange
        // continues to own this case.
        assertThat(result, equalTo(conjuncts));
    }

    private static FieldAttribute flattenedRoot(String name) {
        return new FieldAttribute(SRC, name, new EsField(name, DataType.FLATTENED, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private static FieldExtract fieldExtract(FieldAttribute root, String key) {
        return new FieldExtract(SRC, root, Literal.keyword(SRC, key));
    }

    private static void assumeQueryPushdownEnabled() {
        assumeTrue("fn_field_extract must be enabled for this test path", FieldExtract.isFnFieldExtractCapabilityMet());
    }

    private static final Source SRC = Source.EMPTY;

    /**
     * A stats-backed predicate whose {@code SearchStats} reports the {@code field_extract} loader config as
     * supported (i.e. the sub-key is an unmapped keyed sub-field, so it stays pushable) while relying on the
     * attribute's aggregatable flag for indexed/doc-values. The stats-less
     * {@link LucenePushdownPredicates#DEFAULT} now conservatively reports no loader config as supported
     * (can_match has no mapping access), so these recognition tests use this predicate to exercise the
     * pushable (unmapped) path that local physical planning sees with real {@code SearchStats}.
     */
    private static final LucenePushdownPredicates UNMAPPED_KEY_PREDICATES = LucenePushdownPredicates.from(
        new EsqlTestUtils.TestSearchStats(),
        new EsqlFlags(true)
    );

    private static FieldAttribute fieldAttr(String name) {
        return new FieldAttribute(SRC, name, new EsField(name, DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static Literal intLiteral(int value) {
        return new Literal(SRC, value, DataType.INTEGER);
    }
}

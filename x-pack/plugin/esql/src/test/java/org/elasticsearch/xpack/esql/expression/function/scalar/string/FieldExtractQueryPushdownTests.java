/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.querydsl.query.NotQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.TermsQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for query pushdown of {@code field_extract}. When
 * {@code field_extract(<flattened root>, "<key>")} appears on the left of {@code ==}, {@code !=},
 * {@code IN}, the range comparators ({@code >}, {@code >=}, {@code <}, {@code <=}), or the
 * {@link Range} expression (combined {@code >=}/{@code <=} closed range), the predicate is
 * translated into a bare candidate Lucene {@code TermQuery}, {@code TermsQuery}, or {@code RangeQuery}
 * against the synthetic keyed sub-field name ({@code <root>.<key>}). The comparison reports
 * {@link TranslationAware.Translatable#RECHECK}, so the FilterOperator re-applies the predicate on the
 * extracted keyword column to restore ES|QL single-value semantics instead of wrapping the query in a
 * {@code SingleValueQuery}. Pushdown only fires when the flattened root is indexed.
 * <p>
 *     The {@link FieldExtract#tryAsKeyedSubfieldName(LucenePushdownPredicates)} helper drives the
 *     recognition. The translation lives inside {@code EsqlBinaryComparison.asQuery} (for
 *     {@code ==}, {@code !=}, and the range comparators), {@code In.asQuery} (for {@code IN}),
 *     and {@code Range.asQuery} (for the closed range form).
 * </p>
 * <p>
 *     Only <em>unmapped</em> keyed sub-keys are pushed. A key declared under the flattened root's
 *     {@code properties} resolves to a real typed field, so pushing it would diverge from the keyword
 *     evaluator; {@code tryAsKeyedSubfieldName} returns empty for it. The mapped/unmapped decision needs
 *     the data-node mapping, so these tests pass a stats-backed predicate ({@code UNMAPPED_KEY_PREDICATES}
 *     or {@link #mappedKeyPredicates()}); the stats-less {@link LucenePushdownPredicates#DEFAULT} used by
 *     can_match conservatively pushes nothing.
 * </p>
 */
public class FieldExtractQueryPushdownTests extends ESTestCase {

    private static final String FLATTENED_ROOT_NAME = "resource.attributes";

    /**
     * A stats-backed predicate whose {@code SearchStats} reports the {@code field_extract} loader config as
     * supported (i.e. the sub-key is an unmapped keyed sub-field, so it stays pushable), relying on the
     * attribute's own aggregatable flag for indexed/doc-values. The stats-less
     * {@link LucenePushdownPredicates#DEFAULT} now conservatively reports no loader config as supported
     * (can_match has no mapping access), so these recognition tests use this predicate to exercise the
     * pushable (unmapped) path that local physical planning sees with real {@code SearchStats}.
     */
    private static final LucenePushdownPredicates UNMAPPED_KEY_PREDICATES = LucenePushdownPredicates.from(
        new EsqlTestUtils.TestSearchStats(),
        new EsqlFlags(true)
    );

    public void testTryAsKeyedSubfieldNameReturnsRootDotKeyForFlattenedField() {
        assumeQueryPushdownEnabled();
        FieldExtract fn = new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name"));

        assertThat(fn.tryAsKeyedSubfieldName(UNMAPPED_KEY_PREDICATES), equalTo(Optional.of("resource.attributes.host.name")));
    }

    public void testTryAsKeyedSubfieldNameReturnsEmptyForNonFlattenedField() {
        assumeQueryPushdownEnabled();
        FieldAttribute keywordRoot = new FieldAttribute(
            Source.EMPTY,
            "host.name",
            new EsField("host.name", DataType.KEYWORD, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
        FieldExtract fn = new FieldExtract(Source.EMPTY, keywordRoot, Literal.keyword(Source.EMPTY, "host.name"));

        assertThat(
            "query pushdown must require FLATTENED type on the field argument",
            fn.tryAsKeyedSubfieldName(UNMAPPED_KEY_PREDICATES),
            equalTo(Optional.empty())
        );
    }

    public void testTryAsKeyedSubfieldNameReturnsEmptyForNonFieldAttribute() {
        assumeQueryPushdownEnabled();
        Expression nonFieldAttr = new ReferenceAttribute(Source.EMPTY, "synthetic_root", DataType.FLATTENED);
        FieldExtract fn = new FieldExtract(Source.EMPTY, nonFieldAttr, Literal.keyword(Source.EMPTY, "host.name"));

        assertThat(
            "query pushdown must require a real FieldAttribute, not a ReferenceAttribute or other expression",
            fn.tryAsKeyedSubfieldName(UNMAPPED_KEY_PREDICATES),
            equalTo(Optional.empty())
        );
    }

    public void testTryAsKeyedSubfieldNameReturnsEmptyForNonFoldablePath() {
        assumeQueryPushdownEnabled();
        Expression nonFoldablePath = new ReferenceAttribute(Source.EMPTY, "path_column", DataType.KEYWORD);
        FieldExtract fn = new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), nonFoldablePath);

        assertThat(
            "query pushdown must require a foldable (constant) path. The keyed sub-field name can't be built per row",
            fn.tryAsKeyedSubfieldName(UNMAPPED_KEY_PREDICATES),
            equalTo(Optional.empty())
        );
    }

    public void testTryAsKeyedSubfieldNameReturnsEmptyWhenRootHasNoDocValues() {
        assumeQueryPushdownEnabled();
        // Without doc values the keyed sub-field can't be addressed via Lucene, so pushdown must
        // step aside and let the per-row evaluator handle it (loaded from _source).
        FieldAttribute rootWithoutDocValues = new FieldAttribute(
            Source.EMPTY,
            FLATTENED_ROOT_NAME,
            new EsField(FLATTENED_ROOT_NAME, DataType.FLATTENED, Collections.emptyMap(), false, EsField.TimeSeriesFieldType.NONE)
        );
        FieldExtract fn = new FieldExtract(Source.EMPTY, rootWithoutDocValues, Literal.keyword(Source.EMPTY, "host.name"));

        // EMPTY stats report no doc values, so the decline comes from isIndexedAndHasDocValues (the
        // attribute is also non-aggregatable) rather than the mapped/unmapped loader-config check.
        LucenePushdownPredicates noDocValuesPredicates = LucenePushdownPredicates.from(SearchStats.EMPTY, new EsqlFlags(true));
        assertThat(fn.tryAsKeyedSubfieldName(noDocValuesPredicates), equalTo(Optional.empty()));
    }

    public void testTryAsKeyedSubfieldNameReturnsEmptyWhenCapabilityDisabled() {
        // The gate is set at JVM start; this test is meaningful only when the build leaves it disabled
        // (release builds). On snapshot builds the happy-path test above already exercises the on-state.
        assumeFalse(
            "This test verifies the disabled branch. Only meaningful when fn_field_extract is off",
            FieldExtract.isFnFieldExtractCapabilityMet()
        );
        FieldExtract fn = new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name"));

        assertThat(
            "with fn_field_extract disabled the predicate must stay in the FilterExec",
            fn.tryAsKeyedSubfieldName(UNMAPPED_KEY_PREDICATES),
            equalTo(Optional.empty())
        );
    }

    public void testTryAsKeyedSubfieldNamePreservesDottedKeyVerbatim() {
        assumeQueryPushdownEnabled();
        // A multi-dot key like "service.attributes.host.name" is a single literal storage key for
        // the flattened root. The dot is part of the key, not a path separator, so the keyed
        // sub-field name is just <root>.<key> without any further interpretation.
        String dottedKey = "service.attributes.host.name";
        FieldExtract fn = new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, dottedKey));

        assertThat(
            fn.tryAsKeyedSubfieldName(UNMAPPED_KEY_PREDICATES),
            equalTo(Optional.of("resource.attributes.service.attributes.host.name"))
        );
    }

    /**
     * A key declared under the flattened root's {@code properties} resolves on the data node to a real
     * typed field, not the keyed sub-field. Query pushdown must step aside for it so {@code field_extract}
     * falls back to the per-row keyword evaluator: the keyed channel never stores mapped sub-fields, and a
     * typed-field query would apply different comparison semantics. This is what keeps the result the same
     * whether or not the optimizer pushed the call.
     */
    public void testTryAsKeyedSubfieldNameReturnsEmptyForMappedSubfield() {
        assumeQueryPushdownEnabled();
        FieldExtract fn = new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.ip"));

        assertThat(fn.tryAsKeyedSubfieldName(mappedKeyPredicates()), equalTo(Optional.empty()));
    }

    /**
     * Without {@code SearchStats} (the can_match phase uses {@link LucenePushdownPredicates#DEFAULT}) we
     * can't tell a mapped sub-field from an unmapped keyed one, so query pushdown conservatively fires for
     * nothing. The stats-backed path used during local physical planning still pushes unmapped keys.
     */
    public void testTryAsKeyedSubfieldNameReturnsEmptyWithoutStats() {
        assumeQueryPushdownEnabled();
        FieldExtract fn = new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name"));

        assertThat(fn.tryAsKeyedSubfieldName(LucenePushdownPredicates.DEFAULT), equalTo(Optional.empty()));
    }

    /**
     * The comparison operators must not translate a mapped sub-field {@code field_extract} to a Lucene
     * query: {@code EsqlBinaryComparison.translatable} reports {@code NO} so the predicate stays in the
     * {@code FilterExec} and runs on the keyword evaluator output.
     */
    public void testEqualsTranslatableNoForMappedSubfield() {
        assumeQueryPushdownEnabled();
        Equals eq = new Equals(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.ip")),
            Literal.keyword(Source.EMPTY, "10.0.0.1")
        );

        assertThat(eq.translatable(mappedKeyPredicates()), equalTo(TranslationAware.Translatable.NO));
    }

    public void testEqualsTranslatableRecheckForFieldExtractOnFlattened() {
        assumeQueryPushdownEnabled();
        Equals eq = new Equals(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a")
        );

        // field_extract pushes a candidate query and rechecks single-value equality in the compute engine.
        assertThat(eq.translatable(UNMAPPED_KEY_PREDICATES), equalTo(TranslationAware.Translatable.RECHECK));
    }

    public void testNotEqualsTranslatableRecheckForFieldExtractOnFlattened() {
        assumeQueryPushdownEnabled();
        NotEquals neq = new NotEquals(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a")
        );

        assertThat(neq.translatable(UNMAPPED_KEY_PREDICATES), equalTo(TranslationAware.Translatable.RECHECK));
    }

    /**
     * A doc-values-only flattened root (not indexed) must not push: the keyed term query would degrade to a
     * full doc-values scan. The predicate stays in the FilterExec instead.
     */
    public void testEqualsTranslatableNoWhenRootNotIndexed() {
        assumeQueryPushdownEnabled();
        Equals eq = new Equals(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a")
        );

        assertThat(eq.translatable(docValuesOnlyKeyPredicates()), equalTo(TranslationAware.Translatable.NO));
    }

    /**
     * Each of the four range comparators with a {@code field_extract(<flattened>, "<key>")} LHS
     * and a foldable RHS is now eligible for pushdown. The keyed flattened mapper substitutes
     * a key-prefix sentinel for the open bound, so the single-sided shape that was previously
     * rejected by the mapper is now safe to push.
     */
    public void testGreaterThanTranslatableYesForFieldExtractOnFlattened() {
        assumeQueryPushdownEnabled();
        GreaterThan gt = new GreaterThan(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a")
        );

        assertThat(gt.translatable(UNMAPPED_KEY_PREDICATES), equalTo(TranslationAware.Translatable.RECHECK));
    }

    public void testGreaterThanOrEqualTranslatableYesForFieldExtractOnFlattened() {
        assumeQueryPushdownEnabled();
        GreaterThanOrEqual gte = new GreaterThanOrEqual(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a")
        );

        assertThat(gte.translatable(UNMAPPED_KEY_PREDICATES), equalTo(TranslationAware.Translatable.RECHECK));
    }

    public void testLessThanTranslatableYesForFieldExtractOnFlattened() {
        assumeQueryPushdownEnabled();
        LessThan lt = new LessThan(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a")
        );

        assertThat(lt.translatable(UNMAPPED_KEY_PREDICATES), equalTo(TranslationAware.Translatable.RECHECK));
    }

    public void testLessThanOrEqualTranslatableYesForFieldExtractOnFlattened() {
        assumeQueryPushdownEnabled();
        LessThanOrEqual lte = new LessThanOrEqual(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a")
        );

        assertThat(lte.translatable(UNMAPPED_KEY_PREDICATES), equalTo(TranslationAware.Translatable.RECHECK));
    }

    /**
     * A non-foldable bound on a range comparator must keep the predicate out of the Lucene
     * pushdown path, exactly like for the closed {@link Range} form. The bound is unknown at
     * plan time so no Lucene query can encode it.
     */
    public void testGreaterThanTranslatableNoForFieldExtractWithNonFoldableBound() {
        assumeQueryPushdownEnabled();
        Expression nonFoldableBound = new ReferenceAttribute(Source.EMPTY, "ref_bound", DataType.KEYWORD);
        GreaterThan gt = new GreaterThan(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            nonFoldableBound
        );

        assertThat(gt.translatable(UNMAPPED_KEY_PREDICATES), equalTo(TranslationAware.Translatable.NO));
    }

    /**
     * Pre-existing pushdown of the range comparators over a regular indexed-and-doc-valued
     * {@link FieldAttribute} LHS must not regress. The base {@code EsqlBinaryComparison}
     * still accepts that shape first, so adding the {@code FieldExtract} branch after it must
     * not change the answer for {@code FieldAttribute}.
     */
    public void testGreaterThanTranslatableYesPreservedForFieldAttributeLhs() {
        FieldAttribute keyword = new FieldAttribute(
            Source.EMPTY,
            "host.name",
            new EsField("host.name", DataType.KEYWORD, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
        GreaterThan gt = new GreaterThan(Source.EMPTY, keyword, Literal.keyword(Source.EMPTY, "node-a"));

        // Use the conservative can_match predicate: a plain FieldAttribute must stay pushable even there,
        // confirming the new mapped-sub-field gate only affects the field_extract branch.
        assertThat(gt.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.YES));
    }

    /**
     * A closed range on {@code field_extract(root, "key")} with both bounds foldable must be
     * pushed. The underlying mapper's {@code rangeQuery} requires both bounds; this is the
     * shape we can safely push.
     */
    public void testRangeTranslatableYesForClosedRangeOnFieldExtract() {
        assumeQueryPushdownEnabled();
        Range range = new Range(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a"),
            true,
            Literal.keyword(Source.EMPTY, "node-z"),
            true,
            null
        );

        assertThat(range.translatable(UNMAPPED_KEY_PREDICATES), equalTo(TranslationAware.Translatable.RECHECK));
    }

    /**
     * If either bound of the range is not foldable, the range cannot be pushed regardless of
     * what's on the LHS. This guards the same precondition the existing translatable check
     * applies to {@link FieldAttribute} LHS.
     */
    public void testRangeTranslatableNoForFieldExtractWithNonFoldableBound() {
        assumeQueryPushdownEnabled();
        // A column reference cannot be folded at plan time so the bound is unknown, which
        // disqualifies the range from pushdown even though the LHS would otherwise be eligible.
        Expression nonFoldableUpper = new ReferenceAttribute(Source.EMPTY, "ref_upper", DataType.KEYWORD);
        Range range = new Range(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a"),
            true,
            nonFoldableUpper,
            true,
            null
        );

        assertThat(range.translatable(UNMAPPED_KEY_PREDICATES), equalTo(TranslationAware.Translatable.NO));
    }

    /**
     * The same {@code FieldExtract}-aware paths must not regress the pre-existing behavior of
     * range pushdown over a regular indexed-and-doc-valued {@link FieldAttribute} LHS.
     */
    public void testRangeTranslatableYesPreservedForFieldAttributeLhs() {
        // Indexed keyword FieldAttribute: this is the pre-existing pushable LHS.
        // LucenePushdownPredicates.DEFAULT.isPushableAttribute will accept it because
        // isAggregatable=true and the keyword data type has an exact match. Using the conservative
        // can_match predicate also confirms the new mapped-sub-field gate only affects the field_extract
        // branch, not a plain FieldAttribute.
        FieldAttribute keyword = new FieldAttribute(
            Source.EMPTY,
            "host.name",
            new EsField("host.name", DataType.KEYWORD, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Range range = new Range(
            Source.EMPTY,
            keyword,
            Literal.keyword(Source.EMPTY, "node-a"),
            true,
            Literal.keyword(Source.EMPTY, "node-z"),
            true,
            null
        );

        assertThat(range.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.YES));
    }

    /**
     * The translated query must be a bare {@code RangeQuery} on the synthetic {@code root.key} field
     * name, mirroring how {@code Equals}/{@code NotEquals}/{@code In} on the same LHS push bare candidate
     * queries. Exact single-value semantics come from the RECHECK filter, not a {@code SingleValueQuery}.
     */
    public void testRangeAsQueryProducesBareRangeQueryAgainstKeyedSubfield() {
        assumeQueryPushdownEnabled();
        String keyedName = "resource.attributes.host.name";
        Range range = new Range(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a"),
            true,
            Literal.keyword(Source.EMPTY, "node-z"),
            true,
            null
        );

        Query query = range.asQuery(UNMAPPED_KEY_PREDICATES, TranslatorHandler.TRANSLATOR_HANDLER);

        assertThat(query, equalTo(new RangeQuery(Source.EMPTY, keyedName, "node-a", true, "node-z", true, null, null)));
    }

    /**
     * Inclusive vs exclusive boundary flags on the {@code Range} must survive translation
     * unchanged. Each combination of {@code includeLower} and {@code includeUpper} should map
     * one-to-one onto the produced {@code RangeQuery}.
     */
    public void testRangeAsQueryPreservesInclusiveExclusiveBounds() {
        assumeQueryPushdownEnabled();
        String keyedName = "resource.attributes.host.name";
        // Walk all four (includeLower, includeUpper) combinations so the assertion catches any
        // accidental flipping of a flag in the translator.
        for (boolean includeLower : new boolean[] { true, false }) {
            for (boolean includeUpper : new boolean[] { true, false }) {
                Range range = new Range(
                    Source.EMPTY,
                    new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
                    Literal.keyword(Source.EMPTY, "node-a"),
                    includeLower,
                    Literal.keyword(Source.EMPTY, "node-z"),
                    includeUpper,
                    null
                );

                Query query = range.asQuery(UNMAPPED_KEY_PREDICATES, TranslatorHandler.TRANSLATOR_HANDLER);

                assertThat(
                    "for (includeLower=" + includeLower + ", includeUpper=" + includeUpper + ")",
                    query,
                    equalTo(new RangeQuery(Source.EMPTY, keyedName, "node-a", includeLower, "node-z", includeUpper, null, null))
                );
            }
        }
    }

    /**
     * The translated query for {@code field_extract(...) > "lo"} is a {@code RangeQuery} with the
     * literal on the lower side, exclusive, and a {@code null} upper side. The keyed flattened
     * mapper expands the open upper into the {@code key\1} sentinel on the data node so the
     * resulting Lucene range stays inside this key's slice of the term namespace. The query is a bare
     * candidate range; RECHECK restores exact single-value semantics in the FilterOperator.
     */
    public void testGreaterThanAsQueryProducesLowerOnlyExclusiveRangeQuery() {
        assumeQueryPushdownEnabled();
        String keyedName = "resource.attributes.host.name";
        GreaterThan gt = new GreaterThan(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a")
        );

        Query query = gt.asQuery(UNMAPPED_KEY_PREDICATES, TranslatorHandler.TRANSLATOR_HANDLER);

        assertThat(query, equalTo(new RangeQuery(Source.EMPTY, keyedName, "node-a", false, null, false, null, null)));
    }

    /**
     * Same shape as {@link #testGreaterThanAsQueryProducesLowerOnlyExclusiveRangeQuery} but with
     * an inclusive lower bound. The {@code includeLower=true} flag must reach the produced
     * {@code RangeQuery} unchanged.
     */
    public void testGreaterThanOrEqualAsQueryProducesLowerOnlyInclusiveRangeQuery() {
        assumeQueryPushdownEnabled();
        String keyedName = "resource.attributes.host.name";
        GreaterThanOrEqual gte = new GreaterThanOrEqual(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a")
        );

        Query query = gte.asQuery(UNMAPPED_KEY_PREDICATES, TranslatorHandler.TRANSLATOR_HANDLER);

        assertThat(query, equalTo(new RangeQuery(Source.EMPTY, keyedName, "node-a", true, null, false, null, null)));
    }

    /**
     * The translated query for {@code field_extract(...) < "hi"} is a {@code RangeQuery} with the
     * literal on the upper side, exclusive, and a {@code null} lower side. The mapper expands
     * the open lower into the {@code key\0} sentinel so the lower bound matches the smallest
     * term for this key on the data node.
     */
    public void testLessThanAsQueryProducesUpperOnlyExclusiveRangeQuery() {
        assumeQueryPushdownEnabled();
        String keyedName = "resource.attributes.host.name";
        LessThan lt = new LessThan(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-z")
        );

        Query query = lt.asQuery(UNMAPPED_KEY_PREDICATES, TranslatorHandler.TRANSLATOR_HANDLER);

        assertThat(query, equalTo(new RangeQuery(Source.EMPTY, keyedName, null, false, "node-z", false, null, null)));
    }

    /**
     * Same shape as {@link #testLessThanAsQueryProducesUpperOnlyExclusiveRangeQuery} but with
     * an inclusive upper bound. The {@code includeUpper=true} flag must reach the produced
     * {@code RangeQuery} unchanged.
     */
    public void testLessThanOrEqualAsQueryProducesUpperOnlyInclusiveRangeQuery() {
        assumeQueryPushdownEnabled();
        String keyedName = "resource.attributes.host.name";
        LessThanOrEqual lte = new LessThanOrEqual(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-z")
        );

        Query query = lte.asQuery(UNMAPPED_KEY_PREDICATES, TranslatorHandler.TRANSLATOR_HANDLER);

        assertThat(query, equalTo(new RangeQuery(Source.EMPTY, keyedName, null, false, "node-z", true, null, null)));
    }

    /**
     * Pushdown of range comparators against a regular {@link FieldAttribute} LHS must still
     * produce the original single-sided {@code RangeQuery} shape, with no {@code SingleValueQuery}
     * wrapper coming from the {@code field_extract} path. This guards the {@code FieldExtract}
     * branch from accidentally swallowing the {@code FieldAttribute} case.
     */
    public void testGreaterThanAsQueryAgainstFieldAttributeUsesAttributeRangeQuery() {
        FieldAttribute keyword = new FieldAttribute(
            Source.EMPTY,
            "host.name",
            new EsField("host.name", DataType.KEYWORD, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
        GreaterThan gt = new GreaterThan(Source.EMPTY, keyword, Literal.keyword(Source.EMPTY, "node-a"));

        Query query = gt.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);

        // The {@code FieldAttribute} branch keeps the keyword literal as a {@link BytesRef}, while
        // the {@code FieldExtract} branch unwraps it to a {@code String} to match the keyed
        // sub-field's {@code String}-typed bound representation. Use the same {@code BytesRef}
        // here so {@code RangeQuery.equals} (which compares with {@code Objects.equals}) passes.
        assertThat(query, equalTo(new RangeQuery(Source.EMPTY, "host.name", new BytesRef("node-a"), false, null, false, null, null)));
    }

    public void testInTranslatableYesForFieldExtractOnFlattened() {
        assumeQueryPushdownEnabled();
        In in = new In(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            List.of(Literal.keyword(Source.EMPTY, "node-a"), Literal.keyword(Source.EMPTY, "node-b"))
        );

        assertThat(in.translatable(UNMAPPED_KEY_PREDICATES), equalTo(TranslationAware.Translatable.RECHECK));
    }

    public void testEqualsAsQueryProducesBareTermQueryAgainstKeyedSubfield() {
        assumeQueryPushdownEnabled();
        String keyedName = "resource.attributes.host.name";
        Equals eq = new Equals(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a")
        );

        Query query = eq.asQuery(UNMAPPED_KEY_PREDICATES, TranslatorHandler.TRANSLATOR_HANDLER);

        // Bare candidate term query: the comparison reports RECHECK, so single-value equality is restored by
        // the FilterOperator rechecking the extracted keyword column, not by a SingleValueQuery wrapper.
        assertThat(query, equalTo(new TermQuery(Source.EMPTY, keyedName, "node-a")));
    }

    public void testNotEqualsAsQueryProducesBareNotTermQuery() {
        assumeQueryPushdownEnabled();
        String keyedName = "resource.attributes.host.name";
        NotEquals neq = new NotEquals(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a")
        );

        Query query = neq.asQuery(UNMAPPED_KEY_PREDICATES, TranslatorHandler.TRANSLATOR_HANDLER);

        // Bare candidate NotQuery; RECHECK restores exact "!=" semantics in the FilterOperator.
        assertThat(query, equalTo(new NotQuery(Source.EMPTY, new TermQuery(Source.EMPTY, keyedName, "node-a"))));
    }

    public void testInAsQueryProducesBareTermsQuery() {
        assumeQueryPushdownEnabled();
        String keyedName = "resource.attributes.host.name";
        In in = new In(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            List.of(Literal.keyword(Source.EMPTY, "node-a"), Literal.keyword(Source.EMPTY, "node-b"))
        );

        Query query = in.asQuery(UNMAPPED_KEY_PREDICATES, TranslatorHandler.TRANSLATOR_HANDLER);

        // LinkedHashSet preserves insertion order, matching translateFieldExtractIn's accumulation
        // order over list().
        LinkedHashSet<Object> expectedTerms = new LinkedHashSet<>();
        expectedTerms.add("node-a");
        expectedTerms.add("node-b");
        // Bare candidate terms query; RECHECK restores exact IN semantics in the FilterOperator.
        assertThat(query, equalTo(new TermsQuery(Source.EMPTY, keyedName, expectedTerms)));
    }

    public void testInAsQueryThrowsForAllNullList() {
        assumeQueryPushdownEnabled();
        In in = new In(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            List.of(Literal.NULL, Literal.NULL)
        );

        // The folder normally rewrites IN-with-all-nulls to a constant false before this code runs.
        // The defensive throw catches the case where it slips through to the data-node translator.
        EsqlIllegalArgumentException ex = expectThrows(
            EsqlIllegalArgumentException.class,
            () -> in.asQuery(UNMAPPED_KEY_PREDICATES, TranslatorHandler.TRANSLATOR_HANDLER)
        );
        assertThat(ex.getMessage(), equalTo("field_extract IN with all-null list cannot be translated to a query"));
    }

    private static FieldAttribute flattenedField(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, DataType.FLATTENED, Collections.emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    /**
     * A predicate whose {@code SearchStats} reports the {@code field_extract} loader config as unsupported,
     * mirroring what local physical planning sees for a key declared under the flattened root's
     * {@code properties} (the flattened field type rejects the config for mapped sub-fields).
     * {@code isIndexedAndHasDocValues} is still satisfied through the attribute's aggregatable flag, so the
     * only reason pushdown declines is the mapped-sub-field rejection in {@code supportsBlockLoaderConfig}.
     */
    private static LucenePushdownPredicates mappedKeyPredicates() {
        return LucenePushdownPredicates.from(new EsqlTestUtils.TestSearchStats(false), new EsqlFlags(true));
    }

    /**
     * A predicate whose {@code SearchStats} reports the flattened root as having doc values but not being
     * indexed, mirroring a columnar flattened field without a postings index. Query pushdown must decline
     * so the predicate stays in the {@code FilterExec} rather than pushing a full doc-values scan.
     */
    private static LucenePushdownPredicates docValuesOnlyKeyPredicates() {
        return LucenePushdownPredicates.from(new EsqlTestUtils.TestSearchStats() {
            @Override
            public boolean isIndexed(FieldAttribute.FieldName field) {
                return false;
            }
        }, new EsqlFlags(true));
    }

    private static void assumeQueryPushdownEnabled() {
        assumeTrue("fn_field_extract must be enabled for this test path", FieldExtract.isFnFieldExtractCapabilityMet());
    }
}

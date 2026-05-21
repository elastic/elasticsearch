/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
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
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for query pushdown of {@code field_extract}. When
 * {@code field_extract(<flattened root>, "<key>")} appears on the left of {@code ==}, {@code !=},
 * or {@code IN} with a constant right-hand side, the predicate is translated into a Lucene
 * {@code TermQuery}/{@code TermsQuery} against the synthetic keyed sub-field name
 * ({@code <root>.<key>}) and wrapped in {@code SingleValueQuery}.
 * <p>
 *     The {@link FieldExtract#tryAsKeyedSubfieldName(LucenePushdownPredicates)} helper drives the
 *     recognition. The translation lives inside {@code EsqlBinaryComparison.asQuery} (for
 *     {@code ==}, {@code !=}) and {@code In.asQuery} (for {@code IN}).
 * </p>
 */
public class FieldExtractQueryPushdownTests extends ESTestCase {

    private static final String FLATTENED_ROOT_NAME = "resource.attributes";

    public void testTryAsKeyedSubfieldNameReturnsRootDotKeyForFlattenedField() {
        assumeQueryPushdownEnabled();
        FieldExtract fn = new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name"));

        assertThat(fn.tryAsKeyedSubfieldName(LucenePushdownPredicates.DEFAULT), equalTo(Optional.of("resource.attributes.host.name")));
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
            fn.tryAsKeyedSubfieldName(LucenePushdownPredicates.DEFAULT),
            equalTo(Optional.empty())
        );
    }

    public void testTryAsKeyedSubfieldNameReturnsEmptyForNonFieldAttribute() {
        assumeQueryPushdownEnabled();
        Expression nonFieldAttr = new ReferenceAttribute(Source.EMPTY, "synthetic_root", DataType.FLATTENED);
        FieldExtract fn = new FieldExtract(Source.EMPTY, nonFieldAttr, Literal.keyword(Source.EMPTY, "host.name"));

        assertThat(
            "query pushdown must require a real FieldAttribute, not a ReferenceAttribute or other expression",
            fn.tryAsKeyedSubfieldName(LucenePushdownPredicates.DEFAULT),
            equalTo(Optional.empty())
        );
    }

    public void testTryAsKeyedSubfieldNameReturnsEmptyForNonFoldablePath() {
        assumeQueryPushdownEnabled();
        Expression nonFoldablePath = new ReferenceAttribute(Source.EMPTY, "path_column", DataType.KEYWORD);
        FieldExtract fn = new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), nonFoldablePath);

        assertThat(
            "query pushdown must require a foldable (constant) path. The keyed sub-field name can't be built per row",
            fn.tryAsKeyedSubfieldName(LucenePushdownPredicates.DEFAULT),
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

        assertThat(fn.tryAsKeyedSubfieldName(LucenePushdownPredicates.DEFAULT), equalTo(Optional.empty()));
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
            fn.tryAsKeyedSubfieldName(LucenePushdownPredicates.DEFAULT),
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
            fn.tryAsKeyedSubfieldName(LucenePushdownPredicates.DEFAULT),
            equalTo(Optional.of("resource.attributes.service.attributes.host.name"))
        );
    }

    public void testEqualsTranslatableYesForFieldExtractOnFlattened() {
        assumeQueryPushdownEnabled();
        Equals eq = new Equals(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a")
        );

        assertThat(eq.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.YES));
    }

    public void testNotEqualsTranslatableYesForFieldExtractOnFlattened() {
        assumeQueryPushdownEnabled();
        NotEquals neq = new NotEquals(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a")
        );

        assertThat(neq.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.YES));
    }

    public void testGreaterThanTranslatableNoForFieldExtractOnFlattened() {
        assumeQueryPushdownEnabled();
        // KeyedFlattenedFieldType.rangeQuery requires both bounds. A single-sided range like
        // field_extract(...) > "x" cannot be safely translated, so translatable must say NO and
        // the per-row evaluator handles it. The closed range form (BETWEEN) is handled by the
        // Range expression node; see testRange* below.
        GreaterThan gt = new GreaterThan(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a")
        );

        assertThat(gt.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.NO));
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

        assertThat(range.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.YES));
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

        assertThat(range.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.NO));
    }

    /**
     * The same {@code FieldExtract}-aware paths must not regress the pre-existing behavior of
     * range pushdown over a regular indexed-and-doc-valued {@link FieldAttribute} LHS.
     */
    public void testRangeTranslatableYesPreservedForFieldAttributeLhs() {
        // Indexed keyword FieldAttribute: this is the pre-existing pushable LHS.
        // LucenePushdownPredicates.DEFAULT.isPushableAttribute will accept it because
        // isAggregatable=true and the keyword data type has an exact match.
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
     * The translated query must be a {@code RangeQuery} on the synthetic {@code root.key} field
     * name wrapped in {@code SingleValueQuery}, mirroring how {@code Equals}/{@code NotEquals}/
     * {@code In} on the same LHS wrap their term queries.
     */
    public void testRangeAsQueryProducesSingleValueWrappedRangeQueryAgainstKeyedSubfield() {
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

        Query query = range.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);

        // SingleValueQuery wrapping is mandatory for the same reason it is on Equals/NotEquals/In: a
        // multi-valued sub-key would otherwise let any of its values satisfy the range,
        // which contradicts ES|QL's "multi-value compares to null" rule.
        assertThat(
            query,
            equalTo(
                new SingleValueQuery(new RangeQuery(Source.EMPTY, keyedName, "node-a", true, "node-z", true, null, null), keyedName, false)
            )
        );
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

                Query query = range.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);

                assertThat(
                    "for (includeLower=" + includeLower + ", includeUpper=" + includeUpper + ")",
                    query,
                    equalTo(
                        new SingleValueQuery(
                            new RangeQuery(Source.EMPTY, keyedName, "node-a", includeLower, "node-z", includeUpper, null, null),
                            keyedName,
                            false
                        )
                    )
                );
            }
        }
    }

    public void testInTranslatableYesForFieldExtractOnFlattened() {
        assumeQueryPushdownEnabled();
        In in = new In(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            List.of(Literal.keyword(Source.EMPTY, "node-a"), Literal.keyword(Source.EMPTY, "node-b"))
        );

        assertThat(in.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.YES));
    }

    public void testEqualsAsQueryProducesSingleValueWrappedTermQueryAgainstKeyedSubfield() {
        assumeQueryPushdownEnabled();
        String keyedName = "resource.attributes.host.name";
        Equals eq = new Equals(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a")
        );

        Query query = eq.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);

        // SVQ wrapping is mandatory: a multi-valued sub-key would otherwise let any of its values
        // satisfy the term match, which contradicts ES|QL's "multi-value compares to null" rule.
        assertThat(query, equalTo(new SingleValueQuery(new TermQuery(Source.EMPTY, keyedName, "node-a"), keyedName, false)));
    }

    public void testNotEqualsAsQueryProducesSingleValueWrappedNotTermQuery() {
        assumeQueryPushdownEnabled();
        String keyedName = "resource.attributes.host.name";
        NotEquals neq = new NotEquals(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            Literal.keyword(Source.EMPTY, "node-a")
        );

        Query query = neq.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);

        assertThat(
            query,
            equalTo(new SingleValueQuery(new NotQuery(Source.EMPTY, new TermQuery(Source.EMPTY, keyedName, "node-a")), keyedName, false))
        );
    }

    public void testInAsQueryProducesSingleValueWrappedTermsQuery() {
        assumeQueryPushdownEnabled();
        String keyedName = "resource.attributes.host.name";
        In in = new In(
            Source.EMPTY,
            new FieldExtract(Source.EMPTY, flattenedField(FLATTENED_ROOT_NAME), Literal.keyword(Source.EMPTY, "host.name")),
            List.of(Literal.keyword(Source.EMPTY, "node-a"), Literal.keyword(Source.EMPTY, "node-b"))
        );

        Query query = in.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);

        // LinkedHashSet preserves insertion order, matching translateFieldExtractIn's accumulation
        // order over list().
        LinkedHashSet<Object> expectedTerms = new LinkedHashSet<>();
        expectedTerms.add("node-a");
        expectedTerms.add("node-b");
        assertThat(query, equalTo(new SingleValueQuery(new TermsQuery(Source.EMPTY, keyedName, expectedTerms), keyedName, false)));
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
            () -> in.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER)
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

    private static void assumeQueryPushdownEnabled() {
        assumeTrue("fn_field_extract must be enabled for this test path", FieldExtract.isFnFieldExtractCapabilityMet());
    }
}

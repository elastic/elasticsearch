/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.highlight;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class HighlightSupportTests extends ESTestCase {

    private static Match match(String field, String text, MapExpression options) {
        return new Match(EMPTY, getFieldAttribute(field, KEYWORD), of(text), options);
    }

    private static MatchPhrase matchPhrase(String field, String text, MapExpression options) {
        return new MatchPhrase(EMPTY, getFieldAttribute(field, KEYWORD), of(text), options);
    }

    private static QueryString queryString(String text, MapExpression options) {
        return new QueryString(EMPTY, of(text), options, TEST_CFG);
    }

    /** A MATCH over a runtime text column that declares {@code valuesAnalyzer} via TO_TEXT (a {@link ReferenceAttribute}). */
    private static Match matchRef(String field, String valuesAnalyzer, String text, MapExpression options) {
        ReferenceAttribute ref = new ReferenceAttribute(EMPTY, null, field, TEXT, Nullability.TRUE, null, false, valuesAnalyzer);
        return new Match(EMPTY, ref, of(text), options);
    }

    private static MapExpression options(Object... keyValues) {
        List<Expression> entries = new ArrayList<>(keyValues.length);
        for (Object keyValue : keyValues) {
            entries.add(of(keyValue));
        }
        return new MapExpression(EMPTY, entries);
    }

    public void testSupportedImplicitPredicateShapes() {
        Match match = match("title", "fox", null);
        MatchPhrase phrase = matchPhrase("body", "quick fox", null);
        QueryString qstr = queryString("fox", null);
        Kql kql = new Kql(EMPTY, of("title: fox"), null, TEST_CFG);

        for (Expression supported : List.of(
            match,
            phrase,
            qstr,
            kql,
            new And(EMPTY, match, phrase),
            new Or(EMPTY, qstr, kql),
            match("title", "fox", options("fuzziness", "AUTO")),
            match("title", "fox", options("analyzer", "english")),
            matchPhrase("body", "quick fox", options("analyzer", "english")),
            queryString("fox", options("analyzer", "english")),
            queryString("fox", options("quote_analyzer", "english")),
            new Kql(EMPTY, of("title: fox"), options("analyzer", "english"), TEST_CFG),
            new And(EMPTY, match, match("body", "fox", options("analyzer", "english")))
        )) {
            assertTrue(supported.toString(), HighlightSupport.isSupportedImplicitPredicate(supported));
        }

        for (Expression unsupported : List.of(
            new Not(EMPTY, match),
            of("fox"),
            new And(EMPTY, match, new Not(EMPTY, phrase)),
            new Or(EMPTY, match, new Not(EMPTY, phrase)),
            new Or(EMPTY, match, of("fox"))
        )) {
            assertFalse(unsupported.toString(), HighlightSupport.isSupportedImplicitPredicate(unsupported));
        }
    }

    public void testUniformAnalyzerOfAgreesOnNonDefaultAnalyzer() {
        Expression query = new And(
            EMPTY,
            match("title", "fox", options("analyzer", "english")),
            matchPhrase("body", "fox", options("analyzer", "english"))
        );
        assertThat(HighlightSupport.uniformAnalyzerOf(query), equalTo("english"));
    }

    public void testUniformAnalyzerOfReturnsNullForStandard() {
        Expression query = new And(EMPTY, match("title", "fox", null), match("body", "fox", options("analyzer", "standard")));
        assertNull(HighlightSupport.uniformAnalyzerOf(query));
    }

    public void testUniformAnalyzerOfReturnsNullOnDisagreement() {
        Expression query = new And(EMPTY, match("title", "fox", options("analyzer", "english")), match("body", "fox", null));
        assertNull(HighlightSupport.uniformAnalyzerOf(query));
    }

    public void testUniformAnalyzerOfReturnsNullWithNoFullTextLeaf() {
        assertNull(HighlightSupport.uniformAnalyzerOf(of("fox")));
    }

    public void testLeafAnalyzerNamesOfCollectsQuoteAnalyzers() {
        Expression query = new Or(
            EMPTY,
            queryString("fox", options("quote_analyzer", "english")),
            queryString("dog", options("quote_analyzer", "whitespace"))
        );
        assertThat(HighlightSupport.leafAnalyzerNamesOf(query), equalTo(Set.of("english", "whitespace")));
    }

    public void testLeafAnalyzerNamesOfEmptyWithoutAnalyzerOptions() {
        assertTrue(HighlightSupport.leafAnalyzerNamesOf(match("title", "fox", null)).isEmpty());
        assertThat(HighlightSupport.leafAnalyzerNamesOf(match("title", "fox", options("analyzer", "english"))), equalTo(Set.of("english")));
    }

    public void testCollectImplicitQueryKeepsSingleExtraQuoteAnalyzer() {
        Expression leaf = queryString("fox", options("analyzer", "english", "quote_analyzer", "whitespace"));
        HighlightSupport.ImplicitQuery implicitQuery = HighlightSupport.collectImplicitQuery(filter(leaf), EMPTY);

        assertThat(implicitQuery.query(), equalTo(leaf));
        assertThat(implicitQuery.analyzerName(), equalTo("english"));
        assertThat(HighlightSupport.leafAnalyzerNamesOf(leaf), equalTo(Set.of("english", "whitespace")));
    }

    public void testFieldAnalyzersAssignsEachLeafsFieldItsOwnAnalyzer() {
        Expression query = new Or(
            EMPTY,
            match("title", "fox", options("analyzer", "english")),
            match("body", "fox", options("analyzer", "whitespace"))
        );
        assertThat(
            HighlightSupport.fieldAnalyzers(query, null, List.of("title", "body")),
            equalTo(Map.of("title", "english", "body", "whitespace"))
        );
    }

    public void testFieldAnalyzersCommandAnalyzerOverridesLeaves() {
        // A command analyzer (the user's WITH, or the uniform analyzer synthesized from the borrow) overrides every
        // leaf, including a field a leaf already labels. Here title's "english" leaf is superseded by "whitespace".
        Expression query = match("title", "fox", options("analyzer", "english"));
        assertThat(
            HighlightSupport.fieldAnalyzers(query, "whitespace", List.of("title", "body")),
            equalTo(Map.of("title", "whitespace", "body", "whitespace"))
        );
    }

    public void testFieldAnalyzersWithoutCommandAnalyzerFallsBackPerField() {
        Expression query = match("title", "fox", options("analyzer", "english"));
        assertThat(
            HighlightSupport.fieldAnalyzers(query, null, List.of("title", "body")),
            equalTo(Map.of("title", "english", "body", "standard"))
        );
    }

    public void testFieldAnalyzersCommandAnalyzerOverridesConflictingLeaves() {
        // Two leaves disagree on title's analyzer, which throws with no command analyzer (see
        // testFieldAnalyzersRejectsSameFieldTwoAnalyzers). A command analyzer overrides both, so nothing conflicts.
        Expression query = new Or(
            EMPTY,
            match("title", "fox", options("analyzer", "english")),
            match("title", "dog", options("analyzer", "whitespace"))
        );
        assertThat(HighlightSupport.fieldAnalyzers(query, "keyword", List.of("title")), equalTo(Map.of("title", "keyword")));
    }

    public void testFieldAnalyzersUnlabeledLeafUsesValuesAnalyzer() {
        // A TO_TEXT column defaults MATCH's query analyzer to its values analyzer, so an unlabeled leaf that borrowed
        // that match must highlight with the same analyzer, not the standard fallback (which would return no snippet).
        Expression query = matchRef("title", "english", "ring", null);
        assertThat(HighlightSupport.fieldAnalyzers(query, null, List.of("title")), equalTo(Map.of("title", "english")));
    }

    public void testFieldAnalyzersExplicitOptionOverridesValuesAnalyzer() {
        // An explicit analyzer option overrides the query side, so it also wins over the column's values analyzer.
        Expression query = matchRef("title", "english", "ring", options("analyzer", "whitespace"));
        assertThat(HighlightSupport.fieldAnalyzers(query, null, List.of("title")), equalTo(Map.of("title", "whitespace")));
    }

    public void testFieldAnalyzersValuesAnalyzerAgreesWithMatchingOption() {
        // An unlabeled leaf's values analyzer and a sibling naming the same analyzer explicitly do not conflict.
        Expression query = new Or(EMPTY, matchRef("title", "english", "ring", null), match("title", "fox", options("analyzer", "english")));
        assertThat(HighlightSupport.fieldAnalyzers(query, null, List.of("title")), equalTo(Map.of("title", "english")));
    }

    public void testFieldAnalyzersValuesAnalyzerConflictsWithDifferentOption() {
        Expression query = new Or(
            EMPTY,
            matchRef("title", "english", "ring", null),
            match("title", "fox", options("analyzer", "whitespace"))
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightSupport.fieldAnalyzers(query, null, List.of("title"))
        );
        assertThat(e.getMessage(), containsString("different analyzers"));
    }

    public void testFieldAnalyzersFieldLessLeafBroadcastsToEveryField() {
        Expression query = queryString("fox", options("analyzer", "english"));
        assertThat(
            HighlightSupport.fieldAnalyzers(query, null, List.of("title", "body")),
            equalTo(Map.of("title", "english", "body", "english"))
        );
    }

    public void testFieldAnalyzersRejectsSameFieldTwoAnalyzers() {
        Expression query = new Or(
            EMPTY,
            match("title", "fox", options("analyzer", "english")),
            match("title", "dog", options("analyzer", "whitespace"))
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightSupport.fieldAnalyzers(query, null, List.of("title"))
        );
        assertThat(e.getMessage(), containsString("different analyzers"));
    }

    public void testFieldAnalyzersRejectsFieldLessLeafConflictingWithPerFieldAnalyzer() {
        Expression query = new Or(
            EMPTY,
            match("title", "fox", options("analyzer", "whitespace")),
            queryString("fox", options("analyzer", "english"))
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightSupport.fieldAnalyzers(query, null, List.of("title"))
        );
        assertThat(e.getMessage(), containsString("different analyzers"));
    }

    public void testFieldAnalyzersUnlabeledFieldLessLeafDoesNotConflict() {
        Expression query = new Or(EMPTY, match("title", "fox", options("analyzer", "whitespace")), queryString("fox", null));
        assertThat(HighlightSupport.fieldAnalyzers(query, null, List.of("title")), equalTo(Map.of("title", "whitespace")));
    }

    private static Filter filter(Expression condition) {
        Attribute title = getFieldAttribute("title", TEXT);
        Attribute body = getFieldAttribute("body", TEXT);
        LogicalPlan relation = new LocalRelation(EMPTY, List.of(title, body), null);
        return new Filter(EMPTY, relation, condition);
    }

    public void testAllHighlightableFieldsFiltersAndDeduplicates() {
        Attribute firstDuplicate = getFieldAttribute("duplicate", KEYWORD);
        Attribute integer = getFieldAttribute("count", INTEGER);
        Attribute metadata = new MetadataAttribute(EMPTY, MetadataAttribute.INDEX, KEYWORD, true);
        // Keeping body before the replacement duplicate verifies that putLast moves the duplicate to the end.
        Attribute body = getFieldAttribute("body", TEXT);
        Attribute lastDuplicate = getFieldAttribute("duplicate", TEXT);

        List<NamedExpression> fields = HighlightSupport.allHighlightableFields(
            List.of(firstDuplicate, integer, metadata, body, lastDuplicate)
        );

        assertThat(fields, equalTo(List.of(body, lastDuplicate)));
    }

    public void testDeriveFieldsFromPositiveQueryReferences() {
        Attribute title = getFieldAttribute("title", TEXT);
        Attribute body = getFieldAttribute("body", TEXT);
        Expression query = new And(EMPTY, match("title", "fox", null), new Not(EMPTY, match("body", "bar", null)));

        assertThat(HighlightSupport.deriveFields(query, List.of(title, body)), equalTo(List.of(title)));
        assertTrue(HighlightSupport.deriveFields(new Not(EMPTY, match("body", "bar", null)), List.of(title, body)).isEmpty());
    }

    public void testDeriveFieldsDoesNotNarrowQueryStringWithFieldQualifier() {
        Attribute title = getFieldAttribute("title", TEXT);
        Attribute body = getFieldAttribute("body", TEXT);

        // A QSTR string can name arbitrary fields with `field:term`, so default_field alone cannot bound the target set.
        assertThat(
            HighlightSupport.deriveFields(queryString("description:Tolkien", options("default_field", "title")), List.of(title, body)),
            equalTo(List.of(title, body))
        );
    }

    public void testDeriveFieldsNotKqlFallsBackToAllHighlightableFields() {
        Attribute title = getFieldAttribute("title", TEXT);
        Attribute body = getFieldAttribute("body", TEXT);
        List<Attribute> output = List.of(title, body);

        assertThat(
            HighlightSupport.deriveFields(new Not(EMPTY, new Kql(EMPTY, of("foo"), null, TEST_CFG)), output),
            equalTo(List.of(title, body))
        );
    }

    public void testDeriveFieldsFallsBackWhenNoSpecificFieldExists() {
        Attribute title = getFieldAttribute("title", TEXT);
        Attribute body = getFieldAttribute("body", KEYWORD);
        Attribute count = getFieldAttribute("count", INTEGER);
        List<Attribute> output = List.of(title, body, count);

        assertThat(HighlightSupport.deriveFields(of("fox"), output), equalTo(List.of(title, body)));
        assertThat(HighlightSupport.deriveFields(queryString("fox", null), output), equalTo(List.of(title, body)));
        assertThat(
            HighlightSupport.deriveFields(queryString("fox", options("default_field", "ti*")), output),
            equalTo(List.of(title, body))
        );
        assertThat(HighlightSupport.deriveFields(new Kql(EMPTY, of("title: fox"), null, TEST_CFG), output), equalTo(List.of(title, body)));
    }

    public void testDeriveFieldsSkipsMissingAndNonStringReferences() {
        Attribute title = getFieldAttribute("title", TEXT);
        Attribute count = getFieldAttribute("count", INTEGER);
        Expression query = new And(EMPTY, match("missing", "fox", null), match("count", "1", null));

        assertTrue(HighlightSupport.deriveFields(query, List.of(title, count)).isEmpty());
    }

    public void testDeriveFieldsExcludesMetadataFields() {
        Attribute index = new MetadataAttribute(EMPTY, MetadataAttribute.INDEX, KEYWORD, true);
        Expression query = match(MetadataAttribute.INDEX, "fox", null);

        assertTrue(HighlightSupport.deriveFields(query, List.of(index)).isEmpty());
    }

    public void testDeriveFieldsUsesLastAttributeForDuplicateNames() {
        Attribute firstDuplicate = getFieldAttribute("title", KEYWORD);
        Attribute lastDuplicate = getFieldAttribute("title", TEXT);
        Expression query = match("title", "fox", null);

        assertThat(HighlightSupport.deriveFields(query, List.of(firstDuplicate, lastDuplicate)), equalTo(List.of(lastDuplicate)));
    }

    public void testFieldsRequiredForTranslationKeepsNegativeMatch() {
        Expression query = new And(EMPTY, match("title", "fox", null), new Not(EMPTY, match("body", "bar", null)));
        assertThat(HighlightSupport.fieldsRequiredForTranslation(query), equalTo(Set.of("title", "body")));
    }

    public void testFieldsRequiredForTranslationColonFreeLiteralCanPrune() {
        assertThat(HighlightSupport.fieldsRequiredForTranslation(of("fox")), equalTo(Set.of()));
        assertThat(HighlightSupport.fieldsRequiredForTranslation(of("fox bar")), equalTo(Set.of()));
    }

    public void testFieldsRequiredForTranslationFieldQualifiedLiteralKeepsAll() {
        assertNull(HighlightSupport.fieldsRequiredForTranslation(of("password:fox")));
        assertNull(HighlightSupport.fieldsRequiredForTranslation(new And(EMPTY, match("title", "fox", null), of("body:bar"))));
    }

    public void testFieldsRequiredForTranslationQueryStringKeepsAll() {
        assertNull(HighlightSupport.fieldsRequiredForTranslation(queryString("title:fox", null)));
        assertNull(HighlightSupport.fieldsRequiredForTranslation(new Kql(EMPTY, of("title: fox"), null, TEST_CFG)));
    }
}

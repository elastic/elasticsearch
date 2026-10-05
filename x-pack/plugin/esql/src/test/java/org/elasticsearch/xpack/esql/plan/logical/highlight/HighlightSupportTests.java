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
import org.elasticsearch.xpack.esql.core.expression.NameId;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
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
            queryString("fox", options("quote_analyzer", "english"))
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

    public void testUniformAnalyzerAgreement() {
        Expression unlabeled = match("title", "fox", null);
        Expression named = match("title", "fox", options("analyzer", "english"));
        Expression namedAnd = new And(
            EMPTY,
            match("title", "fox", options("analyzer", "english")),
            match("body", "bar", options("analyzer", "english"))
        );
        Expression namedOrUnlabeled = new Or(EMPTY, named, match("body", "bar", null));

        assertNull(HighlightSupport.uniformAnalyzerOf(unlabeled));
        assertThat(HighlightSupport.uniformAnalyzerOf(named), equalTo("english"));
        assertThat(HighlightSupport.uniformAnalyzerOf(namedAnd), equalTo("english"));
        assertThat(HighlightSupport.uniformAnalyzerOf(namedOrUnlabeled), equalTo("english"));

        HighlightSupport.requireUniformAnalyzer(unlabeled, null);
        HighlightSupport.requireUniformAnalyzer(named, "english");
        HighlightSupport.requireUniformAnalyzer(unlabeled, "english");
        HighlightSupport.requireUniformAnalyzer(namedAnd, "english");
        HighlightSupport.requireUniformAnalyzer(named, null, "english");

        IllegalArgumentException missingValues = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightSupport.requireUniformAnalyzer(named, null)
        );
        assertThat(
            missingValues.getMessage(),
            equalTo("HIGHLIGHT query analyzer [english] does not match the values analyzer [standard]; they must be the same")
        );
    }

    public void testValuesAnalyzerName() {
        assertNull(HighlightSupport.valuesAnalyzerName(List.of(getFieldAttribute("title", TEXT))));
        assertNull(HighlightSupport.valuesAnalyzerName(List.of(textField("title", null))));
        assertNull(HighlightSupport.valuesAnalyzerName(List.of(textField("title", "standard"))));
        assertThat(HighlightSupport.valuesAnalyzerName(List.of(textField("title", "english"))), equalTo("english"));
        assertThat(
            HighlightSupport.valuesAnalyzerName(List.of(textField("title", "english"), textField("body", "english"))),
            equalTo("english")
        );

        IllegalArgumentException mixed = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightSupport.valuesAnalyzerName(List.of(textField("title", "english"), textField("body", "whitespace")))
        );
        assertThat(
            mixed.getMessage(),
            equalTo("HIGHLIGHT ON fields use different values analyzers [english, whitespace]; they must be the same")
        );

        IllegalArgumentException mixedWithDefault = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightSupport.valuesAnalyzerName(List.of(textField("title", "english"), getFieldAttribute("body", TEXT)))
        );
        assertThat(
            mixedWithDefault.getMessage(),
            equalTo("HIGHLIGHT ON fields use different values analyzers [english, standard]; they must be the same")
        );
    }

    private static ReferenceAttribute textField(String name, String valuesAnalyzer) {
        return new ReferenceAttribute(EMPTY, null, name, TEXT, Nullability.FALSE, new NameId(), false, valuesAnalyzer);
    }

    public void testRequireUniformAnalyzerRejectsMixedLeaves() {
        Expression query = new Or(
            EMPTY,
            match("title", "fox", options("analyzer", "english")),
            match("body", "bar", options("analyzer", "whitespace"))
        );
        assertNull(HighlightSupport.uniformAnalyzerOf(query));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightSupport.requireUniformAnalyzer(query, null)
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "HIGHLIGHT full-text functions use different analyzers [english, whitespace]; "
                    + "use the same analyzer for every clause, or write an explicit HIGHLIGHT query using a single analyzer"
            )
        );
    }

    public void testRequireUniformAnalyzerRejectsWithMismatch() {
        Expression query = match("title", "fox", options("analyzer", "english"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HighlightSupport.requireUniformAnalyzer(query, "whitespace")
        );
        assertThat(
            e.getMessage(),
            equalTo("HIGHLIGHT WITH analyzer [whitespace] does not match analyzer [english] specified by the query; they must be the same")
        );
    }

    public void testAllHighlightableFieldsFiltersAndDeduplicates() {
        Attribute firstDuplicate = getFieldAttribute("duplicate", KEYWORD);
        Attribute integer = getFieldAttribute("count", INTEGER);
        Attribute metadata = new MetadataAttribute(EMPTY, MetadataAttribute.INDEX, KEYWORD, true);
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

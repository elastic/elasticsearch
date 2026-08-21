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
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.hamcrest.Matchers.equalTo;

/** Tests the analysis-time HIGHLIGHT helpers: implicit-predicate support and field selection/derivation. */
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

        assertTrue(HighlightSupport.isSupportedImplicitPredicate(match));
        assertTrue(HighlightSupport.isSupportedImplicitPredicate(phrase));
        assertTrue(HighlightSupport.isSupportedImplicitPredicate(qstr));
        assertTrue(HighlightSupport.isSupportedImplicitPredicate(kql));
        assertTrue(HighlightSupport.isSupportedImplicitPredicate(new And(EMPTY, match, phrase)));
        assertTrue(HighlightSupport.isSupportedImplicitPredicate(new Or(EMPTY, qstr, kql)));
        assertFalse(HighlightSupport.isSupportedImplicitPredicate(new Not(EMPTY, match)));
        assertFalse(HighlightSupport.isSupportedImplicitPredicate(of("fox")));
        assertFalse(HighlightSupport.isSupportedImplicitPredicate(new And(EMPTY, match, new Not(EMPTY, phrase))));
    }

    public void testAllHighlightableFieldsFiltersAndDeduplicates() {
        Attribute firstDuplicate = getFieldAttribute("duplicate", KEYWORD);
        Attribute integer = getFieldAttribute("count", INTEGER);
        Attribute metadata = new MetadataAttribute(EMPTY, MetadataAttribute.INDEX, KEYWORD, true);
        Attribute lastDuplicate = getFieldAttribute("duplicate", TEXT);
        Attribute body = getFieldAttribute("body", TEXT);

        List<NamedExpression> fields = HighlightSupport.allHighlightableFields(
            List.of(firstDuplicate, integer, metadata, lastDuplicate, body)
        );

        assertThat(fields, equalTo(List.of(lastDuplicate, body)));
    }

    public void testAllHighlightableFieldsMovesDuplicatesToEnd() {
        // Unlike the fixture above, `body` sits BETWEEN the two colliding `duplicate` attributes, so this input can
        // actually distinguish "relocate to end" (remove+put) from "overwrite in place" (plain put).
        Attribute firstDuplicate = getFieldAttribute("duplicate", KEYWORD);
        Attribute body = getFieldAttribute("body", TEXT);
        Attribute lastDuplicate = getFieldAttribute("duplicate", TEXT);

        assertThat(
            HighlightSupport.allHighlightableFields(List.of(firstDuplicate, body, lastDuplicate)),
            equalTo(List.of(body, lastDuplicate))
        );
    }

    public void testDeriveFieldsFromPositiveQueryReferences() {
        Attribute title = getFieldAttribute("title", TEXT);
        Attribute body = getFieldAttribute("body", TEXT);
        Expression query = new And(EMPTY, match("title", "fox", null), new Not(EMPTY, match("body", "bar", null)));

        assertThat(HighlightSupport.deriveFields(query, List.of(title, body)), equalTo(List.of(title)));
        assertTrue(HighlightSupport.deriveFields(new Not(EMPTY, match("body", "bar", null)), List.of(title, body)).isEmpty());
    }

    public void testDeriveFieldsUsesConcreteQueryStringDefaultField() {
        Attribute title = getFieldAttribute("title", TEXT);
        Attribute body = getFieldAttribute("body", KEYWORD);

        assertThat(
            HighlightSupport.deriveFields(queryString("fox", options("default_field", "title")), List.of(title, body)),
            equalTo(List.of(title))
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

    // deriveFields is defined in terms of allHighlightableFields, so a query naming a metadata field derives nothing
    // for it, matching the ON * form's deliberate exclusion of metadata columns.
    public void testDeriveFieldsExcludesMetadataFields() {
        Attribute index = new MetadataAttribute(EMPTY, MetadataAttribute.INDEX, KEYWORD, true);
        Expression query = match(MetadataAttribute.INDEX, "fox", null);

        assertTrue(HighlightSupport.deriveFields(query, List.of(index)).isEmpty());
    }

    // deriveFields resolves a named, duplicated column to the same attribute allHighlightableFields (ON *) would
    // pick: the last one in the child output, not the first.
    public void testDeriveFieldsUsesLastAttributeForDuplicateNames() {
        Attribute firstDuplicate = getFieldAttribute("title", KEYWORD);
        Attribute lastDuplicate = getFieldAttribute("title", TEXT);
        Expression query = match("title", "fox", null);

        assertThat(HighlightSupport.deriveFields(query, List.of(firstDuplicate, lastDuplicate)), equalTo(List.of(lastDuplicate)));
    }
}

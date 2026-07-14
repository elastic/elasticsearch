/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.kql.parser.KqlParser;

import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

/**
 * The KQL parser, driven by a dataset schema instead of an index mapping, emits the same DSL shapes it would for an
 * index of the same field types — the field-type decisions come from {@link DatasetKqlParsingContext}, not a mapping.
 */
public class DatasetKqlParsingContextTests extends ESTestCase {

    private static final Map<String, DataType> SCHEMA = Map.of(
        "name",
        DataType.KEYWORD,
        "ts",
        DataType.DATETIME,
        "n",
        DataType.INTEGER,
        "flag",
        DataType.BOOLEAN,
        "body",
        DataType.TEXT
    );

    private static QueryBuilder parse(String kql) {
        return new KqlParser().parseKqlQuery(kql, new DatasetKqlParsingContext(SCHEMA, false, null, null));
    }

    public void testKeywordFieldBecomesTerm() {
        assertThat(parse("name: foo"), instanceOf(TermQueryBuilder.class));
    }

    public void testDateFieldBecomesRange() {
        assertThat(parse("ts >= \"2020-01-01\""), instanceOf(RangeQueryBuilder.class));
        assertThat(parse("ts: \"2020-01-01\""), instanceOf(RangeQueryBuilder.class)); // equality on a date is a range
    }

    public void testNumericFieldBecomesLenientMatch() {
        assertThat(parse("n: 5"), instanceOf(MatchQueryBuilder.class));
    }

    public void testTextFieldBecomesMatch() {
        // text routes to match — the translator degrades it loudly rather than the parser dropping it
        assertThat(parse("body: hello"), instanceOf(MatchQueryBuilder.class));
    }

    public void testStarBecomesExists() {
        assertThat(parse("name: *"), instanceOf(ExistsQueryBuilder.class));
    }

    public void testUnknownFieldMatchesNone() {
        assertThat(parse("nope: foo"), instanceOf(MatchNoneQueryBuilder.class));
    }

    public void testDisjunctionBecomesBool() {
        assertThat(parse("name: (a or b)"), instanceOf(BoolQueryBuilder.class));
        assertThat(parse("name: a and n: 5"), instanceOf(BoolQueryBuilder.class));
    }

    public void testWildcardOnKeyword() {
        // the parser emits a wildcard; the translator is what rejects it (no wildcard arm yet)
        assertThat(parse("name: fo*"), instanceOf(WildcardQueryBuilder.class));
    }

    public void testQuotedFieldlessBecomesMultiMatch() {
        assertThat(parse("\"a phrase\""), instanceOf(MultiMatchQueryBuilder.class));
    }
}

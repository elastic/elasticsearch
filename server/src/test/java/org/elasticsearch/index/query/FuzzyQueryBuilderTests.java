/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class FuzzyQueryBuilderTests extends AbstractQueryTestCase<FuzzyQueryBuilder> {

    @Override
    protected FuzzyQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME);
        FuzzyQueryBuilder query = new FuzzyQueryBuilder(fieldName, getRandomValueForFieldName(fieldName));
        if (randomBoolean()) {
            query.fuzziness(randomFuzziness(query.fieldName()));
        }
        if (randomBoolean()) {
            query.prefixLength(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            query.maxExpansions(randomIntBetween(1, 10));
        }
        if (randomBoolean()) {
            query.transpositions(randomBoolean());
        }
        if (randomBoolean()) {
            query.rewrite(getRandomRewriteMethod());
        }
        return query;
    }

    @Override
    protected Map<String, FuzzyQueryBuilder> getAlternateVersions() {
        Map<String, FuzzyQueryBuilder> alternateVersions = new HashMap<>();
        FuzzyQueryBuilder fuzzyQuery = new FuzzyQueryBuilder(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10));
        String contentString = Strings.format("""
            {
                "fuzzy" : {
                    "%s" : "%s"
                }
            }""", fuzzyQuery.fieldName(), fuzzyQuery.value());
        alternateVersions.put(contentString, fuzzyQuery);
        return alternateVersions;
    }

    @Override
    protected void doAssertLuceneQuery(FuzzyQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(query, instanceOf(FuzzyQuery.class));

        FuzzyQuery fuzzyQuery = (FuzzyQuery) query;
        String expectedFieldName = expectedFieldName(queryBuilder.fieldName());
        String actualFieldName = fuzzyQuery.getTerm().field();
        assertThat(actualFieldName, equalTo(expectedFieldName));
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryBuilder(null, "text"));
        assertEquals("field name cannot be null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryBuilder("", "text"));
        assertEquals("field name cannot be null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryBuilder("field", null));
        assertEquals("query value cannot be null", e.getMessage());
    }

    public void testToQueryWithStringField() throws IOException {
        String query = Strings.format("""
            {
                "fuzzy":{
                    "%s":{
                        "value":"sh",
                        "fuzziness": "AUTO",
                        "prefix_length":1,
                        "boost":2.0
                    }
                }
            }""", TEXT_FIELD_NAME);
        Query parsedQuery = parseQuery(query).toQuery(createSearchExecutionContext());
        assertThat(parsedQuery, instanceOf(BoostQuery.class));
        BoostQuery boostQuery = (BoostQuery) parsedQuery;
        assertThat(boostQuery.getBoost(), equalTo(2.0f));
        assertThat(boostQuery.getQuery(), instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) boostQuery.getQuery();
        assertThat(fuzzyQuery.getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "sh")));
        assertThat(fuzzyQuery.getMaxEdits(), equalTo(Fuzziness.AUTO.asDistance("sh")));
        assertThat(fuzzyQuery.getPrefixLength(), equalTo(1));
    }

    public void testToQueryWithStringFieldDefinedFuzziness() throws IOException {
        String query = Strings.format("""
            {
                "fuzzy":{
                    "%s":{
                        "value":"sh",
                        "fuzziness": "AUTO:2,5",
                        "prefix_length":1,
                        "boost":2.0
                    }
                }
            }""", TEXT_FIELD_NAME);
        Query parsedQuery = parseQuery(query).toQuery(createSearchExecutionContext());
        assertThat(parsedQuery, instanceOf(BoostQuery.class));
        BoostQuery boostQuery = (BoostQuery) parsedQuery;
        assertThat(boostQuery.getBoost(), equalTo(2.0f));
        assertThat(boostQuery.getQuery(), instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) boostQuery.getQuery();
        assertThat(fuzzyQuery.getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "sh")));
        assertThat(fuzzyQuery.getMaxEdits(), equalTo(1));
        assertThat(fuzzyQuery.getPrefixLength(), equalTo(1));
    }

    public void testToQueryWithStringFieldDefinedWrongFuzziness() throws IOException {
        String queryMissingFuzzinessUpLimit = Strings.format("""
            {
                "fuzzy":{
                    "%s":{
                        "value":"sh",
                        "fuzziness": "AUTO:2",
                        "prefix_length":1,
                        "boost":2.0
                    }
                }
            }""", TEXT_FIELD_NAME);
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> parseQuery(queryMissingFuzzinessUpLimit).toQuery(createSearchExecutionContext())
        );
        String msg = "failed to find low and high distance values";
        assertTrue(e.getMessage() + " didn't contain: " + msg + " but: " + e.getMessage(), e.getMessage().contains(msg));

        String queryHavingNegativeFuzzinessLowLimit = Strings.format("""
            {
                "fuzzy":{
                    "%s":{
                        "value":"sh",
                        "fuzziness": "AUTO:-1,6",
                        "prefix_length":1,
                        "boost":2.0
                    }
                }
            }""", TEXT_FIELD_NAME);
        String msg2 = "fuzziness wrongly configured";
        ElasticsearchParseException e2 = expectThrows(
            ElasticsearchParseException.class,
            () -> parseQuery(queryHavingNegativeFuzzinessLowLimit).toQuery(createSearchExecutionContext())
        );
        assertTrue(e2.getMessage() + " didn't contain: " + msg2 + " but: " + e.getMessage(), e.getMessage().contains(msg));

        String queryMissingFuzzinessUpLimit2 = Strings.format("""
            {
                "fuzzy":{
                    "%s":{
                        "value":"sh",
                        "fuzziness": "AUTO:1,",
                        "prefix_length":1,
                        "boost":2.0
                    }
                }
            }""", TEXT_FIELD_NAME);
        e = expectThrows(
            ElasticsearchParseException.class,
            () -> parseQuery(queryMissingFuzzinessUpLimit2).toQuery(createSearchExecutionContext())
        );
        assertTrue(e.getMessage() + " didn't contain: " + msg + " but: " + e.getMessage(), e.getMessage().contains(msg));

        String queryMissingFuzzinessLowLimit = Strings.format("""
            {
                "fuzzy":{
                    "%s":{
                        "value":"sh",
                        "fuzziness": "AUTO:,5",
                        "prefix_length":1,
                        "boost":2.0
                    }
                }
            }""", TEXT_FIELD_NAME);
        e = expectThrows(
            ElasticsearchParseException.class,
            () -> parseQuery(queryMissingFuzzinessLowLimit).toQuery(createSearchExecutionContext())
        );
        msg = "failed to parse [AUTO:,5] as a \"auto:int,int\"";
        assertTrue(e.getMessage() + " didn't contain: " + msg + " but: " + e.getMessage(), e.getMessage().contains(msg));
    }

    public void testToQueryWithNumericField() throws IOException {
        String query = Strings.format("""
            {
                "fuzzy":{
                    "%s":{
                        "value":12,
                        "fuzziness":2
                    }
                }
            }
            """, INT_FIELD_NAME);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> parseQuery(query).toQuery(createSearchExecutionContext())
        );
        assertEquals(
            "Can only use fuzzy queries on keyword and text fields - not on [mapped_int] which is of type [integer]",
            e.getMessage()
        );
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "fuzzy" : {
                "user" : {
                  "value" : "ki",
                  "fuzziness" : "2",
                  "prefix_length" : 0,
                  "max_expansions" : 100,
                  "transpositions" : false,
                  "boost" : 42.0
                }
              }
            }""";
        FuzzyQueryBuilder parsed = (FuzzyQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, 42.0, parsed.boost(), 0.00001);
        assertEquals(json, 2, parsed.fuzziness().asFloat(), 0f);
        assertEquals(json, false, parsed.transpositions());
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json1 = """
            {
              "fuzzy" : {
                "message1" : {
                  "value" : "this is a test"
                }
              }
            }""";
        parseQuery(json1); // should be all good

        String json2 = """
            {
              "fuzzy" : {
                "message1" : {
                  "value" : "this is a test"
                },
                "message2" : {
                  "value" : "this is a test"
                }
              }
            }""";

        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json2));
        assertEquals("[fuzzy] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());

        String shortJson = """
            {
              "fuzzy" : {
                "message1" : "this is a test",
                "message2" : "value" : "this is a test"
              }
            }""";

        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[fuzzy] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());
    }

    public void testParseFailsWithValueArray() {
        String query = """
            {
              "fuzzy" : {
                "message1" : {
                  "value" : [ "one", "two", "three"]
                }
              }
            }""";

        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(query));
        assertEquals("[fuzzy] unexpected token [START_ARRAY] after [value]", e.getMessage());
    }

    public void testToQueryWithTranspositions() throws Exception {
        Query query = new FuzzyQueryBuilder(TEXT_FIELD_NAME, "text").toQuery(createSearchExecutionContext());
        assertThat(query, instanceOf(FuzzyQuery.class));
        assertEquals(FuzzyQuery.defaultTranspositions, ((FuzzyQuery) query).getTranspositions());

        query = new FuzzyQueryBuilder(TEXT_FIELD_NAME, "text").transpositions(true).toQuery(createSearchExecutionContext());
        assertThat(query, instanceOf(FuzzyQuery.class));
        assertEquals(true, ((FuzzyQuery) query).getTranspositions());

        query = new FuzzyQueryBuilder(TEXT_FIELD_NAME, "text").transpositions(false).toQuery(createSearchExecutionContext());
        assertThat(query, instanceOf(FuzzyQuery.class));
        assertEquals(false, ((FuzzyQuery) query).getTranspositions());
    }
}

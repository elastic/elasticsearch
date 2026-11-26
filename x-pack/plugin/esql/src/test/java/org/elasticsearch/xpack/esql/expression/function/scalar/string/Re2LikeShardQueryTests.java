/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.TrackingCircuitBreaker;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.search.cost.AutomatonQueryCostEstimator;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.Re2Pattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.Re2RegExp;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.Re2Like;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * The query a shard builds for a pushed down {@link Re2Like}: it applies the checks a {@code regexp} query applies, and
 * builds the automaton against the shard's circuit breaker.
 */
public class Re2LikeShardQueryTests extends MapperServiceTestCase {

    public void testRefusesPatternLongerThanTheIndexAllows() throws IOException {
        MapperService mapperService = createMapperService(
            Settings.builder().put(IndexSettings.MAX_REGEX_LENGTH_SETTING.getKey(), 5).build(),
            mapping(b -> b.startObject("label").field("type", "keyword").endObject())
        );
        SearchExecutionContext context = createSearchExecutionContext(mapperService);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query("abc.*def", context));
        assertThat(
            e.getMessage(),
            containsString("The length of regex [8] used in a label matcher has exceeded the allowed maximum of [5]")
        );
    }

    public void testRefusesWhenExpensiveQueriesAreNotAllowed() throws IOException {
        SearchExecutionContext context = noExpensiveQueries(keyword(true));
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> query("a.*", context));
        assertThat(e.getMessage(), equalTo("[regexp] queries cannot be executed when 'search.allow_expensive_queries' is set to false."));
    }

    /**
     * A field without a terms index refuses it too, as it refuses a {@code regexp} query, even one such as a doc-values-only
     * flattened key, which doesn't check the setting when given an automaton.
     */
    public void testDocValuesOnlyFieldRefusesWhenExpensiveQueriesAreNotAllowed() throws IOException {
        MapperService mapperService = createMapperService(
            mapping(b -> b.startObject("labels").field("type", "flattened").field("index", false).endObject())
        );
        assertThat(
            "the key builds the query when expensive ones are allowed",
            query("labels.pod", "a.*", createSearchExecutionContext(mapperService)),
            notNullValue()
        );
        SearchExecutionContext context = noExpensiveQueries(mapperService);
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> query("labels.pod", "a.*", context));
        assertThat(e.getMessage(), equalTo("[regexp] queries cannot be executed when 'search.allow_expensive_queries' is set to false."));
    }

    /** A constant field tests its one value, so it runs the match as it runs a {@code regexp} query, whatever the setting. */
    public void testConstantFieldRunsWhenExpensiveQueriesAreNotAllowed() throws IOException {
        SearchExecutionContext context = noExpensiveQueries(keyword(true));
        String index = context.getFullyQualifiedIndex().getName();
        assertThat(query("_index", "\\Q" + index + "\\E", context), equalTo(Queries.ALL_DOCS_INSTANCE));
        assertThat(query("_index", "\\Q" + index + "\\Ex", context), instanceOf(MatchNoDocsQuery.class));
    }

    public void testAutomatonIsChargedToTheShardBreaker() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(ByteSizeValue.ofMb(1).getBytes());
        SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(keyword(true)), breaker);

        assertThat(query("(a|b)c.*", context), notNullValue());
        assertTrue("the build reserves memory while it runs", breaker.peak() > 0);
        context.releaseQueryConstructionMemory();
        assertThat("nothing stays reserved once the request ends", breaker.getUsed(), equalTo(0L));

        // a program that fits the breaker, 100 copies of a class of hundreds of ranges, whose transitions don't: the
        // build is charged as it grows and stops when the breaker trips
        String regex = "\\pL{100}";
        TrackingCircuitBreaker small = new TrackingCircuitBreaker(ByteSizeValue.ofMb(1).getBytes());
        SearchExecutionContext limited = new SearchExecutionContext(createSearchExecutionContext(keyword(true)), small);
        expectThrows(CircuitBreakingException.class, () -> query(regex, limited));
        assertThat("past the program's reservation", small.peak(), greaterThan(100 * Re2RegExp.BYTES_PER_INSTRUCTION));
        assertThat(small.getUsed(), equalTo(0L));
    }

    /**
     * The field compiles the automaton while it builds the query, which for an indexed keyword expands it to UTF-8 bytes:
     * the peak of that is held on the breaker while it runs, as it is for a {@code regexp} query.
     */
    public void testCompilationIsChargedToTheShardBreaker() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(ByteSizeValue.ofMb(1).getBytes());
        SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(keyword(true)), breaker);

        assertThat(query("abc", context), notNullValue());
        assertThat(breaker.peak(), greaterThanOrEqualTo(new AutomatonQueryCostEstimator(0).estimate()));
        context.releaseQueryConstructionMemory();
        assertThat(breaker.getUsed(), equalTo(0L));

        TrackingCircuitBreaker small = new TrackingCircuitBreaker(new AutomatonQueryCostEstimator(0).estimate() - 1);
        SearchExecutionContext limited = new SearchExecutionContext(createSearchExecutionContext(keyword(true)), small);
        expectThrows(CircuitBreakingException.class, () -> query("abc", limited));
        assertThat(small.getUsed(), equalTo(0L));
    }

    /**
     * The query keeps its compiled automaton until the request ends, so it stays charged until then, as a {@code regexp}
     * query does, and is marked so that the walk over the built query, which a wrapping query builder runs as well, doesn't
     * charge it again.
     */
    public void testQueryStaysChargedUntilTheRequestEnds() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(ByteSizeValue.ofMb(1).getBytes());
        SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(keyword(true)), breaker);

        AutomatonQuery query = asInstanceOf(AutomatonQuery.class, query("(a|b)c.*", context));
        assertThat(breaker.getUsed(), equalTo(query.ramBytesUsed()));
        assertTrue(context.isQueryMemoryPreCharged(query));

        context.releaseQueryConstructionMemory();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** A regex Prometheus runs without an automaton can be too complex to build one for, as the compatibility docs say. */
    public void testRefusesRegexTooComplexToDeterminize() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext(keyword(true));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query(".*a.{20}", context));
        assertThat(e.getMessage(), equalTo("Pattern was too complex to determinize"));

        // built against the shard breaker, it fails as a regexp query's automaton does, which the search reports the same way
        SearchExecutionContext limited = new SearchExecutionContext(
            createSearchExecutionContext(keyword(true)),
            new TrackingCircuitBreaker(ByteSizeValue.ofMb(100).getBytes())
        );
        expectThrows(TooComplexToDeterminizeException.class, () -> query(".*a.{20}", limited));
    }

    private MapperService keyword(boolean indexed) throws IOException {
        return createMapperService(mapping(b -> b.startObject("label").field("type", "keyword").field("index", indexed).endObject()));
    }

    /**
     * A context whose {@code search.allow_expensive_queries} is false: the test framework's context allows them, and the
     * setting is read through this method.
     */
    private SearchExecutionContext noExpensiveQueries(MapperService mapperService) {
        return new SearchExecutionContext(createSearchExecutionContext(mapperService)) {
            @Override
            public boolean allowExpensiveQueries() {
                return false;
            }
        };
    }

    private static org.apache.lucene.search.Query query(String regex, SearchExecutionContext context) {
        return query("label", regex, context);
    }

    private static org.apache.lucene.search.Query query(String field, String regex, SearchExecutionContext context) {
        Re2Like re2Like = new Re2Like(Source.EMPTY, getFieldAttribute(field), new Re2Pattern(regex));
        MappedFieldType fieldType = context.getFieldType(field);
        return re2Like.asLuceneQuery(fieldType, MultiTermQuery.CONSTANT_SCORE_REWRITE, context);
    }
}

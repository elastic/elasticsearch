/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.search.MultiTermQuery;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.TrackingCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.Re2Pattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.Re2RegExp;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.Re2Like;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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

    /** A field without a terms index refuses it too, as it refuses a {@code regexp} query, through its own check. */
    public void testDocValuesOnlyFieldRefusesWhenExpensiveQueriesAreNotAllowed() throws IOException {
        SearchExecutionContext context = noExpensiveQueries(keyword(false));
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> query("a.*", context));
        assertThat(e.getMessage(), containsString("since it is not indexed and 'search.allow_expensive_queries' is set to false"));
    }

    public void testAutomatonIsChargedToTheShardBreaker() throws IOException {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(ByteSizeValue.ofMb(1).getBytes());
        SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(keyword(true)), breaker);

        assertThat(query("(a|b)c.*", context), notNullValue());
        assertTrue("the build reserves memory while it runs", breaker.peak() > 0);
        assertThat("nothing stays reserved once the automaton is built", breaker.getUsed(), equalTo(0L));

        // a program that fits the breaker, 100 copies of a class of hundreds of ranges, whose transitions don't: the
        // build is charged as it grows and stops when the breaker trips
        String regex = "\\pL{100}";
        TrackingCircuitBreaker small = new TrackingCircuitBreaker(ByteSizeValue.ofMb(1).getBytes());
        SearchExecutionContext limited = new SearchExecutionContext(createSearchExecutionContext(keyword(true)), small);
        expectThrows(CircuitBreakingException.class, () -> query(regex, limited));
        assertThat("past the program's reservation", small.peak(), greaterThan(100 * Re2RegExp.BYTES_PER_INSTRUCTION));
        assertThat(small.getUsed(), equalTo(0L));
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
        Re2Like re2Like = new Re2Like(Source.EMPTY, getFieldAttribute("label"), new Re2Pattern(regex));
        MappedFieldType fieldType = context.getFieldType("label");
        return re2Like.asLuceneQuery(fieldType, MultiTermQuery.CONSTANT_SCORE_REWRITE, context);
    }
}

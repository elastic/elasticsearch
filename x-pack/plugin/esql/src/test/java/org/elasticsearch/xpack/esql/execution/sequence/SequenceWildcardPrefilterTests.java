/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.SearchExecutionContextHelper;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPatternList;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToLower;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLikeList;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;

public class SequenceWildcardPrefilterTests extends MapperServiceTestCase {
    public void testRegexMetacharactersStayLiteral() {
        String pattern = "a.b+c[d](e){f}|g^h$-\"#&<>~@";
        var automaton = automaton(pattern);
        assertTrue(automaton.run(pattern));
        assertTrue(automaton.run("A.B+C[D](E){F}|G^H$-\"#&<>~@"));
        assertFalse(automaton.run("axb+c[d](e){f}|g^h$-\"#&<>~@"));
        assertFalse(automaton.run("a.b+c[d](e){f}xg^h$-\"#&<>~@"));
    }

    public void testWildcardEscapesAndAlternatives() {
        var automaton = automaton("star\\*end", "q\\?end", "slash\\\\end", "w?ld*");
        for (String value : List.of("STAR*end", "Q?end", "SLASH\\end", "WILDcard", "\u00e9")) {
            assertTrue(value, automaton.run(value));
        }
        for (String value : List.of("STARxend", "Qxend", "SLASHend", "WLDcard")) {
            assertFalse(value, automaton.run(value));
        }
    }

    public void testInvalidWildcardEscapesAndIneligibleAlternatives() {
        for (String pattern : List.of("bad\\q", "bad\\.", "trailing\\")) {
            expectThrows(InvalidArgumentException.class, () -> new WildcardPattern(pattern));
        }
        assertNull(prefilter("valid*", "Uppercase"));
        assertNull(prefilter("valid*", "\u00e9"));
    }

    public void testRegexpUsesTheTranslatedFieldName() {
        var field = new FieldAttribute(Source.EMPTY, null, "source", "value", new EsField("value", DataType.KEYWORD, Map.of(), true, NONE));
        var candidate = prefilter(field, "node");
        assertNotNull(candidate);
        assertNotEquals(field.name(), field.qualifiedName());
        assertEquals(field.qualifiedName(), candidate.fieldName());
    }

    public void testAutomatonWorkLimitPreservesTheKeyPredicate() throws Exception {
        // Lowercased literals from Temporarily Scheduled Task Creation. Its complete exclusion list exceeds
        // the descriptor cap even in a positive conjunct. A smaller group reaches shard compilation.
        String[] exclusions = {
            "\\\\softlanding\\\\*",
            "*prtg*",
            "\\\\microsoftedgemaintenance*",
            "*\\\\microsoft\\\\windows\\\\devicelicensingservice\\\\*",
            "\\\\onedrive standalone update*",
            "\\\\powertoys\\\\*",
            "*hewlett-packard*",
            "*\\\\hp\\\\hp support assistant\\\\*",
            "*\\\\mozilla\\\\*",
            "\\\\amdlinkupdate",
            "\\\\amdinstalllauncher",
            "*\\\\googleuserpeh\\\\*",
            "\\\\createexplorershellunelevatedtask",
            "*\\\\runaadchtestconnectivityassystem",
            "*wavebrowser*",
            "\\\\ccleanercrashreporting",
            "\\\\tvinstallrestore" };
        assertNull(prefilter(exclusions));
        var subset = prefilter(Arrays.copyOf(exclusions, 6));
        assertNotNull(subset);
        subset.maxDeterminizedStates(1);
        // Also exercise the default Lucene work limit with a short eligible ES|QL wildcard.
        var suffix = prefilter("*a" + "?".repeat(20));
        assertNotNull(suffix);
        var breaker = requestBreaker();
        var context = new SearchExecutionContext(mapping(), breaker);
        try {
            for (var pattern : List.of(subset, suffix)) {
                expectThrows(TooComplexToDeterminizeException.class, () -> pattern.toQuery(context));
                assertEquals(0, breaker.getUsed());
                var keyOnly = envelope(context, null).toQuery(context);
                assertEquals(keyOnly, envelope(context, pattern).toQuery(context));
                context.releaseQueryConstructionMemory();
                assertEquals(0, breaker.getUsed());
            }
        } finally {
            context.releaseQueryConstructionMemory();
        }
    }

    public void testExpensiveQueriesChangesBetweenRegexpAdmissions() throws Exception {
        var mapper = wildcardMapper();
        var reads = new AtomicInteger();
        // The copy used for mapper compilation inherits this supplier. The setting flips immediately after the first read.
        var context = new SearchExecutionContext(
            0,
            0,
            mapper.getIndexSettings(),
            null,
            null,
            mapper,
            mapper.mappingLookup(),
            null,
            null,
            parserConfig(),
            writableRegistry(),
            null,
            null,
            () -> 0L,
            null,
            null,
            () -> reads.getAndIncrement() == 0,
            null,
            Map.of(),
            null,
            MapperMetrics.NOOP,
            SearchExecutionContextHelper.SHARD_SEARCH_STATS
        );
        var keyOnly = envelope(context, null).toQuery(context);
        var both = envelope(context, prefilter("node"));
        assertNotEquals(keyOnly, both.toQuery(context));
        assertEquals(1, reads.get());
        assertEquals(keyOnly, both.toQuery(context));
        assertEquals(2, reads.get());
    }

    public void testClauseLimitKeepsKeyAndDropsOnlyRegexp() throws Exception {
        var context = mapping();
        var wildcard = prefilter("node");
        var keyOnly = envelope(context, null).toQuery(context);
        var limited = new MaxClauseCountQueryVisitor(2);
        assertEquals(keyOnly, envelope(context, wildcard).toQuery(context, limited));
        assertEquals(2, limited.getNumClauses());
        var sufficient = new MaxClauseCountQueryVisitor(3);
        assertNotEquals(keyOnly, envelope(context, wildcard).toQuery(context, sufficient));
        assertEquals(3, sufficient.getNumClauses());
    }

    public void testRegexpBreakerFallbackKeepsKeyAndBalancesAccounting() throws Exception {
        var breaker = requestBreaker();
        var context = new SearchExecutionContext(mapping(), breaker);
        var wildcard = prefilter("node");
        try {
            var keyOnly = envelope(context, null).toQuery(context);
            long keyOnlyBytes = breaker.getUsed();
            assertTrue(keyOnlyBytes > 0);
            context.releaseQueryConstructionMemory();
            assertEquals(0, breaker.getUsed());
            assertNotEquals(keyOnly, envelope(context, wildcard).toQuery(context));
            context.releaseQueryConstructionMemory();
            assertEquals(0, breaker.getUsed());
            assertEquals(0, breaker.getTrippedCount());

            breaker.setLimitAndOverhead(keyOnlyBytes, 1.0);
            assertEquals(keyOnly, envelope(context, wildcard).toQuery(context));
            assertEquals(1, breaker.getTrippedCount());
            assertEquals(keyOnlyBytes, breaker.getUsed());
            assertEquals(keyOnlyBytes, context.getQueryConstructionMemoryUsed());
        } finally {
            context.releaseQueryConstructionMemory();
        }
        assertEquals(0, breaker.getUsed());
    }

    private SearchExecutionContext mapping() throws IOException {
        return createSearchExecutionContext(wildcardMapper());
    }

    private MapperService wildcardMapper() throws IOException {
        return createMapperService("""
            {"_doc":{"properties":{"phase":{"type":"keyword"},"key":{"type":"keyword"},"value":{"type":"keyword"}}}}
            """);
    }

    private static SequenceSourceQueryBuilder envelope(SearchExecutionContext context, RegexpQueryBuilder wildcard) {
        var query = SequenceSourceQueryBuilder.withOptionalFilters(
            new TermQueryBuilder("phase", "end"),
            new TermQueryBuilder("key", "k"),
            wildcard
        );
        return new SequenceSourceQueryBuilder(query, List.of(context.index().getName()));
    }

    private static CircuitBreaker requestBreaker() {
        var settings = Settings.builder()
            .put("indices.breaker.total.use_real_memory", false)
            .put("indices.breaker.request.limit", "256mb")
            .put("indices.breaker.request.overhead", 1.0)
            .build();
        return new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            List.of(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ).getBreaker(CircuitBreaker.REQUEST);
    }

    private static CharacterRunAutomaton automaton(String... patterns) {
        var prefilter = prefilter(patterns);
        assertNotNull(prefilter);
        var compiled = new RegExp(prefilter.value(), RegExp.NONE).toAutomaton();
        return new CharacterRunAutomaton(Operations.determinize(compiled, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT));
    }

    private static RegexpQueryBuilder prefilter(String... patterns) {
        var value = new FieldAttribute(Source.EMPTY, "value", new EsField("value", DataType.KEYWORD, Map.of(), true, NONE));
        return prefilter(value, patterns);
    }

    private static RegexpQueryBuilder prefilter(FieldAttribute value, String... patterns) {
        var list = new WildcardPatternList(Arrays.stream(patterns).map(WildcardPattern::new).toList());
        var expression = new WildcardLikeList(Source.EMPTY, new ToLower(Source.EMPTY, value, TEST_CFG), list, false);
        return SequenceWildcardPrefilter.fromConjunct(expression);
    }
}

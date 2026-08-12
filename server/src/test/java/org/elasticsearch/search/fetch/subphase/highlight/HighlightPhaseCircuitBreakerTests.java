/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.HighlighterTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class HighlightPhaseCircuitBreakerTests extends HighlighterTestCase {

    public void testFuzzyQueryChargesAndCloseRefundsBreaker() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "field" : { "type" : "text" }
            }}}
            """);
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "field" : "this is some fuzzy text content" }
            """));
        SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.fuzzyQuery("field", "fuzzz").fuzziness(Fuzziness.TWO))
            .highlighter(new HighlightBuilder().field("field"));

        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(100));
        runHighlightProcessor(mapperService, doc, search, breaker, processorAndHit -> {
            long baseline = breaker.getUsed();
            processorAndHit.process();
            assertThat("highlighting a fuzzy query must charge the breaker", breaker.getUsed() - baseline, greaterThan(0L));
            processorAndHit.processor().close();
            assertEquals("close() must refund exactly what highlighting charged", baseline, breaker.getUsed());
        });
    }

    public void testTermQueryChargesNothing() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "field" : { "type" : "text" }
            }}}
            """);
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "field" : "this is some text" }
            """));
        SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "some"))
            .highlighter(new HighlightBuilder().field("field"));

        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(100));
        runHighlightProcessor(mapperService, doc, search, breaker, processorAndHit -> {
            long baseline = breaker.getUsed();
            processorAndHit.process();
            assertEquals("a term query rebuilds no automata, so highlighting should add no charge", baseline, breaker.getUsed());
            processorAndHit.processor().close();
            assertEquals(baseline, breaker.getUsed());
        });
    }

    public void testTwoHighlightedFieldsAccumulateAndSingleCloseRefundsAll() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "field1" : { "type" : "text" },
                "field2" : { "type" : "text" }
            }}}
            """);
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "field1" : "some fuzzy text", "field2" : "other fuzzy content" }
            """));
        SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.fuzzyQuery("field1", "fuzzz").fuzziness(Fuzziness.TWO))
            .highlighter(new HighlightBuilder().field("field1").field("field2"));

        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(100));
        runHighlightProcessor(mapperService, doc, search, breaker, processorAndHit -> {
            long baseline = breaker.getUsed();
            processorAndHit.process();
            long chargedForBothFields = breaker.getUsed() - baseline;
            assertThat(chargedForBothFields, greaterThan(0L));

            processorAndHit.processor().close();
            assertEquals("closing once must refund the accumulated charge for every field", baseline, breaker.getUsed());
        });
    }

    public void testCrossFieldQueryDoesNotChargeForUnhighlightedField() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "field_a" : { "type" : "text" },
                "field_b" : { "type" : "text" }
            }}}
            """);
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "field_a" : "some fuzzy text", "field_b" : "other fuzzy content" }
            """));

        long chargeHighlightingOnlyFieldA = chargeFor(
            mapperService,
            doc,
            new SearchSourceBuilder().query(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.fuzzyQuery("field_a", "fuzzz").fuzziness(Fuzziness.TWO))
                    .should(QueryBuilders.fuzzyQuery("field_b", "fuzzz").fuzziness(Fuzziness.TWO))
            ).highlighter(new HighlightBuilder().field("field_a"))
        );
        long chargeHighlightingFieldAAlone = chargeFor(
            mapperService,
            doc,
            new SearchSourceBuilder().query(QueryBuilders.fuzzyQuery("field_a", "fuzzz").fuzziness(Fuzziness.TWO))
                .highlighter(new HighlightBuilder().field("field_a"))
        );

        assertThat(chargeHighlightingFieldAAlone, greaterThan(0L));
        assertEquals(
            "field_b's fuzzy clause must not be charged when only field_a is highlighted",
            chargeHighlightingFieldAAlone,
            chargeHighlightingOnlyFieldA
        );
    }

    public void testDoubleCloseIsIdempotent() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "field" : { "type" : "text" }
            }}}
            """);
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "field" : "this is some fuzzy text content" }
            """));
        SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.fuzzyQuery("field", "fuzzz").fuzziness(Fuzziness.TWO))
            .highlighter(new HighlightBuilder().field("field"));

        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(100));
        runHighlightProcessor(mapperService, doc, search, breaker, processorAndHit -> {
            long baseline = breaker.getUsed();
            processorAndHit.process();
            assertThat(breaker.getUsed() - baseline, greaterThan(0L));

            processorAndHit.processor().close();
            assertEquals(baseline, breaker.getUsed());

            processorAndHit.processor().close();
            assertEquals(baseline, breaker.getUsed());
        });
    }

    public void testMatchedFieldsDoNotInflateChargeWhenQueryDoesNotTargetThem() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "field" : { "type" : "text", "fields" : { "exact" : { "type" : "text" } } }
            }}}
            """);
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "field" : "this is some fuzzy text content" }
            """));

        long unmatchedCharge = chargeFor(
            mapperService,
            doc,
            new SearchSourceBuilder().query(QueryBuilders.fuzzyQuery("field", "fuzzz").fuzziness(Fuzziness.TWO))
                .highlighter(new HighlightBuilder().field("field"))
        );
        long matchedCharge = chargeFor(
            mapperService,
            doc,
            new SearchSourceBuilder().query(QueryBuilders.fuzzyQuery("field", "fuzzz").fuzziness(Fuzziness.TWO))
                .highlighter(new HighlightBuilder().field(new HighlightBuilder.Field("field").matchedFields("field.exact")))
        );

        assertThat(unmatchedCharge, greaterThan(0L));
        assertEquals(
            "matched_fields must not inflate the charge when the query has no clause on the matched field",
            unmatchedCharge,
            matchedCharge
        );
    }

    public void testMustNotFuzzyClauseChargesNothing() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "field" : { "type" : "text" }
            }}}
            """);
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "field" : "this is some fuzzy text content" }
            """));
        SearchSourceBuilder search = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.matchAllQuery())
                .mustNot(QueryBuilders.fuzzyQuery("field", "fuzzz").fuzziness(Fuzziness.TWO))
        ).highlighter(new HighlightBuilder().field("field"));

        long charged = chargeFor(mapperService, doc, search);
        assertEquals("a fuzzy clause under must_not is never visited by UnifiedHighlighter, so it must not be charged", 0L, charged);
    }

    public void testExistsFilterOnHighlightedFieldSuppressesFuzzyCharge() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "field" : { "type" : "text" }
            }}}
            """);
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "field" : "this is some fuzzy text content" }
            """));
        SearchSourceBuilder search = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.fuzzyQuery("field", "fuzzz").fuzziness(Fuzziness.TWO))
                .filter(QueryBuilders.existsQuery("field"))
        ).highlighter(new HighlightBuilder().field("field"));

        long charged = chargeFor(mapperService, doc, search);
        assertEquals("an exists clause on the highlighted field makes UnifiedHighlighter skip automata extraction entirely", 0L, charged);
    }

    public void testMatchedFieldsChargeSumsClausesActuallyTargetingEachField() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "field" : { "type" : "text", "fields" : { "exact" : { "type" : "text" } } }
            }}}
            """);
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "field" : "this is some fuzzy text content" }
            """));

        long unmatchedCharge = chargeFor(
            mapperService,
            doc,
            new SearchSourceBuilder().query(QueryBuilders.fuzzyQuery("field", "fuzzz").fuzziness(Fuzziness.TWO))
                .highlighter(new HighlightBuilder().field("field"))
        );

        long combinedCharge = chargeFor(
            mapperService,
            doc,
            new SearchSourceBuilder().query(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.fuzzyQuery("field", "fuzzz").fuzziness(Fuzziness.TWO))
                    .should(QueryBuilders.fuzzyQuery("field.exact", "fuzzz").fuzziness(Fuzziness.TWO))
            ).highlighter(new HighlightBuilder().field(new HighlightBuilder.Field("field").matchedFields("field.exact")))
        );

        assertThat(unmatchedCharge, greaterThan(0L));
        assertThat(combinedCharge, equalTo(unmatchedCharge * 2));
    }

    public void testMatchedFieldsChargeDoublesWhenHighlightedFieldListsItself() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "field" : { "type" : "text", "fields" : { "exact" : { "type" : "text" } } }
            }}}
            """);
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "field" : "this is some fuzzy text content" }
            """));

        long unmatchedCharge = chargeFor(
            mapperService,
            doc,
            new SearchSourceBuilder().query(QueryBuilders.fuzzyQuery("field", "fuzzz").fuzziness(Fuzziness.TWO))
                .highlighter(new HighlightBuilder().field("field"))
        );

        long selfMatchedCharge = chargeFor(
            mapperService,
            doc,
            new SearchSourceBuilder().query(QueryBuilders.fuzzyQuery("field", "fuzzz").fuzziness(Fuzziness.TWO))
                .highlighter(new HighlightBuilder().field(new HighlightBuilder.Field("field").matchedFields("field", "field.exact")))
        );

        assertThat(unmatchedCharge, greaterThan(0L));
        assertThat(selfMatchedCharge, equalTo(unmatchedCharge * 2));
    }

    private long chargeFor(MapperService mapperService, ParsedDocument doc, SearchSourceBuilder search) throws IOException {
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(100));
        long[] charged = new long[1];
        runHighlightProcessor(mapperService, doc, search, breaker, processorAndHit -> {
            long baseline = breaker.getUsed();
            processorAndHit.process();
            charged[0] = breaker.getUsed() - baseline;
            processorAndHit.processor().close();
        });
        return charged[0];
    }
}

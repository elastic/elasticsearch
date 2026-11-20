/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.Build;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.test.ListMatcher;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Heap-attack tests that fetch or aggregate big text, multi-valued and histogram fields.
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class HeapAttackFieldsIT extends HeapAttackTestCase {

    public void testFetchManyBigFields() throws IOException {
        initManyBigFieldsIndex(100, "keyword", false, 1000);
        Map<?, ?> response = fetchManyBigFields(100);
        ListMatcher columns = matchesList();
        for (int f = 0; f < 1000; f++) {
            columns = columns.item(matchesMap().entry("name", "f" + String.format(Locale.ROOT, "%03d", f)).entry("type", "keyword"));
        }
        assertMap(response, matchesMap().entry("columns", columns));
    }

    public void testFetchTooManyBigFields() throws IOException {
        initManyBigFieldsIndex(500, "keyword", false, 1000);
        // 500 docs is plenty to circuit break on most nodes
        assertCircuitBreaks(attempt -> fetchManyBigFields(attempt * 500));
    }

    public void testAggManyBigTextFields() throws IOException {
        int docs = 100;
        int fields = 100;
        initManyBigFieldsIndex(docs, "text", false, 1000);
        Map<?, ?> response = aggManyBigFields(fields);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "sum").entry("type", "long"));
        assertMap(
            response,
            matchesMap().entry("columns", columns).entry("values", matchesList().item(matchesList().item(1024 * fields * docs)))
        );
    }

    /**
     * Aggregates on the {@code LENGTH} of a giant text field. Without
     * splitting pages on load (#131053) this throws a {@link CircuitBreakingException}
     * when it tries to load a giant field. With that change it finishes
     * after loading many single-row pages.
     */
    public void testAggGiantTextField() throws IOException {
        int docs = 100;
        initGiantTextField(docs, false, 5);
        Map<?, ?> response = aggGiantTextField();
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "sum").entry("type", "long"));
        assertMap(
            response,
            matchesMap().entry("columns", columns).entry("values", matchesList().item(matchesList().item(1024 * 1024 * 5 * docs)))
        );
    }

    public void testAggMvLongs() throws IOException {
        int fieldValues = 100;
        initMvLongsIndex(1, 3, fieldValues, false);
        Map<?, ?> response = aggMvLongs(3);
        ListMatcher columns = matchesList().item(matchesMap().entry("name", "MAX(f00)").entry("type", "long"))
            .item(matchesMap().entry("name", "f00").entry("type", "long"))
            .item(matchesMap().entry("name", "f01").entry("type", "long"))
            .item(matchesMap().entry("name", "f02").entry("type", "long"));
        assertMap(response, matchesMap().entry("columns", columns));
    }

    public void testAggTooManyMvLongs() throws IOException {
        initMvLongsIndex(1, 3, 1000, false);
        // 3 fields is plenty on most nodes
        assertCircuitBreaks(attempt -> aggMvLongs(attempt * 3));
    }

    public void testFetchMvLongs() throws IOException {
        int fields = 100;
        initMvLongsIndex(100, fields, 1000, false);
        Map<?, ?> response = fetchMvLongs(100);
        ListMatcher columns = matchesList();
        for (int f = 0; f < fields; f++) {
            columns = columns.item(matchesMap().entry("name", String.format(Locale.ROOT, "f%02d", f)).entry("type", "long"));
        }
        assertMap(response, matchesMap().entry("columns", columns));
    }

    public void testFetchTooManyMvLongs() throws IOException {
        int docs = scaleToHeap(500);
        initMvLongsIndex(docs, 100, 1000, false);
        assertCircuitBreaks(attempt -> fetchMvLongs(docs));
    }

    public void testManyExponentialHistograms() throws IOException {
        initManyExponentialHistograms(10_000, 100);

        // Run a successful query first as sanity check
        queryAndVerifyDuplicatedHistograms("many_exponential_histograms", "histo", 1);

        // and now blow up the memory
        assertCircuitBreaks(attempt -> queryDuplicatedHistograms("many_exponential_histograms", "histo", attempt * 10));
    }

    public void testManyTDigests() throws IOException {
        initManyTDigests(10_000, 100, TDigestFieldType.TDIGEST);

        // Run a successful query first as sanity check
        queryAndVerifyDuplicatedHistograms("many_tdigests", "histo", 1);

        // and now blow up the memory
        assertCircuitBreaks(attempt -> queryDuplicatedHistograms("many_tdigests", "histo", attempt * 10));
    }

    public void testManyHistograms() throws IOException {
        initManyTDigests(10_000, 100, TDigestFieldType.HISTOGRAM);

        // Run a successful query first as sanity check
        queryAndVerifyDuplicatedHistograms("many_tdigests", "TO_TDIGEST(histo)", 1);

        // and now blow up the memory
        assertCircuitBreaks(attempt -> queryDuplicatedHistograms("many_tdigests", "TO_TDIGEST(histo)", attempt * 10));
    }

    public void testStreamingApiAvoidsCircuitBreak() throws IOException {
        assumeTrue("ES|QL streaming is not available in release builds yet", Build.current().isSnapshot());
        int docs = 256;
        String esqlQuery = "FROM bigtext | KEEP f";
        initGiantTextField(docs, false, 1);
        try {
            setRequestBreakerLimit("20%");
            assertCircuitBreaks(attempt -> fetchBigText(esqlQuery));

            var s = streamQuery(esqlQuery, 1);
            assertThat("streaming must not surface an error", s.errors(), empty());
            assertThat(s.columns(), hasSize(1));
            assertMap(s.columns().get(0), matchesMap().entry("name", "f").entry("type", "text"));
            assertThat(s.rowCount(), equalTo((long) docs));
            assertMap(s.footer(), matchesMap().extraOk().entry("is_partial", false));
            assertFalse("footer must not contain an error key", s.footer().containsKey("error"));
        } finally {
            setRequestBreakerLimit(null);
        }
    }
}

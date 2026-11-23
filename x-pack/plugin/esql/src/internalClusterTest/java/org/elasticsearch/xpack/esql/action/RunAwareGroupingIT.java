/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSortConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;

/**
 * {@code STATS ... BY} an int key whose equal values are adjacent, like the field an index is sorted on: the hash finds the runs of
 * equal keys and the aggregators combine a run at a time. That is only about speed, so an index sorted on the key and an identical
 * one whose documents arrive in no order, which hashes row by row, must answer every query the same way, and the profile must show
 * which path ran.
 */
public class RunAwareGroupingIT extends AbstractEsqlIntegTestCase {

    private static final String QUERY_TEMPLATE = "FROM %s | STATS c = COUNT(*), cv = COUNT(v), s = SUM(v), a = AVG(v) BY k | SORT k";

    public void testSortedAndUnsortedIndexAgree() {
        createIndices();
        final int docs = between(500, 3000);
        final List<Object[]> sources = new ArrayList<>();
        int key = 0;
        for (int doc = 0; doc < docs; doc++) {
            if (randomInt(30) == 0) {
                key += between(1, 4);
            }
            // Large values, so the sum of a long run leaves the int range.
            sources.add(
                new Object[] { key, randomBoolean() ? randomIntBetween(Integer.MAX_VALUE - 1000, Integer.MAX_VALUE) : randomInt() }
            );
        }
        index("sorted", sources);
        index("plain", shuffled(sources));

        final List<List<Object>> expected = runQuery("plain");
        assertThat(expected.isEmpty(), equalTo(false));
        assertThat(runQuery("sorted"), equalTo(expected));
        assertThat(operatorsOf("sorted"), hasItem(matchesPattern(".*runPages=[1-9][0-9]*, rowByRowPages=[0-9]+.*")));
    }

    /** Keys that are all different have no run to combine: the page is hashed row by row, and the profile says so. */
    public void testUniqueKeysFallBack() {
        createIndices();
        final int docs = between(200, 1000);
        final List<Object[]> sources = new ArrayList<>();
        for (int doc = 0; doc < docs; doc++) {
            sources.add(new Object[] { doc, randomInt(1000) });
        }
        index("sorted", sources);
        index("plain", shuffled(sources));

        final List<List<Object>> expected = runQuery("plain");
        assertThat(runQuery("sorted"), equalTo(expected));
        assertThat(operatorsOf("sorted"), hasItem(matchesPattern(".*runPages=0, rowByRowPages=[1-9][0-9]*.*")));
    }

    /** Only an int key looks for runs so far: a long key on a sorted index is hashed as usual and still agrees. */
    public void testLongKeyOnASortedIndexIsHashedAsUsual() {
        createIndices();
        final int docs = between(200, 1000);
        final List<Object[]> sources = new ArrayList<>();
        int key = 0;
        for (int doc = 0; doc < docs; doc++) {
            if (randomInt(10) == 0) {
                key += between(1, 3);
            }
            sources.add(new Object[] { key, randomInt(1000) });
        }
        index("sorted_long", sources);
        index("plain", shuffled(sources));

        assertThat(withLongKeys(runQuery("sorted_long")), equalTo(withLongKeys(runQuery("plain"))));
        assertThat(operatorsOf("sorted_long"), not(hasItem(containsString("runPages"))));
    }

    /** The key is the last column; an int index and a long one answer with it as different number types. */
    private static List<List<Object>> withLongKeys(List<List<Object>> rows) {
        return rows.stream().map(row -> {
            final List<Object> copy = new ArrayList<>(row);
            final int last = copy.size() - 1;
            copy.set(last, ((Number) copy.get(last)).longValue());
            return copy;
        }).toList();
    }

    /** A key that goes missing has no run, and a missing value has no sum: both take the other paths and still agree. */
    public void testMissingKeysAndValues() {
        createIndices();
        final int docs = between(200, 1000);
        final List<Object[]> sources = new ArrayList<>();
        int key = 0;
        for (int doc = 0; doc < docs; doc++) {
            if (randomInt(20) == 0) {
                key += between(1, 3);
            }
            sources.add(new Object[] { randomInt(15) == 0 ? null : key, randomInt(5) == 0 ? null : randomInt(1000) });
        }
        index("sorted", sources);
        index("plain", shuffled(sources));

        assertThat(runQuery("sorted"), equalTo(runQuery("plain")));
    }

    private List<List<Object>> runQuery(String index) {
        try (
            EsqlQueryResponse response = run(
                EsqlQueryRequest.syncEsqlQueryRequest(String.format(java.util.Locale.ROOT, QUERY_TEMPLATE, index))
            )
        ) {
            return getValuesList(response).stream().map(row -> (List<Object>) new ArrayList<>(row)).toList();
        }
    }

    private static List<Object[]> shuffled(List<Object[]> sources) {
        final List<Object[]> copy = new ArrayList<>(sources);
        Collections.shuffle(copy, random());
        return copy;
    }

    private List<String> operatorsOf(String index) {
        final EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(String.format(java.util.Locale.ROOT, QUERY_TEMPLATE, index));
        request.profile(true);
        try (EsqlQueryResponse response = run(request)) {
            return operatorsOf(response);
        }
    }

    private static List<String> operatorsOf(EsqlQueryResponse response) {
        return response.profile()
            .drivers()
            .stream()
            .flatMap(driver -> driver.operators().stream())
            .map(operator -> operator.operator())
            .collect(Collectors.toList());
    }

    private void createIndices() {
        assertAcked(
            prepareCreate("sorted").setSettings(
                Settings.builder()
                    .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "k")
                    .put(IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey(), "asc")
                    .put("index.number_of_shards", between(1, 2))
            ).setMapping("k", "type=integer", "v", "type=integer")
        );
        assertAcked(
            prepareCreate("sorted_long").setSettings(
                Settings.builder()
                    .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "k")
                    .put(IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey(), "asc")
                    .put("index.number_of_shards", between(1, 2))
            ).setMapping("k", "type=long", "v", "type=integer")
        );
        assertAcked(
            prepareCreate("plain").setSettings(Settings.builder().put("index.number_of_shards", between(1, 2)))
                .setMapping("k", "type=integer", "v", "type=integer")
        );
    }

    private void index(String index, List<Object[]> sources) {
        for (Object[] source : sources) {
            final var builder = prepareIndex(index);
            final List<Object> fields = new ArrayList<>();
            if (source[0] != null) {
                fields.add("k");
                fields.add(source[0]);
            }
            if (source[1] != null) {
                fields.add("v");
                fields.add(source[1]);
            }
            builder.setSource(fields.toArray()).get();
        }
        indicesAdmin().prepareRefresh(index).get();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.cardinality;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Read-path coverage for the implicit flattened {@code _unmapped} sink: unmapped fields absorbed on a strict columnar index stay queryable
 * by their full dotted path (and bare dotless name). The write path is covered by {@link FlattenedUnmappedFieldsTests}; here we index
 * absorbed fields and assert that term/match/exists/agg/sort all resolve and behave like ordinary keyed flattened queries.
 */
public class FlattenedUnmappedFieldsSearchTests extends ESSingleNodeTestCase {

    @Before
    public void setUpIndex() {
        assumeTrue("requires the flattened_unmapped_fields feature flag", FlattenedFieldMapper.UNMAPPED_FIELDS_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexSettings.FLATTENED_UNMAPPED_FIELDS_ENABLED.getKey(), true)
            .build();
        // Empty mapping: every field in a document is unmapped and therefore absorbed into the sink.
        createIndex("test", settings);
    }

    private void index(String id, Object... source) {
        prepareIndex("test").setId(id).setRefreshPolicy(RefreshPolicy.IMMEDIATE).setSource(source).get();
    }

    public void testTermQueryOnAbsorbedNames() {
        String bareValue = randomAlphanumericOfLength(8);
        String nestedValue = randomAlphanumericOfLength(8);
        index("1", "bare", bareValue, "outer.inner.leaf", nestedValue);

        // Bare dotless absorbed name.
        assertHitCount(client().prepareSearch("test").setQuery(termQuery("bare", bareValue)), 1L);
        // Full dotted path of a nested absorbed object.
        assertHitCount(client().prepareSearch("test").setQuery(termQuery("outer.inner.leaf", nestedValue)), 1L);
        // A name that was never indexed still resolves (catch-all) but matches nothing rather than erroring.
        assertHitCount(client().prepareSearch("test").setQuery(termQuery("never.indexed", randomAlphanumericOfLength(5))), 0L);
    }

    public void testExistsQueryOnAbsorbedNames() {
        String bareValue = randomAlphanumericOfLength(8);
        String nestedValue = randomAlphanumericOfLength(8);
        index("1", "bare", bareValue, "outer.inner.leaf", nestedValue);

        // A second document with a disjoint absorbed field, so exists must discriminate between keys rather than matching every doc.
        index("2", "other", randomAlphanumericOfLength(8));

        assertHitCount(client().prepareSearch("test").setQuery(existsQuery("bare")), 1L);
        assertHitCount(client().prepareSearch("test").setQuery(existsQuery("outer.inner.leaf")), 1L);
        assertHitCount(client().prepareSearch("test").setQuery(existsQuery("other")), 1L);
        // Never indexed: resolves through the sink but no document has a value under that key.
        assertHitCount(client().prepareSearch("test").setQuery(existsQuery(randomAlphanumericOfLength(10))), 0L);
    }

    public void testMatchQueryOnAbsorbedName() {
        String value = randomAlphanumericOfLength(8);
        index("1", "bare", value);

        assertHitCount(client().prepareSearch("test").setQuery(matchQuery("bare", value)), 1L);
        assertHitCount(client().prepareSearch("test").setQuery(matchQuery("bare", randomAlphanumericOfLength(9))), 0L);
    }

    public void testTermsAggregationOnAbsorbedName() {
        for (int i = 0; i < 5; i++) {
            index(Integer.toString(i), "color", "red");
        }
        index("blue", "color", "blue");

        assertNoFailuresAndResponse(client().prepareSearch("test").addAggregation(terms("t").field("color")), response -> {
            Terms t = response.getAggregations().get("t");
            assertThat(t, notNullValue());
            assertThat(t.getBuckets().size(), equalTo(2));
            assertThat(t.getBuckets().get(0).getKey(), equalTo("red"));
            assertThat(t.getBuckets().get(0).getDocCount(), equalTo(5L));
        });
    }

    public void testCardinalityAggregationOnAbsorbedName() {
        int numDocs = randomIntBetween(2, 20);
        for (int i = 0; i < numDocs; i++) {
            index(Integer.toString(i), "metric", i);
        }

        assertNoFailuresAndResponse(client().prepareSearch("test").addAggregation(cardinality("c").field("metric")), response -> {
            Cardinality c = response.getAggregations().get("c");
            assertThat(c.getValue(), equalTo((long) numDocs));
        });
    }

    public void testFieldSortOnAbsorbedName() {
        index("1", "k", "A");
        index("2", "k", "B");
        index("3", "k", "C");

        assertNoFailuresAndResponse(client().prepareSearch("test").addSort("k", SortOrder.DESC), response -> {
            assertHitCount(response, 3);
            assertOrderedSearchHits(response, "3", "2", "1");
        });
    }

    // Sorting on a never-indexed name used to fail with "No mapping found for [...] in order to sort on"; every name now resolves through
    // the sink and sorts as all-missing, so every document is still returned. This is the widest consequence of the routing fallback.
    public void testSortOnNeverIndexedNameResolvesInsteadOfFailing() {
        index("1", "k", randomAlphanumericOfLength(8));
        index("2", "k", randomAlphanumericOfLength(8));

        String neverIndexed = randomAlphanumericOfLength(10);
        assertNoFailuresAndResponse(
            client().prepareSearch("test").addSort(neverIndexed, randomFrom(SortOrder.values())),
            response -> assertHitCount(response, 2)
        );
    }

}

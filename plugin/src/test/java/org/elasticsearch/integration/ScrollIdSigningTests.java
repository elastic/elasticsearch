/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.security.crypto.CryptoService;

import java.util.Locale;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationException;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ScrollIdSigningTests extends SecurityIntegTestCase {
    public void testSearchAndClearScroll() throws Exception {
        IndexRequestBuilder[] docs = new IndexRequestBuilder[randomIntBetween(20, 100)];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = client().prepareIndex("idx", "type").setSource("field", "value");
        }
        indexRandom(true, docs);
        SearchResponse response = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setScroll(TimeValue.timeValueMinutes(2))
                .setSize(randomIntBetween(1, 10)).get();

        int hits = 0;
        try {
            while (true) {
                assertSigned(response.getScrollId());
                assertHitCount(response, docs.length);
                hits += response.getHits().getHits().length;
                response = client().prepareSearchScroll(response.getScrollId())
                        .setScroll(TimeValue.timeValueMinutes(2)).get();
                if (response.getHits().getHits().length == 0) {
                    break;
                }
            }
            assertThat(hits, equalTo(docs.length));
        } finally {
            clearScroll(response.getScrollId());
        }
    }

    public void testSearchScrollWithTamperedScrollId() throws Exception {
        IndexRequestBuilder[] docs = new IndexRequestBuilder[randomIntBetween(20, 100)];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = client().prepareIndex("idx", "type").setSource("field", "value");
        }
        indexRandom(true, docs);
        SearchResponse response = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setScroll(TimeValue.timeValueMinutes(2))
                .setSize(randomIntBetween(1, 10)).get();
        String scrollId = response.getScrollId();
        String tamperedScrollId = randomBoolean() ? scrollId.substring(randomIntBetween(1, 10)) :
                scrollId + randomAlphaOfLength(randomIntBetween(3, 10));

        try {
            assertThrowsAuthorizationException(client().prepareSearchScroll(tamperedScrollId).setScroll(TimeValue.timeValueMinutes(2))::get,
                    equalTo("invalid request. tampered signed text"));
        } finally {
            clearScroll(scrollId);
        }
    }

    public void testClearScrollWithTamperedScrollId() throws Exception {
        IndexRequestBuilder[] docs = new IndexRequestBuilder[randomIntBetween(20, 100)];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = client().prepareIndex("idx", "type").setSource("field", "value");
        }
        indexRandom(true, docs);
        SearchResponse response = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setScroll(TimeValue.timeValueMinutes(2))
                .setSize(5).get();
        String scrollId = response.getScrollId();
        String tamperedScrollId = randomBoolean() ? scrollId.substring(randomIntBetween(1, 10)) :
                scrollId + randomAlphaOfLength(randomIntBetween(3, 10));

        try {
            assertThrowsAuthorizationException(client().prepareClearScroll().addScrollId(tamperedScrollId)::get,
                    equalTo("invalid request. tampered signed text"));
        } finally {
            clearScroll(scrollId);
        }
    }

    private void assertSigned(String scrollId) {
        CryptoService cryptoService = internalCluster().getDataNodeInstance(CryptoService.class);
        String message = String.format(Locale.ROOT, "Expected scrollId [%s] to be signed, but was not", scrollId);
        assertThat(message, cryptoService.isSigned(scrollId), is(true));
    }
}

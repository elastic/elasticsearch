/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequestBuilder;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.After;

import java.util.Collections;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SecurityScrollTests extends SecurityIntegTestCase {

    public void testScrollIsPerUser() throws Exception {
        assertSecurityIndexActive();
        new PutRoleRequestBuilder(client()).name("scrollable")
            .addIndices(new String[] { randomAlphaOfLengthBetween(4, 12) }, new String[] { "read" }, null, null, null, randomBoolean())
            .get();
        new PutUserRequestBuilder(client()).username("other")
            .password(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING, getFastStoredHashAlgoForTests())
            .roles("scrollable")
            .get();

        final int numDocs = randomIntBetween(4, 16);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = client().prepareIndex("foo").setSource("doc", i);
        }
        indexRandom(true, docs);

        SearchResponse response = client().prepareSearch("foo")
            .setScroll(TimeValue.timeValueSeconds(5L))
            .setQuery(matchAllQuery())
            .setSize(1)
            .get();
        assertEquals(numDocs, response.getHits().getTotalHits().value);
        assertEquals(1, response.getHits().getHits().length);

        if (randomBoolean()) {
            response = client().prepareSearchScroll(response.getScrollId()).setScroll(TimeValue.timeValueSeconds(5L)).get();
            assertEquals(numDocs, response.getHits().getTotalHits().value);
            assertEquals(1, response.getHits().getHits().length);
        }

        final String scrollId = response.getScrollId();
        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().filterWithHeader(
                Collections.singletonMap(
                    "Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue("other", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
                )
            ).prepareSearchScroll(scrollId).get()
        );
        for (ShardSearchFailure failure : e.shardFailures()) {
            assertThat(ExceptionsHelper.unwrapCause(failure.getCause()), instanceOf(SearchContextMissingException.class));
        }
    }

    public void testSearchAndClearScroll() throws Exception {
        IndexRequestBuilder[] docs = new IndexRequestBuilder[randomIntBetween(20, 100)];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = client().prepareIndex("idx").setSource("field", "value");
        }
        indexRandom(true, docs);
        SearchResponse response = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setScroll(TimeValue.timeValueSeconds(5L))
            .setSize(randomIntBetween(1, 10))
            .get();

        int hits = 0;
        try {
            do {
                assertHitCount(response, docs.length);
                hits += response.getHits().getHits().length;
                response = client().prepareSearchScroll(response.getScrollId()).setScroll(TimeValue.timeValueSeconds(5L)).get();
            } while (response.getHits().getHits().length != 0);

            assertThat(hits, equalTo(docs.length));
        } finally {
            clearScroll(response.getScrollId());
        }
    }

    @After
    public void cleanupSecurityIndex() throws Exception {
        super.deleteSecurityIndex();
    }
}

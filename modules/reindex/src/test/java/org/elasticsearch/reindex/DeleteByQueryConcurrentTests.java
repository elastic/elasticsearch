/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class DeleteByQueryConcurrentTests extends ReindexTestCase {

    public void testConcurrentDeleteByQueriesOnDifferentDocs() throws Throwable {
        final int threadCount = scaledRandomIntBetween(2, 5);
        final long docs = randomIntBetween(1, 50);

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < docs; i++) {
            for (int t = 0; t < threadCount; t++) {
                builders.add(prepareIndex("test").setSource("field", t));
            }
        }
        indexRandom(true, true, true, builders);

        for (int t = 0; t < threadCount; t++) {
            assertHitCount(prepareSearch("test").setSize(0).setQuery(QueryBuilders.termQuery("field", t)), docs);
        }
        startInParallel(
            threadCount,
            threadNum -> assertThat(
                deleteByQuery().source("_all").filter(termQuery("field", threadNum)).refresh(true).get(),
                matcher().deleted(docs)
            )
        );

        for (int t = 0; t < threadCount; t++) {
            assertHitCount(prepareSearch("test").setSize(0).setQuery(QueryBuilders.termQuery("field", t)), 0);
        }
    }

    public void testConcurrentDeleteByQueriesOnSameDocs() throws Throwable {
        final long docs = randomIntBetween(50, 100);

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < docs; i++) {
            builders.add(prepareIndex("test").setId(String.valueOf(i)).setSource("foo", "bar"));
        }
        indexRandom(true, true, true, builders);

        final int threadCount = scaledRandomIntBetween(2, 9);

        final MatchQueryBuilder query = matchQuery("foo", "bar");
        final AtomicLong deleted = new AtomicLong(0);
        // Some deletions might fail due to version conflict, but what matters here is the total of successful deletions
        startInParallel(threadCount, i -> deleted.addAndGet(deleteByQuery().source("test").filter(query).refresh(true).get().getDeleted()));

        assertHitCount(prepareSearch("test").setSize(0), 0L);
        assertThat(deleted.get(), equalTo(docs));
    }
}

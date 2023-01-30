/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.Before;

import java.util.List;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.sameInstance;

public class SemanticSearchActionKnnQueryOptionsTests extends AbstractWireSerializingTestCase<SemanticSearchAction.KnnQueryOptions> {

    private NamedWriteableRegistry namedWriteableRegistry;
    private NamedXContentRegistry namedXContentRegistry;

    @Before
    public void registerNamedXContents() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        namedXContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return namedXContentRegistry;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    public static SemanticSearchAction.KnnQueryOptions randomInstance() {
        int k = randomIntBetween(1, 10);
        var knnOptions = new SemanticSearchAction.KnnQueryOptions(randomAlphaOfLength(5), k, randomIntBetween(k, 100));
        if (randomBoolean()) {
            knnOptions.boost(randomFloat());
        }

        int numFilters = randomIntBetween(0, 3);
        for (int i = 0; i < numFilters; i++) {
            knnOptions.addFilterQueries(List.of(QueryBuilders.termQuery(randomAlphaOfLength(5), randomAlphaOfLength(10))));
        }
        return knnOptions;
    }

    @Override
    protected Writeable.Reader<SemanticSearchAction.KnnQueryOptions> instanceReader() {
        return SemanticSearchAction.KnnQueryOptions::new;
    }

    @Override
    protected SemanticSearchAction.KnnQueryOptions createTestInstance() {
        return randomInstance();
    }

    @Override
    protected SemanticSearchAction.KnnQueryOptions mutateInstance(SemanticSearchAction.KnnQueryOptions instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public void testToKnnSearchBuilder() {
        var knnOptions = new SemanticSearchAction.KnnQueryOptions("foo", 5, 100);
        knnOptions.boost(20.0f);
        var termsQuery = QueryBuilders.termQuery("foo", "bar");
        knnOptions.addFilterQueries(List.of(termsQuery));

        var knnSearch = knnOptions.toKnnSearchBuilder(new float[] { 0.1f, 0.2f });
        assertEquals(5, knnSearch.k());
        var knnQuery = knnSearch.toQueryBuilder();
        assertEquals(100, knnQuery.numCands());
        assertEquals("foo", knnQuery.getFieldName());
        assertThat(knnQuery.filterQueries(), contains(sameInstance(termsQuery)));
        assertEquals(20.0f, knnQuery.boost(), 0.001);
    }
}

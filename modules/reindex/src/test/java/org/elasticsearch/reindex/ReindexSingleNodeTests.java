/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequestBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.reindex.ReindexTestCase.matcher;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class ReindexSingleNodeTests extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(ReindexPlugin.class);
    }

    public void testDeprecatedSort() {
        int max = between(2, 20);
        for (int i = 0; i < max; i++) {
            client().prepareIndex("source").setId(Integer.toString(i)).setSource("foo", i).get();
        }

        client().admin().indices().prepareRefresh("source").get();
        assertHitCount(client().prepareSearch("source").setSize(0).get(), max);

        // Copy a subset of the docs sorted
        int subsetSize = randomIntBetween(1, max - 1);
        ReindexRequestBuilder copy = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE).source("source")
            .destination("dest")
            .refresh(true);
        copy.maxDocs(subsetSize);
        copy.request().addSortField("foo", SortOrder.DESC);
        assertThat(copy.get(), matcher().created(subsetSize));

        assertHitCount(client().prepareSearch("dest").setSize(0).get(), subsetSize);
        assertHitCount(client().prepareSearch("dest").setQuery(new RangeQueryBuilder("foo").gte(0).lt(max - subsetSize)).get(), 0);
        assertWarnings(ReindexValidator.SORT_DEPRECATED_MESSAGE);
    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.index.reindex.ReindexTestCase.matcher;
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
        ReindexRequestBuilder copy = new ReindexRequestBuilder(client(), ReindexAction.INSTANCE)
            .source("source").destination("dest").refresh(true);
        copy.maxDocs(subsetSize);
        copy.request().addSortField("foo", SortOrder.DESC);
        assertThat(copy.get(), matcher().created(subsetSize));

        assertHitCount(client().prepareSearch("dest").setSize(0).get(), subsetSize);
        assertHitCount(client().prepareSearch("dest").setQuery(new RangeQueryBuilder("foo").gte(0).lt(max-subsetSize)).get(), 0);
        assertWarnings(ReindexValidator.SORT_DEPRECATED_MESSAGE);
    }
}

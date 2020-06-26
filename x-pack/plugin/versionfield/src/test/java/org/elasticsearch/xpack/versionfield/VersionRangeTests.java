/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.versionfield;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.Collection;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class VersionRangeTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(VersionFieldPlugin.class, LocalStateCompositeXPackPlugin.class);
    }

    public void testSimpleVersionRange() throws Exception {
        createIndex(
            "test",
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0).build(),
            "_doc",
            "versionrange",
            "type=version_range"
        );
        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource("{\"versionrange\":{\"gt\":\"1.1.0\",\"lte\":\"2.2.3\"}}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex("test")
            .setId("2")
            .setSource("{\"versionrange\":{\"gte\":\"4.0.0-beta.2\",\"lte\":\"4.0.0-rc5\"}}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex("test")
            .setId("3")
            .setSource("{\"versionrange\":{\"gte\":\"8.0.0-alpha.1\",\"lte\":\"8.0.0-alpha.9\"}}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex("test")
            .setId("4")
            .setSource("{\"versionrange\":{\"gte\":\"8.0.0-alpha.0\",\"lte\":\"8.0.0-alpha.0.1\"}}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();

        SearchResponse search = client().prepareSearch()
            .setQuery(rangeQuery("versionrange").gte("0.1.6").lte("2.7.0").relation("within"))
            .get();

        assertHitCount(search, 1L);

        search = client().prepareSearch().setQuery(rangeQuery("versionrange").gte("1.1.6").lte("1.7.0").relation("contains")).get();

        assertHitCount(search, 1L);

        search = client().prepareSearch().setQuery(rangeQuery("versionrange").gte("1.1.6").lte("3.7.0").relation("contains")).get();

        assertHitCount(search, 0L);

        search = client().prepareSearch().setQuery(rangeQuery("versionrange").gte("1.8.6").lte("3.7.0").relation("intersects")).get();

        assertHitCount(search, 1L);

        search = client().prepareSearch().setQuery(rangeQuery("versionrange").gte("3.0.0").lte("4.0.0").relation("within")).get();

        assertHitCount(search, 1L);

        search = client().prepareSearch().setQuery(rangeQuery("versionrange").gte("3.0.0").lte("4.0.0-rc3").relation("within")).get();

        assertHitCount(search, 0L);

        search = client().prepareSearch().setQuery(termQuery("versionrange", "4.0.0-rc3")).get();

        assertHitCount(search, 1L);

        search = client().prepareSearch()
            .setQuery(rangeQuery("versionrange").gte("8.0.0-alpha.2").lte("8.0.0-alpha.4").relation("contains"))
            .get();

        assertHitCount(search, 1L);

        search = client().prepareSearch().setQuery(rangeQuery("versionrange").gte("1.0.0").lte("1.1.0").relation("intersects")).get();

        assertHitCount(search, 0L);

        search = client().prepareSearch().setQuery(rangeQuery("versionrange").gte("1.0.0").lte("1.1.0.0").relation("intersects")).get();

        assertHitCount(search, 1L);

        search = client().prepareSearch().setQuery(rangeQuery("versionrange").gte("2.2.3").lte("3.0.0").relation("intersects")).get();

        assertHitCount(search, 1L);

        search = client().prepareSearch().setQuery(rangeQuery("versionrange").gt("2.2.3").lte("3.0.0").relation("intersects")).get();

        assertHitCount(search, 0L);
    }
}

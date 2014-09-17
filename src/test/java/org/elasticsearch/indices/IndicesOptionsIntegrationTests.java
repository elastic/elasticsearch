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
package org.elasticsearch.indices;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.flush.FlushRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequestBuilder;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersRequestBuilder;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.percolate.MultiPercolateRequestBuilder;
import org.elasticsearch.action.percolate.PercolateRequestBuilder;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.action.percolate.PercolateSourceBuilder.docBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

public class IndicesOptionsIntegrationTests extends ElasticsearchIntegrationTest {

    @Test
    public void testSpecifiedIndexUnavailable() throws Exception {
        createIndex("test1");
        ensureYellow();

        // Verify defaults
        verify(search("test1", "test2"), true);
        verify(msearch(null, "test1", "test2"), true);
        verify(count("test1", "test2"), true);
        verify(clearCache("test1", "test2"), true);
        verify(_flush("test1", "test2"),true);
        verify(segments("test1", "test2"), true);
        verify(stats("test1", "test2"), true);
        verify(optimize("test1", "test2"), true);
        verify(refresh("test1", "test2"), true);
        verify(validateQuery("test1", "test2"), true);
        verify(aliasExists("test1", "test2"), true);
        verify(typesExists("test1", "test2"), true);
        verify(deleteByQuery("test1", "test2"), true);
        verify(percolate("test1", "test2"), true);
        verify(mpercolate(null, "test1", "test2"), false);
        verify(suggest("test1", "test2"), true);
        verify(getAliases("test1", "test2"), true);
        verify(getFieldMapping("test1", "test2"), true);
        verify(getMapping("test1", "test2"), true);
        verify(getWarmer("test1", "test2"), true);
        verify(getSettings("test1", "test2"), true);

        IndicesOptions options = IndicesOptions.strictExpandOpen();
        verify(search("test1", "test2").setIndicesOptions(options), true);
        verify(msearch(options, "test1", "test2"), true);
        verify(count("test1", "test2").setIndicesOptions(options), true);
        verify(clearCache("test1", "test2").setIndicesOptions(options), true);
        verify(_flush("test1", "test2").setIndicesOptions(options),true);
        verify(segments("test1", "test2").setIndicesOptions(options), true);
        verify(stats("test1", "test2").setIndicesOptions(options), true);
        verify(optimize("test1", "test2").setIndicesOptions(options), true);
        verify(refresh("test1", "test2").setIndicesOptions(options), true);
        verify(validateQuery("test1", "test2").setIndicesOptions(options), true);
        verify(aliasExists("test1", "test2").setIndicesOptions(options), true);
        verify(typesExists("test1", "test2").setIndicesOptions(options), true);
        verify(deleteByQuery("test1", "test2").setIndicesOptions(options), true);
        verify(percolate("test1", "test2").setIndicesOptions(options), true);
        verify(mpercolate(options, "test1", "test2").setIndicesOptions(options), false);
        verify(suggest("test1", "test2").setIndicesOptions(options), true);
        verify(getAliases("test1", "test2").setIndicesOptions(options), true);
        verify(getFieldMapping("test1", "test2").setIndicesOptions(options), true);
        verify(getMapping("test1", "test2").setIndicesOptions(options), true);
        verify(getWarmer("test1", "test2").setIndicesOptions(options), true);
        verify(getSettings("test1", "test2").setIndicesOptions(options), true);

        options = IndicesOptions.lenientExpandOpen();
        verify(search("test1", "test2").setIndicesOptions(options), false);
        verify(msearch(options, "test1", "test2").setIndicesOptions(options), false);
        verify(count("test1", "test2").setIndicesOptions(options), false);
        verify(clearCache("test1", "test2").setIndicesOptions(options), false);
        verify(_flush("test1", "test2").setIndicesOptions(options), false);
        verify(segments("test1", "test2").setIndicesOptions(options), false);
        verify(stats("test1", "test2").setIndicesOptions(options), false);
        verify(optimize("test1", "test2").setIndicesOptions(options), false);
        verify(refresh("test1", "test2").setIndicesOptions(options), false);
        verify(validateQuery("test1", "test2").setIndicesOptions(options), false);
        verify(aliasExists("test1", "test2").setIndicesOptions(options), false);
        verify(typesExists("test1", "test2").setIndicesOptions(options), false);
        verify(deleteByQuery("test1", "test2").setIndicesOptions(options), false);
        verify(percolate("test1", "test2").setIndicesOptions(options), false);
        verify(mpercolate(options, "test1", "test2").setIndicesOptions(options), false);
        verify(suggest("test1", "test2").setIndicesOptions(options), false);
        verify(getAliases("test1", "test2").setIndicesOptions(options), false);
        verify(getFieldMapping("test1", "test2").setIndicesOptions(options), false);
        verify(getMapping("test1", "test2").setIndicesOptions(options), false);
        verify(getWarmer("test1", "test2").setIndicesOptions(options), false);
        verify(getSettings("test1", "test2").setIndicesOptions(options), false);

        options = IndicesOptions.strictExpandOpen();
        assertAcked(prepareCreate("test2"));
        ensureYellow();
        verify(search("test1", "test2").setIndicesOptions(options), false);
        verify(msearch(options, "test1", "test2").setIndicesOptions(options), false);
        verify(count("test1", "test2").setIndicesOptions(options), false);
        verify(clearCache("test1", "test2").setIndicesOptions(options), false);
        verify(_flush("test1", "test2").setIndicesOptions(options),false);
        verify(segments("test1", "test2").setIndicesOptions(options), false);
        verify(stats("test1", "test2").setIndicesOptions(options), false);
        verify(optimize("test1", "test2").setIndicesOptions(options), false);
        verify(refresh("test1", "test2").setIndicesOptions(options), false);
        verify(validateQuery("test1", "test2").setIndicesOptions(options), false);
        verify(aliasExists("test1", "test2").setIndicesOptions(options), false);
        verify(typesExists("test1", "test2").setIndicesOptions(options), false);
        verify(deleteByQuery("test1", "test2").setIndicesOptions(options), false);
        verify(percolate("test1", "test2").setIndicesOptions(options), false);
        verify(mpercolate(options, "test1", "test2").setIndicesOptions(options), false);
        verify(suggest("test1", "test2").setIndicesOptions(options), false);
        verify(getAliases("test1", "test2").setIndicesOptions(options), false);
        verify(getFieldMapping("test1", "test2").setIndicesOptions(options), false);
        verify(getMapping("test1", "test2").setIndicesOptions(options), false);
        verify(getWarmer("test1", "test2").setIndicesOptions(options), false);
        verify(getSettings("test1", "test2").setIndicesOptions(options), false);
    }

    @Test
    public void testSpecifiedIndexUnavailable_snapshotRestore() throws Exception {
        createIndex("test1");
        ensureGreen("test1");
        waitForRelocation();

        PutRepositoryResponse putRepositoryResponse = client().admin().cluster().preparePutRepository("dummy-repo")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder().put("location", newTempDir())).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
        client().admin().cluster().prepareCreateSnapshot("dummy-repo", "snap1").setWaitForCompletion(true).get();

        verify(snapshot("snap2", "test1", "test2"), true);
        verify(restore("snap1", "test1", "test2"), true);

        IndicesOptions options = IndicesOptions.strictExpandOpen();
        verify(snapshot("snap2", "test1", "test2").setIndicesOptions(options), true);
        verify(restore("snap1", "test1", "test2").setIndicesOptions(options), true);

        options = IndicesOptions.lenientExpandOpen();
        verify(snapshot("snap2", "test1", "test2").setIndicesOptions(options), false);
        verify(restore("snap2", "test1", "test2").setIndicesOptions(options), false);

        options = IndicesOptions.strictExpandOpen();
        createIndex("test2");
        //TODO: temporary work-around for #5531
        ensureGreen("test2");
        waitForRelocation();
        verify(snapshot("snap3", "test1", "test2").setIndicesOptions(options), false);
        verify(restore("snap3", "test1", "test2").setIndicesOptions(options), false);
    }

    @Test
    public void testWildcardBehaviour() throws Exception {
        // Verify defaults for wildcards, when specifying no indices (*, _all, /)
        String[] indices = Strings.EMPTY_ARRAY;
        verify(search(indices), false);
        verify(msearch(null, indices), false);
        verify(count(indices), false);
        verify(clearCache(indices), false);
        verify(_flush(indices),false);
        verify(segments(indices), true);
        verify(stats(indices), false);
        verify(optimize(indices), false);
        verify(refresh(indices), false);
        verify(validateQuery(indices), true);
        verify(aliasExists(indices), false);
        verify(typesExists(indices), false);
        verify(deleteByQuery(indices), true);
        verify(percolate(indices), false);
        verify(mpercolate(null, indices), false);
        verify(suggest(indices), false);
        verify(getAliases(indices), false);
        verify(getFieldMapping(indices), false);
        verify(getMapping(indices), false);
        verify(getWarmer(indices), false);
        verify(getSettings(indices), false);

        // Now force allow_no_indices=true
        IndicesOptions options = IndicesOptions.fromOptions(false, true, true, false);
        verify(search(indices).setIndicesOptions(options), false);
        verify(msearch(options, indices).setIndicesOptions(options), false);
        verify(count(indices).setIndicesOptions(options), false);
        verify(clearCache(indices).setIndicesOptions(options), false);
        verify(_flush(indices).setIndicesOptions(options),false);
        verify(segments(indices).setIndicesOptions(options), false);
        verify(stats(indices).setIndicesOptions(options), false);
        verify(optimize(indices).setIndicesOptions(options), false);
        verify(refresh(indices).setIndicesOptions(options), false);
        verify(validateQuery(indices).setIndicesOptions(options), false);
        verify(aliasExists(indices).setIndicesOptions(options), false);
        verify(typesExists(indices).setIndicesOptions(options), false);
        verify(deleteByQuery(indices).setIndicesOptions(options), false);
        verify(percolate(indices).setIndicesOptions(options), false);
        verify(mpercolate(options, indices), false);
        verify(suggest(indices).setIndicesOptions(options), false);
        verify(getAliases(indices).setIndicesOptions(options), false);
        verify(getFieldMapping(indices).setIndicesOptions(options), false);
        verify(getMapping(indices).setIndicesOptions(options), false);
        verify(getWarmer(indices).setIndicesOptions(options), false);
        verify(getSettings(indices).setIndicesOptions(options), false);

        assertAcked(prepareCreate("foobar"));
        client().prepareIndex("foobar", "type", "1").setSource("k", "v").setRefresh(true).execute().actionGet();

        // Verify defaults for wildcards, with one wildcard expression and one existing index
        indices = new String[]{"foo*"};
        verify(search(indices), false, 1);
        verify(msearch(null, indices), false, 1);
        verify(count(indices), false, 1);
        verify(clearCache(indices), false);
        verify(_flush(indices),false);
        verify(segments(indices), false);
        verify(stats(indices), false);
        verify(optimize(indices), false);
        verify(refresh(indices), false);
        verify(validateQuery(indices), false);
        verify(aliasExists(indices), false);
        verify(typesExists(indices), false);
        verify(deleteByQuery(indices), false);
        verify(percolate(indices), false);
        verify(mpercolate(null, indices), false);
        verify(suggest(indices), false);
        verify(getAliases(indices), false);
        verify(getFieldMapping(indices), false);
        verify(getMapping(indices), false);
        verify(getWarmer(indices), false);
        verify(getSettings(indices).setIndicesOptions(options), false);

        // Verify defaults for wildcards, with two wildcard expression and one existing index
        indices = new String[]{"foo*", "bar*"};
        verify(search(indices), false, 1);
        verify(msearch(null, indices), false, 1);
        verify(count(indices), false, 1);
        verify(clearCache(indices), false);
        verify(_flush(indices),false);
        verify(segments(indices), true);
        verify(stats(indices), false);
        verify(optimize(indices), false);
        verify(refresh(indices), false);
        verify(validateQuery(indices), true);
        verify(aliasExists(indices), false);
        verify(typesExists(indices), false);
        verify(deleteByQuery(indices), true);
        verify(percolate(indices), false);
        verify(mpercolate(null, indices), false);
        verify(suggest(indices), false);
        verify(getAliases(indices), false);
        verify(getFieldMapping(indices), false);
        verify(getMapping(indices), false);
        verify(getWarmer(indices), false);
        verify(getSettings(indices).setIndicesOptions(options), false);

        // Now force allow_no_indices=true
        options = IndicesOptions.fromOptions(false, true, true, false);
        verify(search(indices).setIndicesOptions(options), false, 1);
        verify(msearch(options, indices).setIndicesOptions(options), false, 1);
        verify(count(indices).setIndicesOptions(options), false, 1);
        verify(clearCache(indices).setIndicesOptions(options), false);
        verify(_flush(indices).setIndicesOptions(options),false);
        verify(segments(indices).setIndicesOptions(options), false);
        verify(stats(indices).setIndicesOptions(options), false);
        verify(optimize(indices).setIndicesOptions(options), false);
        verify(refresh(indices).setIndicesOptions(options), false);
        verify(validateQuery(indices).setIndicesOptions(options), false);
        verify(aliasExists(indices).setIndicesOptions(options), false);
        verify(typesExists(indices).setIndicesOptions(options), false);
        verify(deleteByQuery(indices).setIndicesOptions(options), false);
        verify(percolate(indices).setIndicesOptions(options), false);
        verify(mpercolate(options, indices), false);
        verify(suggest(indices).setIndicesOptions(options), false);
        verify(getAliases(indices).setIndicesOptions(options), false);
        verify(getFieldMapping(indices).setIndicesOptions(options), false);
        verify(getMapping(indices).setIndicesOptions(options), false);
        verify(getWarmer(indices).setIndicesOptions(options), false);
        verify(getSettings(indices).setIndicesOptions(options), false);
    }

    @Test
    public void testWildcardBehaviour_snapshotRestore() throws Exception {
        createIndex("foobar");
        ensureGreen("foobar");
        waitForRelocation();

        PutRepositoryResponse putRepositoryResponse = client().admin().cluster().preparePutRepository("dummy-repo")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder().put("location", newTempDir())).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
        client().admin().cluster().prepareCreateSnapshot("dummy-repo", "snap1").setWaitForCompletion(true).get();

        IndicesOptions options = IndicesOptions.fromOptions(false, false, true, false);
        verify(snapshot("snap2", "foo*", "bar*").setIndicesOptions(options), true);
        verify(restore("snap1", "foo*", "bar*").setIndicesOptions(options), true);

        options = IndicesOptions.strictExpandOpen();
        verify(snapshot("snap2", "foo*", "bar*").setIndicesOptions(options), false);
        verify(restore("snap2", "foo*", "bar*").setIndicesOptions(options), false);

        assertAcked(prepareCreate("barbaz"));
        //TODO: temporary work-around for #5531
        ensureGreen("barbaz");
        waitForRelocation();
        options = IndicesOptions.fromOptions(false, false, true, false);
        verify(snapshot("snap3", "foo*", "bar*").setIndicesOptions(options), false);
        verify(restore("snap3", "foo*", "bar*").setIndicesOptions(options), false);

        options = IndicesOptions.fromOptions(false, false, true, false);
        verify(snapshot("snap4", "foo*", "baz*").setIndicesOptions(options), true);
        verify(restore("snap3", "foo*", "baz*").setIndicesOptions(options), true);
    }

    @Test
    public void testAllMissing_lenient() throws Exception {
        createIndex("test1");
        client().prepareIndex("test1", "type", "1").setSource("k", "v").setRefresh(true).execute().actionGet();
        SearchResponse response = client().prepareSearch("test2")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setQuery(matchAllQuery())
                .execute().actionGet();
        assertHitCount(response, 0l);

        response = client().prepareSearch("test2","test3").setQuery(matchAllQuery())
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .execute().actionGet();
        assertHitCount(response, 0l);
        
        //you should still be able to run empty searches without things blowing up
        response  = client().prepareSearch()
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setQuery(matchAllQuery())
                .execute().actionGet();
        assertHitCount(response, 1l);
    }

    @Test
    public void testAllMissing_strict() throws Exception {
        createIndex("test1");
        ensureYellow();
        try {
            client().prepareSearch("test2")
                    .setQuery(matchAllQuery())
                    .execute().actionGet();
            fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }

        try {
            client().prepareSearch("test2","test3")
                    .setQuery(matchAllQuery())
                    .execute().actionGet();
            fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }

        //you should still be able to run empty searches without things blowing up
        client().prepareSearch().setQuery(matchAllQuery()).execute().actionGet();
    }

    @Test
    // For now don't handle closed indices
    public void testCloseApi_specifiedIndices() throws Exception {
        createIndex("test1", "test2");
        ensureYellow();
        verify(search("test1", "test2"), false);
        verify(count("test1", "test2"), false);
        assertAcked(client().admin().indices().prepareClose("test2").get());

        verify(search("test1", "test2"), true);
        verify(count("test1", "test2"), true);

        IndicesOptions options = IndicesOptions.fromOptions(true, true, true, false, IndicesOptions.strictExpandOpenAndForbidClosed());
        verify(search("test1", "test2").setIndicesOptions(options), false);
        verify(count("test1", "test2").setIndicesOptions(options), false);

        verify(search(), false);
        verify(count(), false);

        verify(search("t*"), false);
        verify(count("t*"), false);
    }

    @Test
    public void testCloseApi_wildcards() throws Exception {
        createIndex("foo", "foobar", "bar", "barbaz");
        ensureYellow();

        verify(client().admin().indices().prepareClose("bar*"), false);
        verify(client().admin().indices().prepareClose("bar*"), true);

        verify(client().admin().indices().prepareClose("foo*"), false);
        verify(client().admin().indices().prepareClose("foo*"), true);
        verify(client().admin().indices().prepareClose("_all"), true);

        verify(client().admin().indices().prepareOpen("bar*"), false);
        verify(client().admin().indices().prepareOpen("_all"), false);
        verify(client().admin().indices().prepareOpen("_all"), true);
    }

    @Test
    public void testDeleteIndex() throws Exception {
        createIndex("foobar");
        ensureYellow();

        verify(client().admin().indices().prepareDelete("foo"), true);
        assertThat(client().admin().indices().prepareExists("foobar").get().isExists(), equalTo(true));
        verify(client().admin().indices().prepareDelete("foobar"), false);
        assertThat(client().admin().indices().prepareExists("foobar").get().isExists(), equalTo(false));
    }

    @Test
    public void testDeleteIndex_wildcard() throws Exception {
        verify(client().admin().indices().prepareDelete("_all"), false);

        createIndex("foo", "foobar", "bar", "barbaz");
        ensureYellow();

        verify(client().admin().indices().prepareDelete("foo*"), false);
        assertThat(client().admin().indices().prepareExists("foo").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareExists("foobar").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareExists("bar").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareExists("barbaz").get().isExists(), equalTo(true));

        verify(client().admin().indices().prepareDelete("foo*"), false);

        verify(client().admin().indices().prepareDelete("_all"), false);
        assertThat(client().admin().indices().prepareExists("foo").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareExists("foobar").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareExists("bar").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareExists("barbaz").get().isExists(), equalTo(false));
    }

    @Test
    public void testDeleteMapping() throws Exception {
        assertAcked(prepareCreate("foobar").addMapping("type1", "field", "type=string"));
        ensureGreen();

        verify(client().admin().indices().prepareDeleteMapping("foo").setType("type1"), true);
        assertThat(client().admin().indices().prepareTypesExists("foobar").setTypes("type1").get().isExists(), equalTo(true));
        verify(client().admin().indices().prepareDeleteMapping("foobar").setType("type1"), false);
        assertThat(client().admin().indices().prepareTypesExists("foobar").setTypes("type1").get().isExists(), equalTo(false));
    }

    @Test
    public void testDeleteMapping_wildcard() throws Exception {
        verify(client().admin().indices().prepareDeleteMapping("_all").setType("type1"), true);

        assertAcked(prepareCreate("foo").addMapping("type1", "field", "type=string"));
        assertAcked(prepareCreate("foobar").addMapping("type1", "field", "type=string"));
        assertAcked(prepareCreate("bar").addMapping("type1", "field", "type=string"));
        assertAcked(prepareCreate("barbaz").addMapping("type1", "field", "type=string"));
        // we wait for green to make sure indices with mappings have been created on all relevant
        // nodes, and that recovery won't re-introduce a mapping
        ensureGreen();

        verify(client().admin().indices().prepareDeleteMapping("foo*").setType("type1"), false);
        assertThat(client().admin().indices().prepareTypesExists("foo").setTypes("type1").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareTypesExists("foobar").setTypes("type1").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareTypesExists("bar").setTypes("type1").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareTypesExists("barbaz").setTypes("type1").get().isExists(), equalTo(true));

        assertAcked(client().admin().indices().prepareDelete("foo*"));

        verify(client().admin().indices().prepareDeleteMapping("foo*").setType("type1"), true);

        verify(client().admin().indices().prepareDeleteMapping("_all").setType("type1"), false);
        assertThat(client().admin().indices().prepareTypesExists("bar").setTypes("type1").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareTypesExists("barbaz").setTypes("type1").get().isExists(), equalTo(false));
    }


    @Test
    public void testPutWarmer() throws Exception {
        createIndex("foobar");
        ensureYellow();
        verify(client().admin().indices().preparePutWarmer("warmer1").setSearchRequest(client().prepareSearch().setIndices("foobar").setQuery(QueryBuilders.matchAllQuery())), false);
        assertThat(client().admin().indices().prepareGetWarmers("foobar").setWarmers("warmer1").get().getWarmers().size(), equalTo(1));

    }
    
    @Test
    public void testPutWarmer_wildcard() throws Exception {
        createIndex("foo", "foobar", "bar", "barbaz");
        ensureYellow();

        verify(client().admin().indices().preparePutWarmer("warmer1").setSearchRequest(client().prepareSearch().setIndices("foo*").setQuery(QueryBuilders.matchAllQuery())), false);
        
        assertThat(client().admin().indices().prepareGetWarmers("foo").setWarmers("warmer1").get().getWarmers().size(), equalTo(1));
        assertThat(client().admin().indices().prepareGetWarmers("foobar").setWarmers("warmer1").get().getWarmers().size(), equalTo(1));
        assertThat(client().admin().indices().prepareGetWarmers("bar").setWarmers("warmer1").get().getWarmers().size(), equalTo(0));
        assertThat(client().admin().indices().prepareGetWarmers("barbaz").setWarmers("warmer1").get().getWarmers().size(), equalTo(0));

        verify(client().admin().indices().preparePutWarmer("warmer2").setSearchRequest(client().prepareSearch().setIndices().setQuery(QueryBuilders.matchAllQuery())), false);
        
        assertThat(client().admin().indices().prepareGetWarmers("foo").setWarmers("warmer2").get().getWarmers().size(), equalTo(1));
        assertThat(client().admin().indices().prepareGetWarmers("foobar").setWarmers("warmer2").get().getWarmers().size(), equalTo(1));
        assertThat(client().admin().indices().prepareGetWarmers("bar").setWarmers("warmer2").get().getWarmers().size(), equalTo(1));
        assertThat(client().admin().indices().prepareGetWarmers("barbaz").setWarmers("warmer2").get().getWarmers().size(), equalTo(1));
        
    }

    @Test
    public void testPutAlias() throws Exception {
        createIndex("foobar");
        ensureYellow();
        verify(client().admin().indices().prepareAliases().addAlias("foobar", "foobar_alias"), false);
        assertThat(client().admin().indices().prepareAliasesExist("foobar_alias").setIndices("foobar").get().exists(), equalTo(true));

    }
    
    @Test
    public void testPutAlias_wildcard() throws Exception {
        createIndex("foo", "foobar", "bar", "barbaz");
        ensureYellow();

        verify(client().admin().indices().prepareAliases().addAlias("foo*", "foobar_alias"), false);
        assertThat(client().admin().indices().prepareAliasesExist("foobar_alias").setIndices("foo").get().exists(), equalTo(true));
        assertThat(client().admin().indices().prepareAliasesExist("foobar_alias").setIndices("foobar").get().exists(), equalTo(true));
        assertThat(client().admin().indices().prepareAliasesExist("foobar_alias").setIndices("bar").get().exists(), equalTo(false));
        assertThat(client().admin().indices().prepareAliasesExist("foobar_alias").setIndices("barbaz").get().exists(), equalTo(false));

        verify(client().admin().indices().prepareAliases().addAlias("*", "foobar_alias"), false);
        assertThat(client().admin().indices().prepareAliasesExist("foobar_alias").setIndices("foo").get().exists(), equalTo(true));
        assertThat(client().admin().indices().prepareAliasesExist("foobar_alias").setIndices("foobar").get().exists(), equalTo(true));
        assertThat(client().admin().indices().prepareAliasesExist("foobar_alias").setIndices("bar").get().exists(), equalTo(true));
        assertThat(client().admin().indices().prepareAliasesExist("foobar_alias").setIndices("barbaz").get().exists(), equalTo(true));
        
    }
    @Test
    public void testDeleteMapping_typeWildcard() throws Exception {
        verify(client().admin().indices().prepareDeleteMapping("_all").setType("type1"), true);

        assertAcked(prepareCreate("foo").addMapping("type1", "field", "type=string"));
        assertAcked(prepareCreate("foobar").addMapping("type2", "field", "type=string"));
        assertAcked(prepareCreate("bar").addMapping("type3", "field", "type=string"));
        assertAcked(prepareCreate("barbaz").addMapping("type4", "field", "type=string"));
        
        ensureGreen();

        assertThat(client().admin().indices().prepareTypesExists("foo").setTypes("type1").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareTypesExists("foobar").setTypes("type2").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareTypesExists("bar").setTypes("type3").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareTypesExists("barbaz").setTypes("type4").get().isExists(), equalTo(true));
        
        verify(client().admin().indices().prepareDeleteMapping("foo*").setType("type*"), false);
        assertThat(client().admin().indices().prepareTypesExists("foo").setTypes("type1").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareTypesExists("foobar").setTypes("type2").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareTypesExists("bar").setTypes("type3").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareTypesExists("barbaz").setTypes("type4").get().isExists(), equalTo(true));

        assertAcked(client().admin().indices().prepareDelete("foo*"));

        verify(client().admin().indices().prepareDeleteMapping("foo*").setType("type1"), true);

        verify(client().admin().indices().prepareDeleteMapping("_all").setType("type3","type4"), false);
        assertThat(client().admin().indices().prepareTypesExists("bar").setTypes("type3").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareTypesExists("barbaz").setTypes("type4").get().isExists(), equalTo(false));
    }

    @Test
    public void testDeleteWarmer() throws Exception {
        IndexWarmersMetaData.Entry entry = new IndexWarmersMetaData.Entry(
                "test1", new String[]{"typ1"}, false, new BytesArray("{\"query\" : { \"match_all\" : {}}}")
        );
        assertAcked(prepareCreate("foobar").addCustom(new IndexWarmersMetaData(entry)));
        ensureYellow();

        verify(client().admin().indices().prepareDeleteWarmer().setIndices("foo").setNames("test1"), true);
        assertThat(client().admin().indices().prepareGetWarmers("foobar").setWarmers("test1").get().getWarmers().size(), equalTo(1));
        verify(client().admin().indices().prepareDeleteWarmer().setIndices("foobar").setNames("test1"), false);
        assertThat(client().admin().indices().prepareGetWarmers("foobar").setWarmers("test1").get().getWarmers().size(), equalTo(0));
    }

    @Test
    public void testDeleteWarmer_wildcard() throws Exception {
        verify(client().admin().indices().prepareDeleteWarmer().setIndices("_all").setNames("test1"), true);

        IndexWarmersMetaData.Entry entry = new IndexWarmersMetaData.Entry(
                "test1", new String[]{"type1"}, false, new BytesArray("{\"query\" : { \"match_all\" : {}}}")
        );
        assertAcked(prepareCreate("foo").addCustom(new IndexWarmersMetaData(entry)));
        assertAcked(prepareCreate("foobar").addCustom(new IndexWarmersMetaData(entry)));
        assertAcked(prepareCreate("bar").addCustom(new IndexWarmersMetaData(entry)));
        assertAcked(prepareCreate("barbaz").addCustom(new IndexWarmersMetaData(entry)));
        ensureYellow();

        verify(client().admin().indices().prepareDeleteWarmer().setIndices("foo*").setNames("test1"), false);
        assertThat(client().admin().indices().prepareGetWarmers("foo").setWarmers("test1").get().getWarmers().size(), equalTo(0));
        assertThat(client().admin().indices().prepareGetWarmers("foobar").setWarmers("test1").get().getWarmers().size(), equalTo(0));
        assertThat(client().admin().indices().prepareGetWarmers("bar").setWarmers("test1").get().getWarmers().size(), equalTo(1));
        assertThat(client().admin().indices().prepareGetWarmers("barbaz").setWarmers("test1").get().getWarmers().size(), equalTo(1));

        assertAcked(client().admin().indices().prepareDelete("foo*"));

        verify(client().admin().indices().prepareDeleteWarmer().setIndices("foo*").setNames("test1"), true);

        verify(client().admin().indices().prepareDeleteWarmer().setIndices("_all").setNames("test1"), false);
        assertThat(client().admin().indices().prepareGetWarmers("bar").setWarmers("test1").get().getWarmers().size(), equalTo(0));
        assertThat(client().admin().indices().prepareGetWarmers("barbaz").setWarmers("test1").get().getWarmers().size(), equalTo(0));
    }

    @Test
    // Indices exists never throws IndexMissingException, the indices options control its behaviour (return true or false)
    public void testIndicesExists() throws Exception {
        assertThat(client().admin().indices().prepareExists("foo").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareExists("foo").setIndicesOptions(IndicesOptions.lenientExpandOpen()).get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareExists("foo*").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareExists("foo*").setIndicesOptions(IndicesOptions.fromOptions(false, true, true, false)).get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareExists("_all").get().isExists(), equalTo(false));

        createIndex("foo", "foobar", "bar", "barbaz");
        ensureYellow();

        assertThat(client().admin().indices().prepareExists("foo*").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareExists("foobar").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareExists("bar*").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareExists("bar").get().isExists(), equalTo(true));
        assertThat(client().admin().indices().prepareExists("_all").get().isExists(), equalTo(true));
    }

    @Test
    public void testPutMapping() throws Exception {
        verify(client().admin().indices().preparePutMapping("foo").setType("type1").setSource("field", "type=string"), true);
        verify(client().admin().indices().preparePutMapping("_all").setType("type1").setSource("field", "type=string"), true);

        createIndex("foo", "foobar", "bar", "barbaz");
        ensureYellow();

        verify(client().admin().indices().preparePutMapping("foo").setType("type1").setSource("field", "type=string"), false);
        assertThat(client().admin().indices().prepareGetMappings("foo").get().mappings().get("foo").get("type1"), notNullValue());
        verify(client().admin().indices().preparePutMapping("b*").setType("type1").setSource("field", "type=string"), false);
        assertThat(client().admin().indices().prepareGetMappings("bar").get().mappings().get("bar").get("type1"), notNullValue());
        assertThat(client().admin().indices().prepareGetMappings("barbaz").get().mappings().get("barbaz").get("type1"), notNullValue());
        verify(client().admin().indices().preparePutMapping("_all").setType("type2").setSource("field", "type=string"), false);
        assertThat(client().admin().indices().prepareGetMappings("foo").get().mappings().get("foo").get("type2"), notNullValue());
        assertThat(client().admin().indices().prepareGetMappings("foobar").get().mappings().get("foobar").get("type2"), notNullValue());
        assertThat(client().admin().indices().prepareGetMappings("bar").get().mappings().get("bar").get("type2"), notNullValue());
        assertThat(client().admin().indices().prepareGetMappings("barbaz").get().mappings().get("barbaz").get("type2"), notNullValue());
        verify(client().admin().indices().preparePutMapping().setType("type3").setSource("field", "type=string"), false);
        assertThat(client().admin().indices().prepareGetMappings("foo").get().mappings().get("foo").get("type3"), notNullValue());
        assertThat(client().admin().indices().prepareGetMappings("foobar").get().mappings().get("foobar").get("type3"), notNullValue());
        assertThat(client().admin().indices().prepareGetMappings("bar").get().mappings().get("bar").get("type3"), notNullValue());
        assertThat(client().admin().indices().prepareGetMappings("barbaz").get().mappings().get("barbaz").get("type3"), notNullValue());
        

        verify(client().admin().indices().preparePutMapping("c*").setType("type1").setSource("field", "type=string"), true);

        assertAcked(client().admin().indices().prepareClose("barbaz").get());
        verify(client().admin().indices().preparePutMapping("barbaz").setType("type4").setSource("field", "type=string"), false);
        assertThat(client().admin().indices().prepareGetMappings("barbaz").get().mappings().get("barbaz").get("type4"), notNullValue());
    }

    @Test
    public void testUpdateSettings() throws Exception {
        verify(client().admin().indices().prepareUpdateSettings("foo").setSettings(ImmutableSettings.builder().put("a", "b")), true);
        verify(client().admin().indices().prepareUpdateSettings("_all").setSettings(ImmutableSettings.builder().put("a", "b")), true);

        createIndex("foo", "foobar", "bar", "barbaz");
        ensureYellow();
        assertAcked(client().admin().indices().prepareClose("_all").get());

        verify(client().admin().indices().prepareUpdateSettings("foo").setSettings(ImmutableSettings.builder().put("a", "b")), false);
        verify(client().admin().indices().prepareUpdateSettings("bar*").setSettings(ImmutableSettings.builder().put("a", "b")), false);
        verify(client().admin().indices().prepareUpdateSettings("_all").setSettings(ImmutableSettings.builder().put("c", "d")), false);

        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings("foo").get();
        assertThat(settingsResponse.getSetting("foo", "index.a"), equalTo("b"));
        settingsResponse = client().admin().indices().prepareGetSettings("bar*").get();
        assertThat(settingsResponse.getSetting("bar", "index.a"), equalTo("b"));
        assertThat(settingsResponse.getSetting("barbaz", "index.a"), equalTo("b"));
        settingsResponse = client().admin().indices().prepareGetSettings("_all").get();
        assertThat(settingsResponse.getSetting("foo", "index.c"), equalTo("d"));
        assertThat(settingsResponse.getSetting("foobar", "index.c"), equalTo("d"));
        assertThat(settingsResponse.getSetting("bar", "index.c"), equalTo("d"));
        assertThat(settingsResponse.getSetting("barbaz", "index.c"), equalTo("d"));

        assertAcked(client().admin().indices().prepareOpen("_all").get());
        try {
            verify(client().admin().indices().prepareUpdateSettings("barbaz").setSettings(ImmutableSettings.builder().put("e", "f")), false);
        } catch (ElasticsearchIllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Can't update non dynamic settings[[index.e]] for open indices[[barbaz]]"));
        }
        verify(client().admin().indices().prepareUpdateSettings("baz*").setSettings(ImmutableSettings.builder().put("a", "b")), true);
    }

    private static SearchRequestBuilder search(String... indices) {
        return client().prepareSearch(indices).setQuery(matchAllQuery());
    }

    private static MultiSearchRequestBuilder msearch(IndicesOptions options, String... indices) {
        MultiSearchRequestBuilder multiSearchRequestBuilder = client().prepareMultiSearch();
        if (options != null) {
            multiSearchRequestBuilder.setIndicesOptions(options);
        }
        return multiSearchRequestBuilder.add(client().prepareSearch(indices).setQuery(matchAllQuery()));
    }

    private static CountRequestBuilder count(String... indices) {
        return client().prepareCount(indices).setQuery(matchAllQuery());
    }

    private static ClearIndicesCacheRequestBuilder clearCache(String... indices) {
        return client().admin().indices().prepareClearCache(indices);
    }

    private static FlushRequestBuilder _flush(String... indices) {
        return client().admin().indices().prepareFlush(indices);
    }

    private static IndicesSegmentsRequestBuilder segments(String... indices) {
        return client().admin().indices().prepareSegments(indices);
    }

    private static IndicesStatsRequestBuilder stats(String... indices) {
        return client().admin().indices().prepareStats(indices);
    }

    private static OptimizeRequestBuilder optimize(String... indices) {
        return client().admin().indices().prepareOptimize(indices);
    }

    private static RefreshRequestBuilder refresh(String... indices) {
        return client().admin().indices().prepareRefresh(indices);
    }

    private static ValidateQueryRequestBuilder validateQuery(String... indices) {
        return client().admin().indices().prepareValidateQuery(indices);
    }

    private static AliasesExistRequestBuilder aliasExists(String... indices) {
        return client().admin().indices().prepareAliasesExist("dummy").addIndices(indices);
    }

    private static TypesExistsRequestBuilder typesExists(String... indices) {
        return client().admin().indices().prepareTypesExists(indices).setTypes("dummy");
    }

    private static DeleteByQueryRequestBuilder deleteByQuery(String... indices) {
        return client().prepareDeleteByQuery(indices).setQuery(boolQuery().mustNot(matchAllQuery()));
    }

    private static PercolateRequestBuilder percolate(String... indices) {
        return client().preparePercolate().setIndices(indices)
                .setSource(new PercolateSourceBuilder().setDoc(docBuilder().setDoc("k", "v")))
                .setDocumentType("type");
    }

    private static MultiPercolateRequestBuilder mpercolate(IndicesOptions options, String... indices) {
        MultiPercolateRequestBuilder builder = client().prepareMultiPercolate();
        if (options != null) {
            builder.setIndicesOptions(options);
        }
        return builder.add(percolate(indices));
    }

    private static SuggestRequestBuilder suggest(String... indices) {
        return client().prepareSuggest(indices).addSuggestion(SuggestBuilders.termSuggestion("name").field("a"));
    }

    private static GetAliasesRequestBuilder getAliases(String... indices) {
        return client().admin().indices().prepareGetAliases("dummy").addIndices(indices);
    }

    private static GetFieldMappingsRequestBuilder getFieldMapping(String... indices) {
        return client().admin().indices().prepareGetFieldMappings(indices);
    }

    private static GetMappingsRequestBuilder getMapping(String... indices) {
        return client().admin().indices().prepareGetMappings(indices);
    }

    private static GetWarmersRequestBuilder getWarmer(String... indices) {
        return client().admin().indices().prepareGetWarmers(indices);
    }

    private static GetSettingsRequestBuilder getSettings(String... indices) {
        return client().admin().indices().prepareGetSettings(indices);
    }

    private static CreateSnapshotRequestBuilder snapshot(String name, String... indices) {
        return client().admin().cluster().prepareCreateSnapshot("dummy-repo", name).setWaitForCompletion(true).setIndices(indices);
    }

    private static RestoreSnapshotRequestBuilder restore(String name, String... indices) {
        return client().admin().cluster().prepareRestoreSnapshot("dummy-repo", name)
                .setRenamePattern("(.+)").setRenameReplacement("$1-copy-" + name)
                .setWaitForCompletion(true)
                .setIndices(indices);
    }

    private static void verify(ActionRequestBuilder requestBuilder, boolean fail) {
        verify(requestBuilder, fail, 0);
    }
    
    private static void verify(ActionRequestBuilder requestBuilder, boolean fail, long expectedCount) {
        if (fail) {
            if (requestBuilder instanceof MultiSearchRequestBuilder) {
                MultiSearchResponse multiSearchResponse = ((MultiSearchRequestBuilder) requestBuilder).get();
                assertThat(multiSearchResponse.getResponses().length, equalTo(1));
                assertThat(multiSearchResponse.getResponses()[0].getResponse(), nullValue());
            } else {
                try {
                    requestBuilder.get();
                    fail("IndexMissingException or IndexClosedException was expected");
                } catch (IndexMissingException | IndexClosedException e) {}
            }
        } else {
            if (requestBuilder instanceof SearchRequestBuilder) {
                SearchRequestBuilder searchRequestBuilder = (SearchRequestBuilder) requestBuilder;
                assertHitCount(searchRequestBuilder.get(), expectedCount);
            } else if (requestBuilder instanceof CountRequestBuilder) {
                CountRequestBuilder countRequestBuilder = (CountRequestBuilder) requestBuilder;
                assertHitCount(countRequestBuilder.get(), expectedCount);
            } else if (requestBuilder instanceof MultiSearchRequestBuilder) {
                MultiSearchResponse multiSearchResponse = ((MultiSearchRequestBuilder) requestBuilder).get();
                assertThat(multiSearchResponse.getResponses().length, equalTo(1));
                assertThat(multiSearchResponse.getResponses()[0].getResponse(), notNullValue());
            } else {
                requestBuilder.get();
            }
        }
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequestBuilder;
import org.elasticsearch.action.admin.indices.flush.FlushRequestBuilder;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequestBuilder;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class IndicesOptionsIntegrationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(TestPlugin.class, InternalSettingsPlugin.class);
    }

    public void testSpecifiedIndexUnavailableMultipleIndices() throws Exception {
        assertAcked(prepareCreate("test1"));

        // Verify defaults
        verify(search("test1", "test2"), true);
        verify(msearch(null, "test1", "test2"), true);
        verify(clearCache("test1", "test2"), true);
        verify(_flush("test1", "test2"), true);
        verify(segments("test1", "test2"), true);
        verify(indicesStats("test1", "test2"), true);
        verify(forceMerge("test1", "test2"), true);
        verify(refreshBuilder("test1", "test2"), true);
        verify(validateQuery("test1", "test2"), true);
        verify(getAliases("test1", "test2"), true);
        verify(getFieldMapping("test1", "test2"), true);
        verify(getMapping("test1", "test2"), true);
        verify(getSettings("test1", "test2"), true);

        IndicesOptions options = IndicesOptions.strictExpandOpen();
        verify(search("test1", "test2").setIndicesOptions(options), true);
        verify(msearch(options, "test1", "test2"), true);
        verify(clearCache("test1", "test2").setIndicesOptions(options), true);
        verify(_flush("test1", "test2").setIndicesOptions(options), true);
        verify(segments("test1", "test2").setIndicesOptions(options), true);
        verify(indicesStats("test1", "test2").setIndicesOptions(options), true);
        verify(forceMerge("test1", "test2").setIndicesOptions(options), true);
        verify(refreshBuilder("test1", "test2").setIndicesOptions(options), true);
        verify(validateQuery("test1", "test2").setIndicesOptions(options), true);
        verify(getAliases("test1", "test2").setIndicesOptions(options), true);
        verify(getFieldMapping("test1", "test2").setIndicesOptions(options), true);
        verify(getMapping("test1", "test2").setIndicesOptions(options), true);
        verify(getSettings("test1", "test2").setIndicesOptions(options), true);

        options = IndicesOptions.lenientExpandOpen();
        verify(search("test1", "test2").setIndicesOptions(options), false);
        verify(msearch(options, "test1", "test2").setIndicesOptions(options), false);
        verify(clearCache("test1", "test2").setIndicesOptions(options), false);
        verify(_flush("test1", "test2").setIndicesOptions(options), false);
        verify(segments("test1", "test2").setIndicesOptions(options), false);
        verify(indicesStats("test1", "test2").setIndicesOptions(options), false);
        verify(forceMerge("test1", "test2").setIndicesOptions(options), false);
        verify(refreshBuilder("test1", "test2").setIndicesOptions(options), false);
        verify(validateQuery("test1", "test2").setIndicesOptions(options), false);
        verify(getAliases("test1", "test2").setIndicesOptions(options), false);
        verify(getFieldMapping("test1", "test2").setIndicesOptions(options), false);
        verify(getMapping("test1", "test2").setIndicesOptions(options), false);
        verify(getSettings("test1", "test2").setIndicesOptions(options), false);

        options = IndicesOptions.strictExpandOpen();
        assertAcked(prepareCreate("test2"));
        verify(search("test1", "test2").setIndicesOptions(options), false);
        verify(msearch(options, "test1", "test2").setIndicesOptions(options), false);
        verify(clearCache("test1", "test2").setIndicesOptions(options), false);
        verify(_flush("test1", "test2").setIndicesOptions(options), false);
        verify(segments("test1", "test2").setIndicesOptions(options), false);
        verify(indicesStats("test1", "test2").setIndicesOptions(options), false);
        verify(forceMerge("test1", "test2").setIndicesOptions(options), false);
        verify(refreshBuilder("test1", "test2").setIndicesOptions(options), false);
        verify(validateQuery("test1", "test2").setIndicesOptions(options), false);
        verify(getAliases("test1", "test2").setIndicesOptions(options), false);
        verify(getFieldMapping("test1", "test2").setIndicesOptions(options), false);
        verify(getMapping("test1", "test2").setIndicesOptions(options), false);
        verify(getSettings("test1", "test2").setIndicesOptions(options), false);
    }

    public void testSpecifiedIndexUnavailableSingleIndexThatIsClosed() throws Exception {
        assertAcked(prepareCreate("test1"));
        // we need to wait until all shards are allocated since recovery from
        // gateway will fail unless the majority of the replicas was allocated
        // pre-closing. with lots of replicas this will fail.
        ensureGreen();

        assertAcked(indicesAdmin().prepareClose("test1"));

        IndicesOptions options = IndicesOptions.strictExpandOpenAndForbidClosed();
        verify(search("test1").setIndicesOptions(options), true);
        verify(msearch(options, "test1"), true);
        verify(clearCache("test1").setIndicesOptions(options), true);
        verify(_flush("test1").setIndicesOptions(options), true);
        verify(segments("test1").setIndicesOptions(options), true);
        verify(indicesStats("test1").setIndicesOptions(options), true);
        verify(forceMerge("test1").setIndicesOptions(options), true);
        verify(refreshBuilder("test1").setIndicesOptions(options), true);
        verify(validateQuery("test1").setIndicesOptions(options), true);
        verify(getAliases("test1").setIndicesOptions(options), true);
        verify(getFieldMapping("test1").setIndicesOptions(options), true);
        verify(getMapping("test1").setIndicesOptions(options), true);
        verify(getSettings("test1").setIndicesOptions(options), true);

        options = IndicesOptions.fromOptions(
            true,
            options.allowNoIndices(),
            options.expandWildcardsOpen(),
            options.expandWildcardsClosed(),
            options
        );
        verify(search("test1").setIndicesOptions(options), false);
        verify(msearch(options, "test1"), false);
        verify(clearCache("test1").setIndicesOptions(options), false);
        verify(_flush("test1").setIndicesOptions(options), false);
        verify(segments("test1").setIndicesOptions(options), false);
        verify(indicesStats("test1").setIndicesOptions(options), false);
        verify(forceMerge("test1").setIndicesOptions(options), false);
        verify(refreshBuilder("test1").setIndicesOptions(options), false);
        verify(validateQuery("test1").setIndicesOptions(options), false);
        verify(getAliases("test1").setIndicesOptions(options), false);
        verify(getFieldMapping("test1").setIndicesOptions(options), false);
        verify(getMapping("test1").setIndicesOptions(options), false);
        verify(getSettings("test1").setIndicesOptions(options), false);

        assertAcked(indicesAdmin().prepareOpen("test1"));
        ensureYellow();

        options = IndicesOptions.strictExpandOpenAndForbidClosed();
        verify(search("test1").setIndicesOptions(options), false);
        verify(msearch(options, "test1"), false);
        verify(clearCache("test1").setIndicesOptions(options), false);
        verify(_flush("test1").setIndicesOptions(options), false);
        verify(segments("test1").setIndicesOptions(options), false);
        verify(indicesStats("test1").setIndicesOptions(options), false);
        verify(forceMerge("test1").setIndicesOptions(options), false);
        verify(refreshBuilder("test1").setIndicesOptions(options), false);
        verify(validateQuery("test1").setIndicesOptions(options), false);
        verify(getAliases("test1").setIndicesOptions(options), false);
        verify(getFieldMapping("test1").setIndicesOptions(options), false);
        verify(getMapping("test1").setIndicesOptions(options), false);
        verify(getSettings("test1").setIndicesOptions(options), false);
    }

    public void testSpecifiedIndexUnavailableSingleIndex() throws Exception {
        IndicesOptions options = IndicesOptions.strictExpandOpenAndForbidClosed();
        verify(search("test1").setIndicesOptions(options), true);
        verify(msearch(options, "test1"), true);
        verify(clearCache("test1").setIndicesOptions(options), true);
        verify(_flush("test1").setIndicesOptions(options), true);
        verify(segments("test1").setIndicesOptions(options), true);
        verify(indicesStats("test1").setIndicesOptions(options), true);
        verify(forceMerge("test1").setIndicesOptions(options), true);
        verify(refreshBuilder("test1").setIndicesOptions(options), true);
        verify(validateQuery("test1").setIndicesOptions(options), true);
        verify(getAliases("test1").setIndicesOptions(options), true);
        verify(getFieldMapping("test1").setIndicesOptions(options), true);
        verify(getMapping("test1").setIndicesOptions(options), true);
        verify(getSettings("test1").setIndicesOptions(options), true);

        options = IndicesOptions.fromOptions(
            true,
            options.allowNoIndices(),
            options.expandWildcardsOpen(),
            options.expandWildcardsClosed(),
            options
        );
        verify(search("test1").setIndicesOptions(options), false);
        verify(msearch(options, "test1"), false);
        verify(clearCache("test1").setIndicesOptions(options), false);
        verify(_flush("test1").setIndicesOptions(options), false);
        verify(segments("test1").setIndicesOptions(options), false);
        verify(indicesStats("test1").setIndicesOptions(options), false);
        verify(forceMerge("test1").setIndicesOptions(options), false);
        verify(refreshBuilder("test1").setIndicesOptions(options), false);
        verify(validateQuery("test1").setIndicesOptions(options), false);
        verify(getAliases("test1").setIndicesOptions(options), false);
        verify(getFieldMapping("test1").setIndicesOptions(options), false);
        verify(getMapping("test1").setIndicesOptions(options), false);
        verify(getSettings("test1").setIndicesOptions(options), false);

        assertAcked(prepareCreate("test1"));

        options = IndicesOptions.strictExpandOpenAndForbidClosed();
        verify(search("test1").setIndicesOptions(options), false);
        verify(msearch(options, "test1"), false);
        verify(clearCache("test1").setIndicesOptions(options), false);
        verify(_flush("test1").setIndicesOptions(options), false);
        verify(segments("test1").setIndicesOptions(options), false);
        verify(indicesStats("test1").setIndicesOptions(options), false);
        verify(forceMerge("test1").setIndicesOptions(options), false);
        verify(refreshBuilder("test1").setIndicesOptions(options), false);
        verify(validateQuery("test1").setIndicesOptions(options), false);
        verify(getAliases("test1").setIndicesOptions(options), false);
        verify(getFieldMapping("test1").setIndicesOptions(options), false);
        verify(getMapping("test1").setIndicesOptions(options), false);
        verify(getSettings("test1").setIndicesOptions(options), false);
    }

    public void testSpecifiedIndexUnavailableSnapshotRestore() throws Exception {
        createIndex("test1");
        ensureGreen("test1");
        waitForRelocation();

        AcknowledgedResponse putRepositoryResponse = clusterAdmin().preparePutRepository("dummy-repo")
            .setType("fs")
            .setSettings(Settings.builder().put("location", randomRepoPath()))
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
        clusterAdmin().prepareCreateSnapshot("dummy-repo", "snap1").setWaitForCompletion(true).get();

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
        // TODO: temporary work-around for #5531
        ensureGreen("test2");
        waitForRelocation();
        verify(snapshot("snap3", "test1", "test2").setIndicesOptions(options), false);
        verify(restore("snap3", "test1", "test2").setIndicesOptions(options), false);
    }

    public void testWildcardBehaviour() throws Exception {
        // Verify defaults for wildcards, when specifying no indices (*, _all, /)
        String[] indices = Strings.EMPTY_ARRAY;
        verify(search(indices), false);
        verify(msearch(null, indices), false);
        verify(clearCache(indices), false);
        verify(_flush(indices), false);
        verify(segments(indices), false);
        verify(indicesStats(indices), false);
        verify(forceMerge(indices), false);
        verify(refreshBuilder(indices), false);
        verify(validateQuery(indices), true);
        verify(getAliases(indices), false);
        verify(getFieldMapping(indices), false);
        verify(getMapping(indices), false);
        verify(getSettings(indices), false);

        // Now force allow_no_indices=true
        IndicesOptions options = IndicesOptions.fromOptions(false, true, true, false);
        verify(search(indices).setIndicesOptions(options), false);
        verify(msearch(options, indices).setIndicesOptions(options), false);
        verify(clearCache(indices).setIndicesOptions(options), false);
        verify(_flush(indices).setIndicesOptions(options), false);
        verify(segments(indices).setIndicesOptions(options), false);
        verify(indicesStats(indices).setIndicesOptions(options), false);
        verify(forceMerge(indices).setIndicesOptions(options), false);
        verify(refreshBuilder(indices).setIndicesOptions(options), false);
        verify(validateQuery(indices).setIndicesOptions(options), false);
        verify(getAliases(indices).setIndicesOptions(options), false);
        verify(getFieldMapping(indices).setIndicesOptions(options), false);
        verify(getMapping(indices).setIndicesOptions(options), false);
        verify(getSettings(indices).setIndicesOptions(options), false);

        assertAcked(prepareCreate("foobar"));
        client().prepareIndex("foobar").setId("1").setSource("k", "v").setRefreshPolicy(IMMEDIATE).get();

        // Verify defaults for wildcards, with one wildcard expression and one existing index
        indices = new String[] { "foo*" };
        verify(search(indices), false, 1);
        verify(msearch(null, indices), false, 1);
        verify(clearCache(indices), false);
        verify(_flush(indices), false);
        verify(segments(indices), false);
        verify(indicesStats(indices), false);
        verify(forceMerge(indices), false);
        verify(refreshBuilder(indices), false);
        verify(validateQuery(indices), false);
        verify(getAliases(indices), false);
        verify(getFieldMapping(indices), false);
        verify(getMapping(indices), false);
        verify(getSettings(indices).setIndicesOptions(options), false);

        // Verify defaults for wildcards, with two wildcard expression and one existing index
        indices = new String[] { "foo*", "bar*" };
        verify(search(indices), false, 1);
        verify(msearch(null, indices), false, 1);
        verify(clearCache(indices), false);
        verify(_flush(indices), false);
        verify(segments(indices), false);
        verify(indicesStats(indices), false);
        verify(forceMerge(indices), false);
        verify(refreshBuilder(indices), false);
        verify(validateQuery(indices), true);
        verify(getAliases(indices), false);
        verify(getFieldMapping(indices), false);
        verify(getMapping(indices), false);
        verify(getSettings(indices).setIndicesOptions(options), false);

        // Now force allow_no_indices=true
        options = IndicesOptions.fromOptions(false, true, true, false);
        verify(search(indices).setIndicesOptions(options), false, 1);
        verify(msearch(options, indices).setIndicesOptions(options), false, 1);
        verify(clearCache(indices).setIndicesOptions(options), false);
        verify(_flush(indices).setIndicesOptions(options), false);
        verify(segments(indices).setIndicesOptions(options), false);
        verify(indicesStats(indices).setIndicesOptions(options), false);
        verify(forceMerge(indices).setIndicesOptions(options), false);
        verify(refreshBuilder(indices).setIndicesOptions(options), false);
        verify(validateQuery(indices).setIndicesOptions(options), false);
        verify(getAliases(indices).setIndicesOptions(options), false);
        verify(getFieldMapping(indices).setIndicesOptions(options), false);
        verify(getMapping(indices).setIndicesOptions(options), false);
        verify(getSettings(indices).setIndicesOptions(options), false);
    }

    public void testWildcardBehaviourSnapshotRestore() throws Exception {
        createIndex("foobar");
        ensureGreen("foobar");
        waitForRelocation();

        AcknowledgedResponse putRepositoryResponse = clusterAdmin().preparePutRepository("dummy-repo")
            .setType("fs")
            .setSettings(Settings.builder().put("location", randomRepoPath()))
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
        clusterAdmin().prepareCreateSnapshot("dummy-repo", "snap1").setWaitForCompletion(true).get();

        IndicesOptions options = IndicesOptions.fromOptions(false, false, true, false);
        verify(snapshot("snap2", "foo*", "bar*").setIndicesOptions(options), true);
        verify(restore("snap1", "foo*", "bar*").setIndicesOptions(options), true);

        options = IndicesOptions.strictExpandOpen();
        verify(snapshot("snap2", "foo*", "bar*").setIndicesOptions(options), false);
        verify(restore("snap2", "foo*", "bar*").setIndicesOptions(options), false);

        assertAcked(prepareCreate("barbaz"));
        // TODO: temporary work-around for #5531
        ensureGreen("barbaz");
        waitForRelocation();
        options = IndicesOptions.fromOptions(false, false, true, false);
        verify(snapshot("snap3", "foo*", "bar*").setIndicesOptions(options), false);
        verify(restore("snap3", "foo*", "bar*").setIndicesOptions(options), false);

        options = IndicesOptions.fromOptions(false, false, true, false);
        verify(snapshot("snap4", "foo*", "baz*").setIndicesOptions(options), true);
        verify(restore("snap3", "foo*", "baz*").setIndicesOptions(options), true);
    }

    public void testAllMissingLenient() throws Exception {
        createIndex("test1");
        client().prepareIndex("test1").setId("1").setSource("k", "v").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse response = client().prepareSearch("test2")
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setQuery(matchAllQuery())
            .execute()
            .actionGet();
        assertHitCount(response, 0L);

        response = client().prepareSearch("test2", "test3")
            .setQuery(matchAllQuery())
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .execute()
            .actionGet();
        assertHitCount(response, 0L);

        // you should still be able to run empty searches without things blowing up
        response = client().prepareSearch()
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setQuery(matchAllQuery())
            .execute()
            .actionGet();
        assertHitCount(response, 1L);
    }

    public void testAllMissingStrict() throws Exception {
        createIndex("test1");
        expectThrows(IndexNotFoundException.class, () -> client().prepareSearch("test2").setQuery(matchAllQuery()).execute().actionGet());

        expectThrows(
            IndexNotFoundException.class,
            () -> client().prepareSearch("test2", "test3").setQuery(matchAllQuery()).execute().actionGet()
        );

        // you should still be able to run empty searches without things blowing up
        client().prepareSearch().setQuery(matchAllQuery()).execute().actionGet();
    }

    // For now don't handle closed indices
    public void testCloseApiSpecifiedIndices() throws Exception {
        createIndex("test1", "test2");
        ensureGreen();
        verify(search("test1", "test2"), false);
        assertAcked(indicesAdmin().prepareClose("test2").get());

        verify(search("test1", "test2"), true);

        IndicesOptions options = IndicesOptions.fromOptions(true, true, true, false, IndicesOptions.strictExpandOpenAndForbidClosed());
        verify(search("test1", "test2").setIndicesOptions(options), false);

        verify(search(), false);

        verify(search("t*"), false);
    }

    public void testOpenCloseApiWildcards() throws Exception {
        createIndex("foo", "foobar", "bar", "barbaz");
        ensureGreen();

        // if there are no indices to open/close and allow_no_indices=true (default), the open/close is a no-op
        verify(indicesAdmin().prepareClose("bar*"), false);
        verify(indicesAdmin().prepareClose("bar*"), false);

        verify(indicesAdmin().prepareClose("foo*"), false);
        verify(indicesAdmin().prepareClose("foo*"), false);
        verify(indicesAdmin().prepareClose("_all"), false);

        verify(indicesAdmin().prepareOpen("bar*"), false);
        verify(indicesAdmin().prepareOpen("_all"), false);
        verify(indicesAdmin().prepareOpen("_all"), false);

        // if there are no indices to open/close throw an exception
        IndicesOptions openIndicesOptions = IndicesOptions.fromOptions(false, false, false, true);
        IndicesOptions closeIndicesOptions = IndicesOptions.fromOptions(false, false, true, false);

        verify(indicesAdmin().prepareClose("bar*").setIndicesOptions(closeIndicesOptions), false);
        verify(indicesAdmin().prepareClose("bar*").setIndicesOptions(closeIndicesOptions), true);

        verify(indicesAdmin().prepareClose("foo*").setIndicesOptions(closeIndicesOptions), false);
        verify(indicesAdmin().prepareClose("foo*").setIndicesOptions(closeIndicesOptions), true);
        verify(indicesAdmin().prepareClose("_all").setIndicesOptions(closeIndicesOptions), true);

        verify(indicesAdmin().prepareOpen("bar*").setIndicesOptions(openIndicesOptions), false);
        verify(indicesAdmin().prepareOpen("_all").setIndicesOptions(openIndicesOptions), false);
        verify(indicesAdmin().prepareOpen("_all").setIndicesOptions(openIndicesOptions), true);
    }

    public void testDeleteIndex() throws Exception {
        createIndex("foobar");

        verify(indicesAdmin().prepareDelete("foo"), true);
        assertThat(indexExists("foobar"), equalTo(true));
        verify(indicesAdmin().prepareDelete("foobar"), false);
        assertThat(indexExists("foobar"), equalTo(false));
    }

    public void testDeleteIndexWildcard() throws Exception {
        verify(indicesAdmin().prepareDelete("_all"), false);

        createIndex("foo", "foobar", "bar", "barbaz");

        verify(indicesAdmin().prepareDelete("foo*"), false);
        assertThat(indexExists("foobar"), equalTo(false));
        assertThat(indexExists("bar"), equalTo(true));
        assertThat(indexExists("barbaz"), equalTo(true));

        verify(indicesAdmin().prepareDelete("foo*"), false);

        verify(indicesAdmin().prepareDelete("_all"), false);
        assertThat(indexExists("foo"), equalTo(false));
        assertThat(indexExists("foobar"), equalTo(false));
        assertThat(indexExists("bar"), equalTo(false));
        assertThat(indexExists("barbaz"), equalTo(false));
    }

    public void testPutAlias() throws Exception {
        createIndex("foobar");
        verify(indicesAdmin().prepareAliases().addAlias("foobar", "foobar_alias"), false);
        assertFalse(indicesAdmin().prepareGetAliases("foobar_alias").setIndices("foobar").get().getAliases().isEmpty());

    }

    public void testPutAliasWildcard() throws Exception {
        createIndex("foo", "foobar", "bar", "barbaz");

        verify(indicesAdmin().prepareAliases().addAlias("foo*", "foobar_alias"), false);
        assertFalse(indicesAdmin().prepareGetAliases("foobar_alias").setIndices("foo").get().getAliases().isEmpty());
        assertFalse(indicesAdmin().prepareGetAliases("foobar_alias").setIndices("foobar").get().getAliases().isEmpty());
        assertTrue(indicesAdmin().prepareGetAliases("foobar_alias").setIndices("bar").get().getAliases().isEmpty());
        assertTrue(indicesAdmin().prepareGetAliases("foobar_alias").setIndices("barbaz").get().getAliases().isEmpty());

        verify(indicesAdmin().prepareAliases().addAlias("*", "foobar_alias"), false);
        assertFalse(indicesAdmin().prepareGetAliases("foobar_alias").setIndices("foo").get().getAliases().isEmpty());
        assertFalse(indicesAdmin().prepareGetAliases("foobar_alias").setIndices("foobar").get().getAliases().isEmpty());
        assertFalse(indicesAdmin().prepareGetAliases("foobar_alias").setIndices("bar").get().getAliases().isEmpty());
        assertFalse(indicesAdmin().prepareGetAliases("foobar_alias").setIndices("barbaz").get().getAliases().isEmpty());

    }

    public void testPutMapping() throws Exception {
        verify(indicesAdmin().preparePutMapping("foo").setSource("field", "type=text"), true);
        verify(indicesAdmin().preparePutMapping("_all").setSource("field", "type=text"), true);

        for (String index : Arrays.asList("foo", "foobar", "bar", "barbaz")) {
            assertAcked(prepareCreate(index));
        }

        verify(indicesAdmin().preparePutMapping("foo").setSource("field", "type=text"), false);
        assertThat(indicesAdmin().prepareGetMappings("foo").get().mappings().get("foo"), notNullValue());
        verify(indicesAdmin().preparePutMapping("b*").setSource("field", "type=text"), false);
        assertThat(indicesAdmin().prepareGetMappings("bar").get().mappings().get("bar"), notNullValue());
        assertThat(indicesAdmin().prepareGetMappings("barbaz").get().mappings().get("barbaz"), notNullValue());
        verify(indicesAdmin().preparePutMapping("_all").setSource("field", "type=text"), false);
        assertThat(indicesAdmin().prepareGetMappings("foo").get().mappings().get("foo"), notNullValue());
        assertThat(indicesAdmin().prepareGetMappings("foobar").get().mappings().get("foobar"), notNullValue());
        assertThat(indicesAdmin().prepareGetMappings("bar").get().mappings().get("bar"), notNullValue());
        assertThat(indicesAdmin().prepareGetMappings("barbaz").get().mappings().get("barbaz"), notNullValue());
        verify(indicesAdmin().preparePutMapping().setSource("field", "type=text"), false);
        assertThat(indicesAdmin().prepareGetMappings("foo").get().mappings().get("foo"), notNullValue());
        assertThat(indicesAdmin().prepareGetMappings("foobar").get().mappings().get("foobar"), notNullValue());
        assertThat(indicesAdmin().prepareGetMappings("bar").get().mappings().get("bar"), notNullValue());
        assertThat(indicesAdmin().prepareGetMappings("barbaz").get().mappings().get("barbaz"), notNullValue());

        verify(indicesAdmin().preparePutMapping("c*").setSource("field", "type=text"), true);

        assertAcked(indicesAdmin().prepareClose("barbaz").get());
        verify(indicesAdmin().preparePutMapping("barbaz").setSource("field", "type=text"), false);
        assertThat(indicesAdmin().prepareGetMappings("barbaz").get().mappings().get("barbaz"), notNullValue());
    }

    public static final class TestPlugin extends Plugin {

        private static final Setting<String> INDEX_A = new Setting<>(
            "index.a",
            "",
            Function.identity(),
            Property.Dynamic,
            Property.IndexScope
        );
        private static final Setting<String> INDEX_C = new Setting<>(
            "index.c",
            "",
            Function.identity(),
            Property.Dynamic,
            Property.IndexScope
        );
        private static final Setting<String> INDEX_E = new Setting<>("index.e", "", Function.identity(), Property.IndexScope);

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(INDEX_A, INDEX_C, INDEX_E);
        }
    }

    public void testUpdateSettings() throws Exception {
        verify(indicesAdmin().prepareUpdateSettings("foo").setSettings(Settings.builder().put("a", "b")), true);
        verify(indicesAdmin().prepareUpdateSettings("_all").setSettings(Settings.builder().put("a", "b")), true);

        createIndex("foo", "foobar", "bar", "barbaz");
        ensureGreen();
        assertAcked(indicesAdmin().prepareClose("_all").get());

        verify(indicesAdmin().prepareUpdateSettings("foo").setSettings(Settings.builder().put("a", "b")), false);
        verify(indicesAdmin().prepareUpdateSettings("bar*").setSettings(Settings.builder().put("a", "b")), false);
        verify(indicesAdmin().prepareUpdateSettings("_all").setSettings(Settings.builder().put("c", "d")), false);

        GetSettingsResponse settingsResponse = indicesAdmin().prepareGetSettings("foo").get();
        assertThat(settingsResponse.getSetting("foo", "index.a"), equalTo("b"));
        settingsResponse = indicesAdmin().prepareGetSettings("bar*").get();
        assertThat(settingsResponse.getSetting("bar", "index.a"), equalTo("b"));
        assertThat(settingsResponse.getSetting("barbaz", "index.a"), equalTo("b"));
        settingsResponse = indicesAdmin().prepareGetSettings("_all").get();
        assertThat(settingsResponse.getSetting("foo", "index.c"), equalTo("d"));
        assertThat(settingsResponse.getSetting("foobar", "index.c"), equalTo("d"));
        assertThat(settingsResponse.getSetting("bar", "index.c"), equalTo("d"));
        assertThat(settingsResponse.getSetting("barbaz", "index.c"), equalTo("d"));

        assertAcked(indicesAdmin().prepareOpen("_all").get());
        try {
            verify(indicesAdmin().prepareUpdateSettings("barbaz").setSettings(Settings.builder().put("e", "f")), false);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), startsWith("Can't update non dynamic settings [[index.e]] for open indices [[barbaz"));
        }
        verify(indicesAdmin().prepareUpdateSettings("baz*").setSettings(Settings.builder().put("a", "b")), true);
    }

    static SearchRequestBuilder search(String... indices) {
        return client().prepareSearch(indices).setQuery(matchAllQuery());
    }

    static MultiSearchRequestBuilder msearch(IndicesOptions options, String... indices) {
        MultiSearchRequestBuilder multiSearchRequestBuilder = client().prepareMultiSearch();
        if (options != null) {
            multiSearchRequestBuilder.setIndicesOptions(options);
        }
        return multiSearchRequestBuilder.add(client().prepareSearch(indices).setQuery(matchAllQuery()));
    }

    static ClearIndicesCacheRequestBuilder clearCache(String... indices) {
        return indicesAdmin().prepareClearCache(indices);
    }

    static FlushRequestBuilder _flush(String... indices) {
        return indicesAdmin().prepareFlush(indices);
    }

    static IndicesSegmentsRequestBuilder segments(String... indices) {
        return indicesAdmin().prepareSegments(indices);
    }

    static IndicesStatsRequestBuilder indicesStats(String... indices) {
        return indicesAdmin().prepareStats(indices);
    }

    static ForceMergeRequestBuilder forceMerge(String... indices) {
        return indicesAdmin().prepareForceMerge(indices);
    }

    static RefreshRequestBuilder refreshBuilder(String... indices) {
        return indicesAdmin().prepareRefresh(indices);
    }

    static ValidateQueryRequestBuilder validateQuery(String... indices) {
        return indicesAdmin().prepareValidateQuery(indices);
    }

    static GetAliasesRequestBuilder getAliases(String... indices) {
        return indicesAdmin().prepareGetAliases("dummy").addIndices(indices);
    }

    static GetFieldMappingsRequestBuilder getFieldMapping(String... indices) {
        return indicesAdmin().prepareGetFieldMappings(indices);
    }

    static GetMappingsRequestBuilder getMapping(String... indices) {
        return indicesAdmin().prepareGetMappings(indices);
    }

    static GetSettingsRequestBuilder getSettings(String... indices) {
        return indicesAdmin().prepareGetSettings(indices);
    }

    private static CreateSnapshotRequestBuilder snapshot(String name, String... indices) {
        return clusterAdmin().prepareCreateSnapshot("dummy-repo", name).setWaitForCompletion(true).setIndices(indices);
    }

    private static RestoreSnapshotRequestBuilder restore(String name, String... indices) {
        return clusterAdmin().prepareRestoreSnapshot("dummy-repo", name)
            .setRenamePattern("(.+)")
            .setRenameReplacement("$1-copy-" + name)
            .setWaitForCompletion(true)
            .setIndices(indices);
    }

    private static void verify(ActionRequestBuilder<?, ?> requestBuilder, boolean fail) {
        verify(requestBuilder, fail, 0);
    }

    private static void verify(ActionRequestBuilder<?, ?> requestBuilder, boolean fail, long expectedCount) {
        if (fail) {
            if (requestBuilder instanceof MultiSearchRequestBuilder multiSearchRequestBuilder) {
                MultiSearchResponse multiSearchResponse = multiSearchRequestBuilder.get();
                assertThat(multiSearchResponse.getResponses().length, equalTo(1));
                assertThat(multiSearchResponse.getResponses()[0].isFailure(), is(true));
                assertThat(multiSearchResponse.getResponses()[0].getResponse(), nullValue());
            } else {
                try {
                    requestBuilder.get();
                    fail("IndexNotFoundException or IndexClosedException was expected");
                } catch (IndexNotFoundException | IndexClosedException e) {}
            }
        } else {
            if (requestBuilder instanceof SearchRequestBuilder searchRequestBuilder) {
                assertHitCount(searchRequestBuilder.get(), expectedCount);
            } else if (requestBuilder instanceof MultiSearchRequestBuilder multiSearchRequestBuilder) {
                MultiSearchResponse multiSearchResponse = multiSearchRequestBuilder.get();
                assertThat(multiSearchResponse.getResponses().length, equalTo(1));
                assertThat(multiSearchResponse.getResponses()[0].getResponse(), notNullValue());
            } else {
                requestBuilder.get();
            }
        }
    }
}

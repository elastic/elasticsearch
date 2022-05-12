/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.reindex.AbstractAsyncBulkByScrollActionTestCase;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.xcontent.XContentType;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Reindex
 */
public class ReindexIdTests extends AbstractAsyncBulkByScrollActionTestCase<ReindexRequest, BulkByScrollResponse> {
    public void testEmptyStateCopiesId() throws Exception {
        assertThat(action(ClusterState.EMPTY_STATE).buildRequest(doc()).getId(), equalTo(doc().getId()));
    }

    public void testStandardIndexCopiesId() throws Exception {
        Settings.Builder settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.STANDARD);
        assertThat(action(stateWithIndex(settings)).buildRequest(doc()).getId(), equalTo(doc().getId()));
    }

    public void testTsdbIndexClearsId() throws Exception {
        Settings.Builder settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo");
        assertThat(action(stateWithIndex(settings)).buildRequest(doc()).getId(), nullValue());
    }

    private ClusterState stateWithIndex(Settings.Builder settings) {
        IndexMetadata.Builder meta = IndexMetadata.builder(request().getDestination().index())
            .settings(settings.put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfReplicas(0)
            .numberOfShards(1);
        return ClusterState.builder(ClusterState.EMPTY_STATE).metadata(Metadata.builder(Metadata.EMPTY_METADATA).put(meta)).build();
    }

    private ScrollableHitSource.BasicHit doc() {
        return new ScrollableHitSource.BasicHit("index", "id", -1).setSource(new BytesArray("{}"), XContentType.JSON);
    }

    @Override
    protected ReindexRequest request() {
        return new ReindexRequest().setDestIndex("dest_index");
    }

    private Reindexer.AsyncIndexBySearchAction action(ClusterState state) {
        return new Reindexer.AsyncIndexBySearchAction(task, logger, null, null, threadPool, null, state, null, request(), listener());
    }
}

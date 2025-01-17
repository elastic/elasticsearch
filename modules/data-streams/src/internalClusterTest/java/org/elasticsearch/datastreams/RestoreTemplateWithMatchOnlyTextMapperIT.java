/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

public class RestoreTemplateWithMatchOnlyTextMapperIT extends AbstractSnapshotIntegTestCase {
    public static final String REPO = "repo";
    public static final String SNAPSHOT = "snap";
    private Client client;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockRepository.Plugin.class, MapperExtrasPlugin.class);
    }

    @Before
    public void setup() {
        client = client();
        Path location = randomRepoPath();
        createRepository(REPO, "fs", location);
    }

    public void test() throws IOException {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request("t1");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("test-index-*"))
                .template(new Template(null, CompressedXContent.fromJSON("""
                    {
                         "_doc": {
                           "properties": {
                             "@timestamp": {
                               "format": "date_optional_time||epoch_millis",
                               "type": "date"
                             },
                             "flag": {
                               "type": "boolean"
                             },
                             "message": {
                               "type": "match_only_text"
                             }
                           }
                         }
                    }"""), null))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();

        final String snapshot = "test-snapshot";
        final String indexWithoutDataStream = "test-idx-no-ds";
        createIndexWithContent(indexWithoutDataStream);
        createFullSnapshot(REPO, snapshot);
        assertAcked(client.admin().indices().prepareDelete(indexWithoutDataStream));
        RestoreInfo restoreInfo = client.admin()
            .cluster()
            .prepareRestoreSnapshot(TimeValue.THIRTY_SECONDS, REPO, snapshot)
            .setIndices(indexWithoutDataStream)
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .get()
            .getRestoreInfo();
        assertThat(restoreInfo.failedShards(), is(0));
        assertThat(restoreInfo.successfulShards(), is(1));
    }
}

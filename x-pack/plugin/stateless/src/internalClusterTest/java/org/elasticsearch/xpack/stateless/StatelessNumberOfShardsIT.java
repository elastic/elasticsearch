/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.serverless.constants.ProjectType;

import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.PROJECT_TYPE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

public class StatelessNumberOfShardsIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), DataStreamsPlugin.class);
    }

    public void testNumberOfShards() throws Exception {
        final var projectType = randomFrom(ProjectType.values());
        logger.info("--> testing with project type [{}] and default number of shards [{}]", projectType, projectType.getNumberOfShards());

        startMasterAndIndexNode(Settings.builder().put(PROJECT_TYPE.getKey(), projectType).build());
        startSearchNode();

        // Regular indices - default number of shards affected by project type
        final String indexName = randomIdentifier();
        assertAcked(indicesAdmin().prepareCreate(indexName));
        assertNumberOfShards(indexName, projectType.getNumberOfShards());

        // Regular indices - explicitly configuring number of shards overrides the default
        final String anotherIndexName = randomIdentifier();
        final int anotherNumberOfShards = randomValueOtherThan(projectType.getNumberOfShards(), () -> between(1, 5));
        assertAcked(
            indicesAdmin().prepareCreate(anotherIndexName)
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, anotherNumberOfShards))
        );
        assertNumberOfShards(anotherIndexName, anotherNumberOfShards);

        // System index - default number of shards not affected by project type and defaults to 1
        createSystemIndex(Settings.EMPTY);
        assertNumberOfShards(SYSTEM_INDEX_NAME, 1);

        // Data streams - default number of shards not affected by project type and defaults to 1
        final String dataStreamName = "data-stream-" + randomIdentifier();
        final String composableTemplateName = "composable-template-" + randomIdentifier();
        final var request = new TransportPutComposableIndexTemplateAction.Request(composableTemplateName);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStreamName + "*"))
                .template(new Template(indexSettings(1, 1).build(), CompressedXContent.fromJSON("""
                    {
                      "properties": {
                        "@timestamp": {
                          "type": "date",
                          "format": "date_optional_time||epoch_millis"
                        },
                        "message": {
                          "type": "text"
                        }
                      }
                    }"""), null, null))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));

        final var createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest));

        assertResponse(
            client().execute(GetDataStreamAction.INSTANCE, new GetDataStreamAction.Request(new String[] { dataStreamName })),
            response -> {
                final String backingIndexName = response.getDataStreams()
                    .iterator()
                    .next()
                    .getDataStream()
                    .getIndices()
                    .iterator()
                    .next()
                    .getName();

                assertNumberOfShards(backingIndexName, 1);
            }
        );
    }

    private void assertNumberOfShards(String indexName, int expectedNumberOfShards) {
        assertResponse(
            indicesAdmin().prepareGetSettings(indexName),
            response -> assertThat(
                Integer.valueOf(response.getSetting(indexName, SETTING_NUMBER_OF_SHARDS)),
                equalTo(expectedNumberOfShards)
            )
        );
    }
}

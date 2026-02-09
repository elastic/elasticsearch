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

package org.elasticsearch.xpack.stateless.cluster.metadata;

import co.elastic.elasticsearch.serverless.constants.ProjectType;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.IndexLimitExceededException;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings.PROJECT_TYPE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public class CreateIndexLimitIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DataStreamsPlugin.class);
        plugins.add(TestPlugin.class);
        return plugins;
    }

    public static class TestPlugin extends Plugin implements SystemIndexPlugin {
        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            // In additional to ".sys-idx" provided by SystemIndexTestPlugin.class.
            return List.of(
                SystemIndexDescriptorUtils.createUnmanaged(".my-elasticsearch-system-*", "my elasticsearch system"),
                SystemIndexDescriptorUtils.createUnmanaged(".test-index-limit-*", ".test index limit"),
                SystemIndexDescriptorUtils.createUnmanaged(".system-index-exempted-*", "system index exempted")
            );
        }

        @Override
        public Collection<SystemDataStreamDescriptor> getSystemDataStreamDescriptors() {
            try {
                CompressedXContent mappings = new CompressedXContent("{\"properties\":{\"name\":{\"type\":\"keyword\"}}}");
                return List.of(
                    new SystemDataStreamDescriptor(
                        ".my-elasticsearch-data-stream",
                        "system data stream test",
                        SystemDataStreamDescriptor.Type.EXTERNAL,
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of(".my-test-limit-data-stream"))
                            .template(new Template(Settings.EMPTY, mappings, null))
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                            .build(),
                        Map.of(),
                        List.of("product"),
                        "product",
                        ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
                    ),
                    new SystemDataStreamDescriptor(
                        ".test-limit-index-data-stream",
                        "system data stream test limit",
                        SystemDataStreamDescriptor.Type.EXTERNAL,
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of(".test-limit-index-data-stream"))
                            .template(new Template(Settings.EMPTY, mappings, null))
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                            .build(),
                        Map.of(),
                        List.of("product"),
                        "product",
                        ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
                    ),
                    new SystemDataStreamDescriptor(
                        ".test-system-index-exempted",
                        "system data stream test exempted",
                        SystemDataStreamDescriptor.Type.EXTERNAL,
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of(".test-system-index-exempted"))
                            .template(new Template(Settings.EMPTY, mappings, null))
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                            .build(),
                        Map.of(),
                        List.of("product"),
                        "product",
                        ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
                    )
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String getFeatureName() {
            return "Test-User-Indices-Limit";
        }

        @Override
        public String getFeatureDescription() {
            return "Test User Indices Limit";
        }

        @Override
        public void cleanUpFeature(
            ClusterService clusterService,
            ProjectResolver projectResolver,
            Client client,
            TimeValue masterNodeTimeout,
            ActionListener<ResetFeatureStateResponse.ResetFeatureStateStatus> listener
        ) {
            Collection<SystemDataStreamDescriptor> dataStreamDescriptors = getSystemDataStreamDescriptors();
            final DeleteDataStreamAction.Request request = new DeleteDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                dataStreamDescriptors.stream()
                    .map(SystemDataStreamDescriptor::getDataStreamName)
                    .collect(Collectors.toList())
                    .toArray(Strings.EMPTY_ARRAY)
            );
            request.indicesOptions(
                IndicesOptions.builder(request.indicesOptions())
                    .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
                    .build()
            );
            try {
                client.execute(
                    DeleteDataStreamAction.INSTANCE,
                    request,
                    ActionListener.wrap(
                        response -> SystemIndexPlugin.super.cleanUpFeature(
                            clusterService,
                            projectResolver,
                            client,
                            masterNodeTimeout,
                            listener
                        ),
                        e -> {
                            Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
                            if (unwrapped instanceof ResourceNotFoundException) {
                                SystemIndexPlugin.super.cleanUpFeature(
                                    clusterService,
                                    projectResolver,
                                    client,
                                    masterNodeTimeout,
                                    listener
                                );
                            } else {
                                listener.onFailure(e);
                            }
                        }
                    )
                );
            } catch (Exception e) {
                Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
                if (unwrapped instanceof ResourceNotFoundException) {
                    SystemIndexPlugin.super.cleanUpFeature(clusterService, projectResolver, client, masterNodeTimeout, listener);
                } else {
                    listener.onFailure(e);
                }
            }
        }
    }

    private void testCreateIndex(String indexPattern, String suffix, boolean expectedSuccess) throws IOException {
        var request = new TransportPutComposableIndexTemplateAction.Request("template-" + indexPattern);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(indexPattern + "*"))
                .template(Template.builder().settings(indexSettings(1, 1)).mappings(CompressedXContent.fromJSON("""
                    { "properties": { "field": { "type": "keyword" } } }
                    """)))
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));

        if (expectedSuccess) {
            createIndex(indexPattern + suffix);
        } else {
            final IndexLimitExceededException e = ESTestCase.expectThrows(
                IndexLimitExceededException.class,
                prepareCreate(indexPattern + suffix)
            );
            assertThat(
                e.getMessage(),
                containsString(
                    "see https://www.elastic.co/docs/deploy-manage/production-guidance/"
                        + "optimize-performance/index-count-limit?version=master"
                )
            );
        }
    }

    private void verifySystemIndicesBackingDataStreams(String dataStream) {
        CreateDataStreamAction.Request createRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStream
        );
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createRequest));

        safeGet(
            client().prepareIndex(dataStream)
                .setOpType(DocWriteRequest.OpType.CREATE)
                .setSource("@timestamp", "2099-03-08T11:06:07.000Z", "name", "my-name")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .execute()
        );
    }

    public void testCreateIndexLimit() throws Exception {
        final var projectType = randomFrom(ProjectType.values());
        startMasterAndIndexNode(Settings.builder().put(PROJECT_TYPE.getKey(), projectType).build());
        startSearchNode(Settings.builder().put(PROJECT_TYPE.getKey(), projectType).build());
        ensureStableCluster(2);

        final String indexName = randomIndexName();
        int userIndicesLimit = randomIntBetween(3, 5);
        final Set<String> systemIndexAndDataStreamsPatterns = Set.of(
            ".my-elasticsearch-system-",
            ".test-index-limit-",
            ".system-index-exempted-",
            ".sys-idx"
        );

        AtomicInteger systemIndicesCounter = new AtomicInteger(0);
        updateClusterSettings(
            Settings.builder()
                .put(MetadataCreateIndexService.CLUSTER_MAX_INDICES_PER_PROJECT_ENABLED_SETTING.getKey(), true)
                .put(MetadataCreateIndexService.CLUSTER_MAX_INDICES_PER_PROJECT_SETTING.getKey(), userIndicesLimit)
        );

        // Case 1: System indices are exempted from index count limit constraint.
        var systemIndices = userIndicesLimit + randomIntBetween(1, 2);
        for (int i = 0; i < systemIndices; i++) {
            testCreateIndex(randomFrom(systemIndexAndDataStreamsPatterns), String.valueOf(i), true);
        }
        systemIndicesCounter.addAndGet(systemIndices);

        // Verify system indices backing data streams creation proceed unimpeded.
        verifySystemIndicesBackingDataStreams(".my-elasticsearch-data-stream");

        // Case 2: Within index limit, all user index creations should succeed.
        for (int i = 0; i < userIndicesLimit; i++) {
            testCreateIndex(indexName, "-" + i, true);
        }

        // Case 3: all subsequent attempts should fail.
        var failedAttempts = randomIntBetween(1, 2);
        for (int i = 0; i < failedAttempts; i++) {
            var suffix = userIndicesLimit + i;
            testCreateIndex(indexName, "-" + suffix, false);
        }

        var suffix = systemIndicesCounter.addAndGet(1);

        // Case 4: System indices is never unaffected.
        var someMoreSystemIndices = randomIntBetween(2, 3);
        for (int i = 0; i < someMoreSystemIndices; i++) {
            suffix++;
            testCreateIndex(randomFrom(systemIndexAndDataStreamsPatterns), String.valueOf(suffix), true);
        }

        // Verify system data stream is unaffected.
        verifySystemIndicesBackingDataStreams(".test-limit-index-data-stream");
        verifySystemIndicesBackingDataStreams(".test-system-index-exempted");

        var increase = randomIntBetween(5, 10);
        updateClusterSettings(
            Settings.builder().put(MetadataCreateIndexService.CLUSTER_MAX_INDICES_PER_PROJECT_SETTING.getKey(), userIndicesLimit + increase)
        );

        // After increase, further user indices can be created.
        for (int i = 0; i < increase; i++) {
            testCreateIndex(indexName, "-" + (userIndicesLimit + i), true);
        }
        ensureGreen();
    }
}

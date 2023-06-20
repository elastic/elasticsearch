/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataLifecycle;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.dlm.DataLifecycleErrorStore;
import org.elasticsearch.dlm.DataLifecyclePlugin;
import org.elasticsearch.dlm.DataLifecycleService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.security.LocalStateSecurity;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * This test suite ensures that DLM runtime tasks work correctly with security enabled, i.e., that the internal user for DLM has all
 * requisite privileges to orchestrate DLM
 */
public class DataLifecycleServiceRuntimeSecurityIT extends SecurityIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataLifecyclePlugin.class, LocalStateSecurity.class, DataStreamsPlugin.class, SystemDataStreamTestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(DataLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, "1s");
        settings.put(DataLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=1,max_docs=1");
        return settings.build();
    }

    public void testRolloverLifecycleAndForceMergeAuthorized() throws Exception {
        String dataStreamName = randomDataStreamName();
        // empty lifecycle contains the default rollover
        prepareDataStreamAndIndex(dataStreamName, new DataLifecycle());

        assertBusy(() -> {
            assertNoAuthzErrors();
            List<Index> backingIndices = getDataStreamBackingIndices(dataStreamName);
            assertThat(backingIndices.size(), equalTo(2));
            String backingIndex = backingIndices.get(0).getName();
            assertThat(backingIndex, backingIndexEqualTo(dataStreamName, 1));
            String writeIndex = backingIndices.get(1).getName();
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
        });
        // Index another doc to force another rollover and trigger an attempted force-merge. The force-merge may be a noop under
        // the hood but for authz purposes this doesn't matter, it only matters that the force-merge API was called
        indexDoc(dataStreamName);
        assertBusy(() -> {
            assertNoAuthzErrors();
            List<Index> backingIndices = getDataStreamBackingIndices(dataStreamName);
            assertThat(backingIndices.size(), equalTo(3));
        });
    }

    public void testRolloverAndRetentionAuthorized() throws Exception {
        String dataStreamName = randomDataStreamName();
        prepareDataStreamAndIndex(dataStreamName, new DataLifecycle(TimeValue.timeValueMillis(0)));

        assertBusy(() -> {
            assertNoAuthzErrors();
            List<Index> backingIndices = getDataStreamBackingIndices(dataStreamName);
            assertThat(backingIndices.size(), equalTo(1));
            // we expect the data stream to have only one backing index, the write one, with generation 2
            // as generation 1 would've been deleted by DLM given the lifecycle configuration
            String writeIndex = backingIndices.get(0).getName();
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
        });
    }

    public void testUnauthorized() throws Exception {
        // this is an example index pattern for a system index that DLM does not have access for. DLM will therefore fail at runtime with an
        // authz exception
        prepareDataStreamAndIndex(SECURITY_MAIN_ALIAS, new DataLifecycle());

        assertBusy(() -> {
            Map<String, String> indicesAndErrors = collectErrorsFromStoreAsMap();
            assertThat(indicesAndErrors, is(not(anEmptyMap())));
            assertThat(
                indicesAndErrors.values(),
                hasItem(allOf(containsString("security_exception"), containsString("unauthorized for user [_dlm]")))
            );
        });
    }

    public void testRolloverAndRetentionWithSystemDataStreamAuthorized() throws Exception {
        String dataStreamName = SystemDataStreamTestPlugin.SYSTEM_DATA_STREAM_NAME;
        indexDoc(dataStreamName);

        assertBusy(() -> {
            assertNoAuthzErrors();
            List<Index> backingIndices = getDataStreamBackingIndices(dataStreamName);
            assertThat(backingIndices.size(), equalTo(1));
            // we expect the data stream to have only one backing index, the write one, with generation 2
            // as generation 1 would've been deleted by DLM given the lifecycle configuration
            String writeIndex = backingIndices.get(0).getName();
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
        });
    }

    private static String randomDataStreamName() {
        // lower-case since this is required for a valid data stream name
        return randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT);
    }

    private Map<String, String> collectErrorsFromStoreAsMap() {
        Iterable<DataLifecycleService> lifecycleServices = internalCluster().getInstances(DataLifecycleService.class);
        Map<String, String> indicesAndErrors = new HashMap<>();
        for (DataLifecycleService lifecycleService : lifecycleServices) {
            DataLifecycleErrorStore errorStore = lifecycleService.getErrorStore();
            List<String> allIndices = errorStore.getAllIndices();
            for (var index : allIndices) {
                indicesAndErrors.put(index, errorStore.getError(index));
            }
        }
        return indicesAndErrors;
    }

    private void prepareDataStreamAndIndex(String dataStreamName, DataLifecycle lifecycle) throws IOException, InterruptedException,
        ExecutionException {
        putComposableIndexTemplate("id1", null, List.of(dataStreamName + "*"), null, null, lifecycle);
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        indexDoc(dataStreamName);
    }

    private List<Index> getDataStreamBackingIndices(String dataStreamName) {
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
        GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
        return getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
    }

    private void assertNoAuthzErrors() {
        var indicesAndErrors = collectErrorsFromStoreAsMap();
        for (var entry : indicesAndErrors.entrySet()) {
            assertThat(
                "unexpected authz error for index [" + entry.getKey() + "] with error message [" + entry.getValue() + "]",
                entry.getValue(),
                not(anyOf(containsString("security_exception"), containsString("unauthorized for user [_dlm]")))
            );
        }
    }

    private static void putComposableIndexTemplate(
        String id,
        @Nullable String mappings,
        List<String> patterns,
        @Nullable Settings settings,
        @Nullable Map<String, Object> metadata,
        @Nullable DataLifecycle lifecycle
    ) throws IOException {
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            new ComposableIndexTemplate(
                patterns,
                new Template(settings, mappings == null ? null : CompressedXContent.fromJSON(mappings), null, lifecycle),
                null,
                null,
                null,
                metadata,
                new ComposableIndexTemplate.DataStreamTemplate(),
                null
            )
        );
        client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();
    }

    private static void indexDoc(String dataStream) {
        BulkRequest bulkRequest = new BulkRequest();
        String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
        bulkRequest.add(
            new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
                .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
        );
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(1));
        String backingIndexPrefix = DataStream.BACKING_INDEX_PREFIX + dataStream;
        for (BulkItemResponse itemResponse : bulkResponse) {
            assertThat(itemResponse.getFailureMessage(), nullValue());
            assertThat(itemResponse.status(), equalTo(RestStatus.CREATED));
            assertThat(itemResponse.getIndex(), startsWith(backingIndexPrefix));
        }
        indicesAdmin().refresh(new RefreshRequest(dataStream)).actionGet();
    }

    public static class SystemDataStreamTestPlugin extends Plugin implements SystemIndexPlugin {

        static final String SYSTEM_DATA_STREAM_NAME = ".fleet-actions-results";

        @Override
        public Collection<SystemDataStreamDescriptor> getSystemDataStreamDescriptors() {
            return List.of(
                new SystemDataStreamDescriptor(
                    SYSTEM_DATA_STREAM_NAME,
                    "a system data stream for testing",
                    SystemDataStreamDescriptor.Type.EXTERNAL,
                    new ComposableIndexTemplate(
                        List.of(SYSTEM_DATA_STREAM_NAME),
                        new Template(Settings.EMPTY, null, null, new DataLifecycle(0)),
                        null,
                        null,
                        null,
                        null,
                        new ComposableIndexTemplate.DataStreamTemplate()
                    ),
                    Map.of(),
                    Collections.singletonList("test"),
                    new ExecutorNames(ThreadPool.Names.SYSTEM_CRITICAL_READ, ThreadPool.Names.SYSTEM_READ, ThreadPool.Names.SYSTEM_WRITE)
                )
            );
        }

        @Override
        public String getFeatureName() {
            return SystemDataStreamTestPlugin.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "A plugin for testing DLM runtime actions on system data streams";
        }
    }
}

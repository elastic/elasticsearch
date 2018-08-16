/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class DatafeedConfigProviderIT extends MlSingleNodeTestCase {

    private DatafeedConfigProvider datafeedConfigProvider;

    @Before
    public void createComponents() throws Exception {
        datafeedConfigProvider = new DatafeedConfigProvider(client(), Settings.EMPTY, xContentRegistry());
        waitForMlTemplates();
    }

    public void testCrud() throws InterruptedException {
        String datafeedId = "df1";

        AtomicReference<IndexResponse> indexResponseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        // Create datafeed config
        DatafeedConfig config = createDatafeedConfig(datafeedId, "j1");
        blockingCall(actionListener -> datafeedConfigProvider.putDatafeedConfig(config, actionListener),
                indexResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertEquals(RestStatus.CREATED, indexResponseHolder.get().status());

        // Read datafeed config
        AtomicReference<DatafeedConfig.Builder> configBuilderHolder = new AtomicReference<>();
        blockingCall(actionListener -> datafeedConfigProvider.getDatafeedConfig(datafeedId, actionListener),
                configBuilderHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertEquals(config, configBuilderHolder.get().build());

        // Update
        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeedId);
        List<String> updateIndices = Collections.singletonList("a-different-index");
        update.setIndices(updateIndices);
        Map<String, String> updateHeaders = new HashMap<>();
        // Only security headers are updated, grab the first one
        String securityHeader = ClientHelper.SECURITY_HEADER_FILTERS.iterator().next();
        updateHeaders.put(securityHeader, "CHANGED");

        AtomicReference<DatafeedConfig> configHolder = new AtomicReference<>();
        blockingCall(actionListener ->
                        datafeedConfigProvider.updateDatefeedConfig(datafeedId, update.build(), updateHeaders, actionListener),
                configHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertThat(configHolder.get().getIndices(), equalTo(updateIndices));
        assertThat(configHolder.get().getHeaders().get(securityHeader), equalTo("CHANGED"));

        // Delete
        AtomicReference<DeleteResponse> deleteResponseHolder = new AtomicReference<>();
        blockingCall(actionListener -> datafeedConfigProvider.deleteDatafeedConfig(datafeedId, actionListener),
                deleteResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponseHolder.get().getResult());
    }

    public void testMultipleCreateAndDeletes() throws InterruptedException {
        String datafeedId = "df2";

        AtomicReference<IndexResponse> indexResponseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        // Create datafeed config
        DatafeedConfig config = createDatafeedConfig(datafeedId, "j1");
        blockingCall(actionListener -> datafeedConfigProvider.putDatafeedConfig(config, actionListener),
                indexResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertEquals(RestStatus.CREATED, indexResponseHolder.get().status());

        // cannot create another with the same id
        indexResponseHolder.set(null);
        blockingCall(actionListener -> datafeedConfigProvider.putDatafeedConfig(config, actionListener),
                indexResponseHolder, exceptionHolder);
        assertNull(indexResponseHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(VersionConflictEngineException.class));

        // delete
        exceptionHolder.set(null);
        AtomicReference<DeleteResponse> deleteResponseHolder = new AtomicReference<>();
        blockingCall(actionListener -> datafeedConfigProvider.deleteDatafeedConfig(datafeedId, actionListener),
                deleteResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponseHolder.get().getResult());

        // error deleting twice
        deleteResponseHolder.set(null);
        blockingCall(actionListener -> datafeedConfigProvider.deleteDatafeedConfig(datafeedId, actionListener),
                deleteResponseHolder, exceptionHolder);
        assertNull(deleteResponseHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceNotFoundException.class));
    }

    public void testUpdateWithAValidationError() throws Exception {
        final String datafeedId = "df-bad-update";

        DatafeedConfig config = createDatafeedConfig(datafeedId, "j2");
        putDatafeedConfig(config);

        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeedId);
        update.setId("wrong-datafeed-id");

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<DatafeedConfig> configHolder = new AtomicReference<>();
        blockingCall(actionListener ->
                        datafeedConfigProvider.updateDatefeedConfig(datafeedId, update.build(), Collections.emptyMap(), actionListener),
                configHolder, exceptionHolder);
        assertNull(configHolder.get());
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), IsInstanceOf.instanceOf(IllegalArgumentException.class));
        assertThat(exceptionHolder.get().getMessage(), containsString("Cannot apply update to datafeedConfig with different id"));
    }

    public void testAllowNoDatafeeds() throws InterruptedException {
        AtomicReference<Set<String>> datafeedIdsHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(actionListener -> datafeedConfigProvider.expandDatafeedIds("_all", false, actionListener),
                datafeedIdsHolder, exceptionHolder);

        assertNull(datafeedIdsHolder.get());
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), IsInstanceOf.instanceOf(ResourceNotFoundException.class));
        assertThat(exceptionHolder.get().getMessage(), containsString("No datafeed with id [*] exists"));

        exceptionHolder.set(null);
        blockingCall(actionListener -> datafeedConfigProvider.expandDatafeedIds("_all", true, actionListener),
                datafeedIdsHolder, exceptionHolder);
        assertNotNull(datafeedIdsHolder.get());
        assertNull(exceptionHolder.get());

        AtomicReference<List<DatafeedConfig.Builder>> datafeedsHolder = new AtomicReference<>();
        blockingCall(actionListener -> datafeedConfigProvider.expandDatafeedConfigs("*", false, actionListener),
                datafeedsHolder, exceptionHolder);

        assertNull(datafeedsHolder.get());
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), IsInstanceOf.instanceOf(ResourceNotFoundException.class));
        assertThat(exceptionHolder.get().getMessage(), containsString("No datafeed with id [*] exists"));

        exceptionHolder.set(null);
        blockingCall(actionListener -> datafeedConfigProvider.expandDatafeedConfigs("*", true, actionListener),
                datafeedsHolder, exceptionHolder);
        assertNotNull(datafeedsHolder.get());
        assertNull(exceptionHolder.get());
    }

    public void testExpandDatafeeds() throws Exception {
        DatafeedConfig foo1 = putDatafeedConfig(createDatafeedConfig("foo-1", "j1"));
        DatafeedConfig foo2 = putDatafeedConfig(createDatafeedConfig("foo-2", "j2"));
        DatafeedConfig bar1 = putDatafeedConfig(createDatafeedConfig("bar-1", "j3"));
        DatafeedConfig bar2 = putDatafeedConfig(createDatafeedConfig("bar-2", "j4"));
        putDatafeedConfig(createDatafeedConfig("not-used", "j5"));

        client().admin().indices().prepareRefresh(AnomalyDetectorsIndex.configIndexName()).get();

        // Test job IDs only
        Set<String> expandedIds = blockingCall(actionListener -> datafeedConfigProvider.expandDatafeedIds("foo*", true, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("foo-1", "foo-2")), expandedIds);

        expandedIds = blockingCall(actionListener -> datafeedConfigProvider.expandDatafeedIds("*-1", true, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("bar-1", "foo-1")), expandedIds);

        expandedIds = blockingCall(actionListener -> datafeedConfigProvider.expandDatafeedIds("bar*", true, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("bar-1", "bar-2")), expandedIds);

        expandedIds = blockingCall(actionListener -> datafeedConfigProvider.expandDatafeedIds("b*r-1", true, actionListener));
        assertEquals(new TreeSet<>(Collections.singletonList("bar-1")), expandedIds);

        expandedIds = blockingCall(actionListener -> datafeedConfigProvider.expandDatafeedIds("bar-1,foo*", true, actionListener));
        assertEquals(new TreeSet<>(Arrays.asList("bar-1", "foo-1", "foo-2")), expandedIds);

        // Test full job config
        List<DatafeedConfig.Builder> expandedDatafeedBuilders =
                blockingCall(actionListener -> datafeedConfigProvider.expandDatafeedConfigs("foo*", true, actionListener));
        List<DatafeedConfig> expandedDatafeeds =
                expandedDatafeedBuilders.stream().map(DatafeedConfig.Builder::build).collect(Collectors.toList());
        assertThat(expandedDatafeeds, containsInAnyOrder(foo1, foo2));

        expandedDatafeedBuilders =
                blockingCall(actionListener -> datafeedConfigProvider.expandDatafeedConfigs("*-1", true, actionListener));
        expandedDatafeeds = expandedDatafeedBuilders.stream().map(DatafeedConfig.Builder::build).collect(Collectors.toList());
        assertThat(expandedDatafeeds, containsInAnyOrder(foo1, bar1));

        expandedDatafeedBuilders =
                blockingCall(actionListener -> datafeedConfigProvider.expandDatafeedConfigs("bar*", true, actionListener));
        expandedDatafeeds = expandedDatafeedBuilders.stream().map(DatafeedConfig.Builder::build).collect(Collectors.toList());
        assertThat(expandedDatafeeds, containsInAnyOrder(bar1, bar2));

        expandedDatafeedBuilders =
                blockingCall(actionListener -> datafeedConfigProvider.expandDatafeedConfigs("b*r-1", true, actionListener));
        expandedDatafeeds = expandedDatafeedBuilders.stream().map(DatafeedConfig.Builder::build).collect(Collectors.toList());
        assertThat(expandedDatafeeds, containsInAnyOrder(bar1));

        expandedDatafeedBuilders =
                blockingCall(actionListener -> datafeedConfigProvider.expandDatafeedConfigs("bar-1,foo*", true, actionListener));
        expandedDatafeeds = expandedDatafeedBuilders.stream().map(DatafeedConfig.Builder::build).collect(Collectors.toList());
        assertThat(expandedDatafeeds, containsInAnyOrder(bar1, foo1, foo2));
    }

    private DatafeedConfig createDatafeedConfig(String id, String jobId) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(id, jobId);
        builder.setIndices(Collections.singletonList("beats*"));

        Map<String, String> headers = new HashMap<>();
        // Only security headers are updated, grab the first one
        String securityHeader = ClientHelper.SECURITY_HEADER_FILTERS.iterator().next();
        headers.put(securityHeader, "SECURITY_");
        builder.setHeaders(headers);
        return builder.build();
    }

    private DatafeedConfig putDatafeedConfig(DatafeedConfig config) throws Exception {
        this.<IndexResponse>blockingCall(actionListener -> datafeedConfigProvider.putDatafeedConfig(config, actionListener));
        return config;
    }
}

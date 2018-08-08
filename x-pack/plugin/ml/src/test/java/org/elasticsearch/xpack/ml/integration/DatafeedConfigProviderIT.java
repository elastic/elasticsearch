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
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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

    private DatafeedConfig createDatafeedConfig(String id, String jobId) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(id, jobId);
        builder.setIndices(Collections.singletonList("beats*"));
        return builder.build();
    }

    private void putDatafeedConfig(DatafeedConfig config) throws Exception {
        this.<IndexResponse>blockingCall(actionListener -> datafeedConfigProvider.putDatafeedConfig(config, actionListener));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class DatafeedConfigProviderIT extends MlSingleNodeTestCase {
    private DatafeedConfigProvider datafeedConfigProvider;
    private String dummyAuthenticationHeader;

    @Before
    public void createComponents() throws Exception {
        datafeedConfigProvider = new DatafeedConfigProvider(client(), xContentRegistry(), getInstanceFromNode(ClusterService.class));
        waitForMlTemplates();
        dummyAuthenticationHeader = Authentication.newRealmAuthentication(
            new User("dummy"),
            new Authentication.RealmRef("name", "type", "node")
        ).encode();
    }

    public void testCrud() throws InterruptedException {
        String datafeedId = "df1";

        AtomicReference<Tuple<DatafeedConfig, IndexResponse>> responseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        // Create datafeed config
        DatafeedConfig.Builder config = createDatafeedConfig(datafeedId, "j1");
        blockingCall(
            actionListener -> datafeedConfigProvider.putDatafeedConfig(config.build(), createSecurityHeader(), actionListener),
            responseHolder,
            exceptionHolder
        );
        assertNull(exceptionHolder.get());
        assertEquals(RestStatus.CREATED, responseHolder.get().v2().status());
        assertThat(responseHolder.get().v1().getHeaders(), not(anEmptyMap()));

        // Read datafeed config
        AtomicReference<DatafeedConfig.Builder> configBuilderHolder = new AtomicReference<>();
        blockingCall(
            actionListener -> datafeedConfigProvider.getDatafeedConfig(datafeedId, null, actionListener),
            configBuilderHolder,
            exceptionHolder
        );
        assertNull(exceptionHolder.get());

        // Headers are set by the putDatafeedConfig method so they
        // must be added to the original config before equality testing
        config.setHeaders(createSecurityHeader());
        assertEquals(config.build(), configBuilderHolder.get().build());

        // Update
        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeedId);
        List<String> updateIndices = Collections.singletonList("a-different-index");
        update.setIndices(updateIndices);
        Map<String, String> updateHeaders = createSecurityHeader();

        AtomicReference<DatafeedConfig> configHolder = new AtomicReference<>();
        blockingCall(
            actionListener -> datafeedConfigProvider.updateDatefeedConfig(
                datafeedId,
                update.build(),
                updateHeaders,
                (updatedConfig, listener) -> listener.onResponse(Boolean.TRUE),
                actionListener
            ),
            configHolder,
            exceptionHolder
        );
        assertNull(exceptionHolder.get());
        assertThat(configHolder.get().getIndices(), equalTo(updateIndices));
        updateHeaders.forEach((key, value) -> assertThat(configHolder.get().getHeaders(), hasEntry(key, value)));

        // Read the updated config
        configBuilderHolder.set(null);
        blockingCall(
            actionListener -> datafeedConfigProvider.getDatafeedConfig(datafeedId, null, actionListener),
            configBuilderHolder,
            exceptionHolder
        );
        assertNull(exceptionHolder.get());
        assertThat(configBuilderHolder.get().build().getIndices(), equalTo(updateIndices));
        updateHeaders.forEach((key, value) -> assertThat(configHolder.get().getHeaders(), hasEntry(key, value)));

        // Delete
        AtomicReference<DeleteResponse> deleteResponseHolder = new AtomicReference<>();
        blockingCall(
            actionListener -> datafeedConfigProvider.deleteDatafeedConfig(datafeedId, actionListener),
            deleteResponseHolder,
            exceptionHolder
        );
        assertNull(exceptionHolder.get());
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponseHolder.get().getResult());
    }

    public void testGetDatafeedConfig_missing() throws InterruptedException {
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<DatafeedConfig.Builder> configBuilderHolder = new AtomicReference<>();
        blockingCall(
            actionListener -> datafeedConfigProvider.getDatafeedConfig("missing", null, actionListener),
            configBuilderHolder,
            exceptionHolder
        );
        assertNull(configBuilderHolder.get());
        assertEquals(ResourceNotFoundException.class, exceptionHolder.get().getClass());
    }

    public void testMultipleCreateAndDeletes() throws InterruptedException {
        String datafeedId = "df2";

        AtomicReference<Tuple<DatafeedConfig, IndexResponse>> responseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        // Create datafeed config
        DatafeedConfig.Builder config = createDatafeedConfig(datafeedId, "j1");
        blockingCall(
            actionListener -> datafeedConfigProvider.putDatafeedConfig(config.build(), Collections.emptyMap(), actionListener),
            responseHolder,
            exceptionHolder
        );
        assertNull(exceptionHolder.get());
        assertEquals(RestStatus.CREATED, responseHolder.get().v2().status());

        // cannot create another with the same id
        responseHolder.set(null);
        blockingCall(
            actionListener -> datafeedConfigProvider.putDatafeedConfig(config.build(), Collections.emptyMap(), actionListener),
            responseHolder,
            exceptionHolder
        );
        assertNull(responseHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceAlreadyExistsException.class));
        assertEquals("A datafeed with id [df2] already exists", exceptionHolder.get().getMessage());

        // delete
        exceptionHolder.set(null);
        AtomicReference<DeleteResponse> deleteResponseHolder = new AtomicReference<>();
        blockingCall(
            actionListener -> datafeedConfigProvider.deleteDatafeedConfig(datafeedId, actionListener),
            deleteResponseHolder,
            exceptionHolder
        );
        assertNull(exceptionHolder.get());
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponseHolder.get().getResult());

        // error deleting twice
        deleteResponseHolder.set(null);
        blockingCall(
            actionListener -> datafeedConfigProvider.deleteDatafeedConfig(datafeedId, actionListener),
            deleteResponseHolder,
            exceptionHolder
        );
        assertNull(deleteResponseHolder.get());
        assertEquals(ResourceNotFoundException.class, exceptionHolder.get().getClass());
    }

    public void testUpdateWhenApplyingTheUpdateThrows() throws Exception {
        final String datafeedId = "df-bad-update";

        DatafeedConfig.Builder config = createDatafeedConfig(datafeedId, "j2");
        putDatafeedConfig(config, Collections.emptyMap());

        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeedId);
        update.setId("wrong-datafeed-id");

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<DatafeedConfig> configHolder = new AtomicReference<>();
        blockingCall(
            actionListener -> datafeedConfigProvider.updateDatefeedConfig(
                datafeedId,
                update.build(),
                Collections.emptyMap(),
                (updatedConfig, listener) -> listener.onResponse(Boolean.TRUE),
                actionListener
            ),
            configHolder,
            exceptionHolder
        );
        assertNull(configHolder.get());
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), IsInstanceOf.instanceOf(IllegalArgumentException.class));
        assertThat(exceptionHolder.get().getMessage(), containsString("Cannot apply update to datafeedConfig with different id"));
    }

    public void testUpdateWithValidatorFunctionThatErrors() throws Exception {
        final String datafeedId = "df-validated-update";

        DatafeedConfig.Builder config = createDatafeedConfig(datafeedId, "hob-job");
        putDatafeedConfig(config, Collections.emptyMap());

        DatafeedUpdate.Builder update = new DatafeedUpdate.Builder(datafeedId);
        List<String> updateIndices = Collections.singletonList("a-different-index");
        update.setIndices(updateIndices);

        BiConsumer<DatafeedConfig, ActionListener<Boolean>> validateErrorFunction = (updatedConfig, listener) -> new Thread(
            () -> listener.onFailure(new IllegalArgumentException("this is a bad update")),
            getTestName()
        ).start();

        AtomicReference<DatafeedConfig> configHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        blockingCall(
            actionListener -> datafeedConfigProvider.updateDatefeedConfig(
                datafeedId,
                update.build(),
                Collections.emptyMap(),
                validateErrorFunction,
                actionListener
            ),
            configHolder,
            exceptionHolder
        );

        assertNull(configHolder.get());
        assertThat(exceptionHolder.get(), IsInstanceOf.instanceOf(IllegalArgumentException.class));
        assertThat(exceptionHolder.get().getMessage(), containsString("this is a bad update"));

    }

    public void testAllowNoMatch() throws InterruptedException {
        AtomicReference<SortedSet<String>> datafeedIdsHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(
            actionListener -> datafeedConfigProvider.expandDatafeedIds("_all", false, null, false, null, actionListener),
            datafeedIdsHolder,
            exceptionHolder
        );

        assertNull(datafeedIdsHolder.get());
        assertNotNull(exceptionHolder.get());
        assertEquals(ResourceNotFoundException.class, exceptionHolder.get().getClass());
        assertThat(exceptionHolder.get().getMessage(), containsString("No datafeed with id [*] exists"));

        exceptionHolder.set(null);
        blockingCall(
            actionListener -> datafeedConfigProvider.expandDatafeedIds("_all", true, null, false, null, actionListener),
            datafeedIdsHolder,
            exceptionHolder
        );
        assertNotNull(datafeedIdsHolder.get());
        assertNull(exceptionHolder.get());

        AtomicReference<List<DatafeedConfig.Builder>> datafeedsHolder = new AtomicReference<>();
        blockingCall(
            actionListener -> datafeedConfigProvider.expandDatafeedConfigs("*", false, null, actionListener),
            datafeedsHolder,
            exceptionHolder
        );

        assertNull(datafeedsHolder.get());
        assertNotNull(exceptionHolder.get());
        assertEquals(ResourceNotFoundException.class, exceptionHolder.get().getClass());
        assertThat(exceptionHolder.get().getMessage(), containsString("No datafeed with id [*] exists"));

        exceptionHolder.set(null);
        blockingCall(
            actionListener -> datafeedConfigProvider.expandDatafeedConfigs("*", true, null, actionListener),
            datafeedsHolder,
            exceptionHolder
        );
        assertNotNull(datafeedsHolder.get());
        assertNull(exceptionHolder.get());
    }

    public void testExpandDatafeeds() throws Exception {
        DatafeedConfig foo1 = putDatafeedConfig(createDatafeedConfig("foo-1", "j1"), Collections.emptyMap());
        DatafeedConfig foo2 = putDatafeedConfig(createDatafeedConfig("foo-2", "j2"), Collections.emptyMap());
        DatafeedConfig bar1 = putDatafeedConfig(createDatafeedConfig("bar-1", "j3"), Collections.emptyMap());
        DatafeedConfig bar2 = putDatafeedConfig(createDatafeedConfig("bar-2", "j4"), Collections.emptyMap());
        putDatafeedConfig(createDatafeedConfig("not-used", "j5"), Collections.emptyMap());

        client().admin().indices().prepareRefresh(MlConfigIndex.indexName()).get();

        // Test datafeed IDs only
        SortedSet<String> expandedIds = blockingCall(
            actionListener -> datafeedConfigProvider.expandDatafeedIds("foo*", true, null, false, null, actionListener)
        );
        assertEquals(new TreeSet<>(Arrays.asList("foo-1", "foo-2")), expandedIds);

        expandedIds = blockingCall(
            actionListener -> datafeedConfigProvider.expandDatafeedIds("*-1", true, null, false, null, actionListener)
        );
        assertEquals(new TreeSet<>(Arrays.asList("bar-1", "foo-1")), expandedIds);

        expandedIds = blockingCall(
            actionListener -> datafeedConfigProvider.expandDatafeedIds("bar*", true, null, false, null, actionListener)
        );
        assertEquals(new TreeSet<>(Arrays.asList("bar-1", "bar-2")), expandedIds);

        expandedIds = blockingCall(
            actionListener -> datafeedConfigProvider.expandDatafeedIds("b*r-1", true, null, false, null, actionListener)
        );
        assertEquals(new TreeSet<>(Collections.singletonList("bar-1")), expandedIds);

        expandedIds = blockingCall(
            actionListener -> datafeedConfigProvider.expandDatafeedIds("bar-1,foo*", true, null, false, null, actionListener)
        );
        assertEquals(new TreeSet<>(Arrays.asList("bar-1", "foo-1", "foo-2")), expandedIds);

        // Test full datafeed config
        List<DatafeedConfig.Builder> expandedDatafeedBuilders = blockingCall(
            actionListener -> datafeedConfigProvider.expandDatafeedConfigs("foo*", true, null, actionListener)
        );
        List<DatafeedConfig> expandedDatafeeds = expandedDatafeedBuilders.stream()
            .map(DatafeedConfig.Builder::build)
            .collect(Collectors.toList());
        assertThat(expandedDatafeeds, containsInAnyOrder(foo1, foo2));

        expandedDatafeedBuilders = blockingCall(
            actionListener -> datafeedConfigProvider.expandDatafeedConfigs("*-1", true, null, actionListener)
        );
        expandedDatafeeds = expandedDatafeedBuilders.stream().map(DatafeedConfig.Builder::build).collect(Collectors.toList());
        assertThat(expandedDatafeeds, containsInAnyOrder(foo1, bar1));

        expandedDatafeedBuilders = blockingCall(
            actionListener -> datafeedConfigProvider.expandDatafeedConfigs("bar*", true, null, actionListener)
        );
        expandedDatafeeds = expandedDatafeedBuilders.stream().map(DatafeedConfig.Builder::build).collect(Collectors.toList());
        assertThat(expandedDatafeeds, containsInAnyOrder(bar1, bar2));

        expandedDatafeedBuilders = blockingCall(
            actionListener -> datafeedConfigProvider.expandDatafeedConfigs("b*r-1", true, null, actionListener)
        );
        expandedDatafeeds = expandedDatafeedBuilders.stream().map(DatafeedConfig.Builder::build).collect(Collectors.toList());
        assertThat(expandedDatafeeds, containsInAnyOrder(bar1));

        expandedDatafeedBuilders = blockingCall(
            actionListener -> datafeedConfigProvider.expandDatafeedConfigs("bar-1,foo*", true, null, actionListener)
        );
        expandedDatafeeds = expandedDatafeedBuilders.stream().map(DatafeedConfig.Builder::build).collect(Collectors.toList());
        assertThat(expandedDatafeeds, containsInAnyOrder(bar1, foo1, foo2));
    }

    public void testExpandDatafeedsWithTaskData() throws Exception {
        putDatafeedConfig(createDatafeedConfig("foo-2", "j2"), Collections.emptyMap());
        client().admin().indices().prepareRefresh(MlConfigIndex.indexName()).get();

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        tasksBuilder.addTask(
            MlTasks.datafeedTaskId("foo-1"),
            MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams("foo-1", 0L),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );

        PersistentTasksCustomMetadata tasks = tasksBuilder.build();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<SortedSet<String>> datafeedIdsHolder = new AtomicReference<>();
        // Test datafeed IDs only
        SortedSet<String> expandedIds = blockingCall(
            actionListener -> datafeedConfigProvider.expandDatafeedIds("foo*", false, tasks, true, null, actionListener)
        );
        assertEquals(new TreeSet<>(Arrays.asList("foo-1", "foo-2")), expandedIds);

        blockingCall(
            actionListener -> datafeedConfigProvider.expandDatafeedIds("foo-1*,foo-2*", false, tasks, false, null, actionListener),
            datafeedIdsHolder,
            exceptionHolder
        );
        assertThat(exceptionHolder.get(), is(not(nullValue())));
        assertEquals(ResourceNotFoundException.class, exceptionHolder.get().getClass());
        assertThat(exceptionHolder.get().getMessage(), containsString("No datafeed with id [foo-1*] exists"));
    }

    public void testFindDatafeedIdsForJobIds() throws Exception {
        putDatafeedConfig(createDatafeedConfig("foo-1", "j1"), Collections.emptyMap());
        putDatafeedConfig(createDatafeedConfig("foo-2", "j2"), Collections.emptyMap());
        putDatafeedConfig(createDatafeedConfig("bar-1", "j3"), Collections.emptyMap());

        client().admin().indices().prepareRefresh(MlConfigIndex.indexName()).get();

        AtomicReference<Set<String>> datafeedIdsHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(
            actionListener -> datafeedConfigProvider.findDatafeedIdsForJobIds(Collections.singletonList("new-job"), actionListener),
            datafeedIdsHolder,
            exceptionHolder
        );
        assertThat(datafeedIdsHolder.get(), empty());

        blockingCall(
            actionListener -> datafeedConfigProvider.findDatafeedIdsForJobIds(Collections.singletonList("j2"), actionListener),
            datafeedIdsHolder,
            exceptionHolder
        );
        assertThat(datafeedIdsHolder.get(), contains("foo-2"));

        blockingCall(
            actionListener -> datafeedConfigProvider.findDatafeedIdsForJobIds(Arrays.asList("j3", "j1"), actionListener),
            datafeedIdsHolder,
            exceptionHolder
        );
        assertThat(datafeedIdsHolder.get(), contains("bar-1", "foo-1"));
    }

    public void testFindDatafeedIdsForJobIds_ManyJobs() throws Exception {
        var jobIds = new ArrayList<String>();
        var dfIds = new HashSet<String>();
        for (int i = 0; i < 13; i++) {
            String id = Integer.toString(i);
            var dfId = "df-" + id;
            var jobId = "j-" + id;
            putDatafeedConfig(createDatafeedConfig(dfId, jobId), Collections.emptyMap());
            dfIds.add(dfId);
            jobIds.add(jobId);
        }

        client().admin().indices().prepareRefresh(MlConfigIndex.indexName()).get();

        AtomicReference<Set<String>> datafeedIdsHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(
            actionListener -> datafeedConfigProvider.findDatafeedIdsForJobIds(jobIds, actionListener),
            datafeedIdsHolder,
            exceptionHolder
        );
        assertEquals(dfIds, datafeedIdsHolder.get());
    }

    public void testFindDatafeedsForJobIds() throws Exception {
        putDatafeedConfig(createDatafeedConfig("foo-1", "j1"), Collections.emptyMap());
        putDatafeedConfig(createDatafeedConfig("foo-2", "j2"), Collections.emptyMap());
        putDatafeedConfig(createDatafeedConfig("bar-1", "j3"), Collections.emptyMap());

        client().admin().indices().prepareRefresh(MlConfigIndex.indexName()).get();

        AtomicReference<Map<String, DatafeedConfig.Builder>> datafeedMapHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(
            actionListener -> datafeedConfigProvider.findDatafeedsByJobIds(Collections.singletonList("new-job"), null, actionListener),
            datafeedMapHolder,
            exceptionHolder
        );
        assertThat(datafeedMapHolder.get(), anEmptyMap());

        blockingCall(
            actionListener -> datafeedConfigProvider.findDatafeedsByJobIds(Collections.singletonList("j2"), null, actionListener),
            datafeedMapHolder,
            exceptionHolder
        );
        assertThat(datafeedMapHolder.get(), hasKey("j2"));
        assertThat(datafeedMapHolder.get().get("j2").getId(), equalTo("foo-2"));

        blockingCall(
            actionListener -> datafeedConfigProvider.findDatafeedsByJobIds(Arrays.asList("j3", "j1"), null, actionListener),
            datafeedMapHolder,
            exceptionHolder
        );
        assertThat(datafeedMapHolder.get(), allOf(hasKey("j3"), hasKey("j1")));
        assertThat(datafeedMapHolder.get().get("j3").getId(), equalTo("bar-1"));
        assertThat(datafeedMapHolder.get().get("j1").getId(), equalTo("foo-1"));
    }

    public void testFindDatafeedsForJobIds_ManyJobs() throws Exception {
        var jobIds = new ArrayList<String>();
        for (int i = 0; i < 13; i++) {
            String id = Integer.toString(i);
            var dfId = "df-" + id;
            var jobId = "j-" + id;
            putDatafeedConfig(createDatafeedConfig(dfId, jobId), Collections.emptyMap());
            jobIds.add(jobId);
        }

        client().admin().indices().prepareRefresh(MlConfigIndex.indexName()).get();

        AtomicReference<Map<String, DatafeedConfig.Builder>> datafeedMapHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(
            actionListener -> datafeedConfigProvider.findDatafeedsByJobIds(jobIds, null, actionListener),
            datafeedMapHolder,
            exceptionHolder
        );
        assertThat(datafeedMapHolder.get().entrySet(), hasSize(jobIds.size()));
    }

    public void testHeadersAreOverwritten() throws Exception {
        String dfId = "df-with-headers";
        DatafeedConfig.Builder configWithUnrelatedHeaders = createDatafeedConfig(dfId, "j1");
        Map<String, String> headers = new HashMap<>();
        headers.put("UNRELATED-FIELD", "WILL-BE-FILTERED");
        configWithUnrelatedHeaders.setHeaders(headers);

        putDatafeedConfig(configWithUnrelatedHeaders, createSecurityHeader());

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<DatafeedConfig.Builder> configBuilderHolder = new AtomicReference<>();
        blockingCall(
            actionListener -> datafeedConfigProvider.getDatafeedConfig(dfId, null, actionListener),
            configBuilderHolder,
            exceptionHolder
        );
        assertNull(exceptionHolder.get());
        assertThat(configBuilderHolder.get().build().getHeaders().entrySet(), hasSize(1));
        assertEquals(configBuilderHolder.get().build().getHeaders(), createSecurityHeader());
    }

    private DatafeedConfig.Builder createDatafeedConfig(String id, String jobId) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(id, jobId);
        builder.setIndices(Collections.singletonList("beats*"));
        return builder;
    }

    private Map<String, String> createSecurityHeader() {
        Map<String, String> headers = new HashMap<>();
        // Only security headers are updated, grab the first one
        String securityHeader = ClientHelper.SECURITY_HEADER_FILTERS.iterator().next();
        if (Set.of(AuthenticationField.AUTHENTICATION_KEY, SecondaryAuthentication.THREAD_CTX_KEY).contains(securityHeader)) {
            headers.put(securityHeader, dummyAuthenticationHeader);
        } else {
            headers.put(securityHeader, "SECURITY_");
        }
        return headers;
    }

    private DatafeedConfig putDatafeedConfig(DatafeedConfig.Builder builder, Map<String, String> headers) throws Exception {
        builder.setHeaders(headers);
        DatafeedConfig config = builder.build();
        this.<Tuple<DatafeedConfig, IndexResponse>>blockingCall(
            actionListener -> datafeedConfigProvider.putDatafeedConfig(config, headers, actionListener)
        );
        return config;
    }
}

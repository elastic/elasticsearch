/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.Step;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.waitUntil;
import static org.elasticsearch.test.rest.ESRestTestCase.assertOK;
import static org.elasticsearch.test.rest.ESRestTestCase.ensureHealth;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ilm.ShrinkIndexNameSupplier.SHRUNKEN_INDEX_PREFIX;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * This class provides the operational REST functions needed to control an ILM time series lifecycle.
 */
public final class TimeSeriesRestDriver {

    private static final Logger logger = LogManager.getLogger(TimeSeriesRestDriver.class);

    private TimeSeriesRestDriver() {}

    public static Step.StepKey getStepKeyForIndex(RestClient client, String indexName) throws IOException {
        Map<String, Object> indexResponse = explainIndex(client, indexName);
        if (indexResponse == null) {
            return new Step.StepKey(null, null, null);
        }

        return getStepKey(indexResponse);
    }

    private static Step.StepKey getStepKey(Map<String, Object> explainIndexResponse) {
        String phase = (String) explainIndexResponse.get("phase");
        String action = (String) explainIndexResponse.get("action");
        String step = (String) explainIndexResponse.get("step");
        return new Step.StepKey(phase, action, step);
    }

    public static Map<String, Object> explainIndex(RestClient client, String indexName) throws IOException {
        return explain(client, indexName, false, false).get(indexName);
    }

    public static Map<String, Map<String, Object>> explain(RestClient client, String indexPattern, boolean onlyErrors, boolean onlyManaged)
        throws IOException {
        RequestOptions consumeWarningsOptions = RequestOptions.DEFAULT.toBuilder()
            .setWarningsHandler(warnings -> warnings.isEmpty() == false && List.of("""
                [indices.lifecycle.rollover.only_if_has_documents] setting was deprecated in Elasticsearch \
                and will be removed in a future release. \
                See the deprecation documentation for the next major version.""").equals(warnings) == false)
            .build();

        Request explainRequest = new Request("GET", indexPattern + "/_ilm/explain");
        explainRequest.addParameter("only_errors", Boolean.toString(onlyErrors));
        explainRequest.addParameter("only_managed", Boolean.toString(onlyManaged));
        explainRequest.setOptions(consumeWarningsOptions);
        Response response = client.performRequest(explainRequest);
        Map<String, Object> responseMap;
        try (InputStream is = response.getEntity().getContent()) {
            responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
        }

        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> indexResponse = ((Map<String, Map<String, Object>>) responseMap.get("indices"));
        return indexResponse;
    }

    public static void indexDocument(RestClient client, String indexAbstractionName) throws IOException {
        indexDocument(client, indexAbstractionName, false);
    }

    public static void indexDocument(RestClient client, String indexAbstractionName, boolean refresh) throws IOException {
        Request indexRequest = new Request("POST", indexAbstractionName + "/_doc" + (refresh ? "?refresh" : ""));
        indexRequest.setEntity(new StringEntity("{\"@timestamp\": \"2020-12-12\"}", ContentType.APPLICATION_JSON));
        Response response = client.performRequest(indexRequest);
        logger.info(response.getStatusLine());
    }

    public static void index(RestClient client, String index, String id, Object... fields) throws IOException {
        index(client, index, false, id, fields);
    }

    public static void index(RestClient client, String index, boolean refresh, String id, Object... fields) throws IOException {
        XContentBuilder document = jsonBuilder().startObject();
        for (int i = 0; i < fields.length; i += 2) {
            document.field((String) fields[i], fields[i + 1]);
        }
        document.endObject();
        final Request request = new Request("POST", "/" + index + "/_doc/" + (id != null ? id : "") + (refresh ? "?refresh" : ""));
        request.setJsonEntity(Strings.toString(document));
        assertThat(client.performRequest(request).getStatusLine().getStatusCode(), anyOf(equalTo(200), equalTo(201)));
    }

    public static void createNewSingletonPolicy(RestClient client, String policyName, String phaseName, LifecycleAction action)
        throws IOException {
        createNewSingletonPolicy(client, policyName, phaseName, action, TimeValue.ZERO);
    }

    public static void createNewSingletonPolicy(
        RestClient client,
        String policyName,
        String phaseName,
        LifecycleAction action,
        TimeValue after
    ) throws IOException {
        Phase phase = new Phase(phaseName, after, Map.of(action.getWriteableName(), action));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policyName, Map.of(phase.getName(), phase));
        XContentBuilder builder = jsonBuilder();
        lifecyclePolicy.toXContent(builder, null);
        final StringEntity entity = new StringEntity("{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", "_ilm/policy/" + policyName);
        request.setEntity(entity);
        assertOK(client.performRequest(request));
    }

    public static void createComposableTemplate(RestClient client, String templateName, String indexPattern, Template template)
        throws IOException {
        XContentBuilder builder = jsonBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        StringEntity templateJSON = new StringEntity(String.format(Locale.ROOT, """
            {
              "index_patterns": "%s",
              "data_stream": {},
              "template": %s
            }""", indexPattern, Strings.toString(builder)), ContentType.APPLICATION_JSON);
        Request createIndexTemplateRequest = new Request("PUT", "_index_template/" + templateName);
        createIndexTemplateRequest.setEntity(templateJSON);
        client.performRequest(createIndexTemplateRequest);
    }

    public static void rolloverMaxOneDocCondition(RestClient client, String indexAbstractionName) throws IOException {
        Request rolloverRequest = new Request("POST", "/" + indexAbstractionName + "/_rollover");
        rolloverRequest.setJsonEntity("""
            {
              "conditions": {
                "max_docs": "1"
              }
            }""");
        client.performRequest(rolloverRequest);
    }

    public static void createFullPolicy(RestClient client, String policyName, TimeValue hotTime) throws IOException {
        Map<String, LifecycleAction> hotActions = new HashMap<>();
        hotActions.put(SetPriorityAction.NAME, new SetPriorityAction(100));
        hotActions.put(RolloverAction.NAME, new RolloverAction(null, null, null, 1L, null, null, null, null, null, null));
        Map<String, LifecycleAction> warmActions = new HashMap<>();
        warmActions.put(SetPriorityAction.NAME, new SetPriorityAction(50));
        warmActions.put(ForceMergeAction.NAME, new ForceMergeAction(1, null));
        warmActions.put(
            AllocateAction.NAME,
            new AllocateAction(
                1,
                null,
                Map.of("_name", "javaRestTest-0,javaRestTest-1," + "javaRestTest-2," + "javaRestTest-3"),
                null,
                null
            )
        );
        warmActions.put(ShrinkAction.NAME, new ShrinkAction(1, null, false));
        Map<String, LifecycleAction> coldActions = new HashMap<>();
        coldActions.put(SetPriorityAction.NAME, new SetPriorityAction(0));
        coldActions.put(
            AllocateAction.NAME,
            new AllocateAction(
                0,
                null,
                Map.of("_name", "javaRestTest-0,javaRestTest-1," + "javaRestTest-2," + "javaRestTest-3"),
                null,
                null
            )
        );
        Map<String, Phase> phases = new HashMap<>();
        phases.put("hot", new Phase("hot", hotTime, hotActions));
        phases.put("warm", new Phase("warm", TimeValue.ZERO, warmActions));
        phases.put("cold", new Phase("cold", TimeValue.ZERO, coldActions));
        phases.put("delete", new Phase("delete", TimeValue.ZERO, Map.of(DeleteAction.NAME, DeleteAction.WITH_SNAPSHOT_DELETE)));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policyName, phases);
        // PUT policy
        XContentBuilder builder = jsonBuilder();
        lifecyclePolicy.toXContent(builder, null);
        final StringEntity entity = new StringEntity("{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", "_ilm/policy/" + policyName);
        request.setEntity(entity);
        client.performRequest(request);
    }

    public static void createPolicy(
        RestClient client,
        String policyName,
        @Nullable Phase hotPhase,
        @Nullable Phase warmPhase,
        @Nullable Phase coldPhase,
        @Nullable Phase frozenPhase,
        @Nullable Phase deletePhase
    ) throws IOException {
        if (hotPhase == null && warmPhase == null && coldPhase == null && deletePhase == null) {
            throw new IllegalArgumentException("specify at least one phase");
        }
        Map<String, Phase> phases = new HashMap<>();
        if (hotPhase != null) {
            phases.put("hot", hotPhase);
        }
        if (warmPhase != null) {
            phases.put("warm", warmPhase);
        }
        if (coldPhase != null) {
            phases.put("cold", coldPhase);
        }
        if (frozenPhase != null) {
            phases.put("frozen", frozenPhase);
        }
        if (deletePhase != null) {
            phases.put("delete", deletePhase);
        }
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policyName, phases);
        XContentBuilder builder = jsonBuilder();
        lifecyclePolicy.toXContent(builder, null);
        final StringEntity entity = new StringEntity("{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", "_ilm/policy/" + policyName);
        request.setEntity(entity);
        client.performRequest(request);
    }

    public static void createSnapshotRepo(RestClient client, String repoName, boolean compress) throws IOException {
        Request request = new Request("PUT", "/_snapshot/" + repoName);
        request.setJsonEntity(
            Strings.toString(
                JsonXContent.contentBuilder()
                    .startObject()
                    .field("type", "fs")
                    .startObject("settings")
                    .field("compress", compress)
                    // random location to avoid clash with other snapshots
                    .field("location", System.getProperty("tests.path.repo") + "/" + randomAlphaOfLengthBetween(4, 10))
                    .field("max_snapshot_bytes_per_sec", "100m")
                    .endObject()
                    .endObject()
            )
        );
        client.performRequest(request);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> getOnlyIndexSettings(RestClient client, String index) throws IOException {
        Request request = new Request("GET", "/" + index + "/_settings");
        request.addParameter("flat_settings", "true");
        Response response = client.performRequest(request);
        try (InputStream is = response.getEntity().getContent()) {
            Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            Map<String, Object> indexSettings = (Map<String, Object>) responseMap.get(index);
            if (indexSettings == null) {
                return Map.of();
            }
            return (Map<String, Object>) indexSettings.get("settings");
        }
    }

    public static void createIndexWithSettings(RestClient client, String index, String alias, Settings.Builder settings)
        throws IOException {
        createIndexWithSettings(client, index, alias, settings, null);
    }

    public static void createIndexWithSettings(RestClient client, String index, String alias, Settings.Builder settings, String mapping)
        throws IOException {
        createIndexWithSettings(client, index, alias, settings, mapping, randomBoolean());
    }

    public static void createIndexWithSettings(
        RestClient client,
        String index,
        String alias,
        Settings.Builder settings,
        boolean useWriteIndex
    ) throws IOException {
        createIndexWithSettings(client, index, alias, settings, null, useWriteIndex);
    }

    public static void createIndexWithSettings(
        RestClient client,
        String index,
        String alias,
        Settings.Builder settings,
        String mapping,
        boolean useWriteIndex
    ) throws IOException {
        Request request = new Request("PUT", "/" + index);

        String writeIndexSnippet = "";
        if (useWriteIndex) {
            writeIndexSnippet = "\"is_write_index\": true";
        }
        String m = mapping != null ? String.format(Locale.ROOT, "\"mappings\": %s, ", mapping) : "";
        request.setJsonEntity(String.format(Locale.ROOT, """
            {
             "settings": %s,
             %s
             "aliases" : { "%s": { %s } }
            }""", Strings.toString(settings.build()), m, alias, writeIndexSnippet));
        client.performRequest(request);
        // wait for the shards to initialize
        ensureGreen(index);
    }

    public static void createIndexWithSettings(RestClient client, String index, Settings.Builder settings) throws IOException {
        Request request = new Request("PUT", "/" + index);
        request.setJsonEntity(String.format(Locale.ROOT, """
            {
             "settings": %s
            }""", Strings.toString(settings.build())));
        client.performRequest(request);
        // wait for the shards to initialize
        ensureGreen(index);
    }

    private static void ensureGreen(String index) throws IOException {
        ensureHealth(index, (request) -> {
            request.addParameter("wait_for_status", "green");
            request.addParameter("wait_for_no_relocating_shards", "true");
        });
    }

    @SuppressWarnings("unchecked")
    public static Integer getNumberOfPrimarySegments(RestClient client, String index) throws IOException {
        Response response = client.performRequest(new Request("GET", index + "/_segments"));
        XContentType entityContentType = XContentType.fromMediaType(response.getEntity().getContentType().getValue());
        final Map<String, Object> originalResponseEntity = XContentHelper.convertToMap(
            entityContentType.xContent(),
            response.getEntity().getContent(),
            false
        );
        if (logger.isTraceEnabled()) {
            logger.trace(
                "segments response for {}: {}",
                index,
                originalResponseEntity.keySet()
                    .stream()
                    .map(key -> key + "=" + originalResponseEntity.get(key))
                    .collect(Collectors.joining(", ", "{", "}"))
            );
        }
        Map<String, Object> responseEntity = (Map<String, Object>) originalResponseEntity.get("indices");
        responseEntity = (Map<String, Object>) responseEntity.get(index);
        responseEntity = (Map<String, Object>) responseEntity.get("shards");
        List<Map<String, Object>> shards = (List<Map<String, Object>>) responseEntity.get("0");
        // We want to mamke sure to get the primary shard because there is a chance the replica doesn't have data yet:
        Optional<Map<String, Object>> shardOptional = shards.stream()
            .filter(shard -> ((Map<String, Object>) shard.get("routing")).get("primary").equals(true))
            .findAny();
        if (shardOptional.isPresent()) {
            return (Integer) shardOptional.get().get("num_search_segments");
        } else {
            throw new RuntimeException("No primary shard found for index " + index);
        }
    }

    public static void updatePolicy(RestClient client, String indexName, String policy) throws IOException {
        Request changePolicyRequest = new Request("PUT", "/" + indexName + "/_settings");
        final StringEntity changePolicyEntity = new StringEntity(
            "{ \"index.lifecycle.name\": \"" + policy + "\" }",
            ContentType.APPLICATION_JSON
        );
        changePolicyRequest.setEntity(changePolicyEntity);
        assertOK(client.performRequest(changePolicyRequest));
    }

    @SuppressWarnings("unchecked")
    public static String getSnapshotState(RestClient client, String snapshot) throws IOException {
        Response response = client.performRequest(new Request("GET", "/_snapshot/repo/" + snapshot));
        Map<String, Object> responseMap;
        try (InputStream is = response.getEntity().getContent()) {
            responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
        }

        Map<String, Object> snapResponse = ((List<Map<String, Object>>) responseMap.get("snapshots")).get(0);
        assertThat(snapResponse.get("snapshot"), equalTo(snapshot));
        return (String) snapResponse.get("state");
    }

    /**
     * This method waits to get the shrunk index name and if it fails, it triggers once a cluster state update and tries again.
     * The motivation behind this method is that in ShrinkAction there are cluster state dependent steps, for example
     * {@link org.elasticsearch.xpack.core.ilm.CheckTargetShardsCountStep}, that might miss the latest policy update if they are
     * already queued, see {@link org.elasticsearch.xpack.ilm.IndexLifecycleRunner#submitUnlessAlreadyQueued}. In the real world
     * there is usually another cluster state coming but since this is the test world there is not. That is what this method is simulating.
     */
    @Nullable
    public static String waitAndGetShrinkIndexNameWithExtraClusterStateChange(RestClient client, String originalIndex)
        throws InterruptedException, IOException {
        String shrunkenIndexName = waitAndGetShrinkIndexName(client, originalIndex);
        if (shrunkenIndexName == null) {
            logger.info("Executing dummy cluster update to re-trigger a cluster state dependent step.");
            executeDummyClusterStateUpdate(client);
            shrunkenIndexName = waitAndGetShrinkIndexName(client, originalIndex);
        }
        return shrunkenIndexName;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public static String waitAndGetShrinkIndexName(RestClient client, String originalIndex) throws InterruptedException {
        String[] shrunkenIndexName = new String[1];
        waitUntil(() -> {
            try {
                // we're including here the case where the original index was already deleted and we have to look for the shrunken index
                Request explainRequest = new Request(
                    "GET",
                    SHRUNKEN_INDEX_PREFIX + "*" + originalIndex + "," + originalIndex + "/_ilm/explain"
                );
                // Sometimes, the original index might already have been deleted, so we need to ignore unavailable (concrete) indices.
                explainRequest.addParameter("ignore_unavailable", Boolean.toString(true));
                explainRequest.addParameter("expand_wildcards", "open,hidden");
                explainRequest.addParameter("only_errors", Boolean.toString(false));
                explainRequest.addParameter("only_managed", Boolean.toString(false));
                Response response = client.performRequest(explainRequest);
                Map<String, Object> responseMap;
                try (InputStream is = response.getEntity().getContent()) {
                    responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                }

                Map<String, Map<String, Object>> indexResponse = ((Map<String, Map<String, Object>>) responseMap.get("indices"));
                Map<String, Object> explainIndexResponse = indexResponse.get(originalIndex);
                if (explainIndexResponse == null) {
                    // maybe we swapped the alias from the original index to the shrunken one already
                    for (Map.Entry<String, Map<String, Object>> indexToExplainMap : indexResponse.entrySet()) {
                        // we don't know the exact name of the shrunken index, but we know it starts with the configured prefix
                        String indexName = indexToExplainMap.getKey();
                        if (indexName.startsWith(SHRUNKEN_INDEX_PREFIX) && indexName.contains(originalIndex)) {
                            explainIndexResponse = indexToExplainMap.getValue();
                            break;
                        }
                    }
                }

                logger.info("--> index {}, explain {}", originalIndex, indexResponse);
                if (explainIndexResponse == null) {
                    return false;
                }
                shrunkenIndexName[0] = (String) explainIndexResponse.get("shrink_index_name");
                return shrunkenIndexName[0] != null;
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS);
        logger.info("--> original index name is [{}], shrunken index name is [{}]", originalIndex, shrunkenIndexName[0]);
        return shrunkenIndexName[0];
    }

    @SuppressWarnings("unchecked")
    public static List<String> getBackingIndices(RestClient client, String dataStreamName) throws IOException {
        Response getDataStream = client.performRequest(new Request("GET", "_data_stream/" + dataStreamName));
        Map<String, Object> responseMap;
        try (InputStream is = getDataStream.getEntity().getContent()) {
            responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
        }

        List<Map<String, Object>> dataStreams = (List<Map<String, Object>>) responseMap.get("data_streams");
        assertThat(dataStreams.size(), is(1));
        Map<String, Object> dataStream = dataStreams.get(0);
        assertThat(dataStream.get("name"), is(dataStreamName));
        List<String> indices = ((List<Map<String, Object>>) dataStream.get("indices")).stream()
            .map(indexMap -> (String) indexMap.get("index_name"))
            .toList();
        return indices;
    }

    private static void executeDummyClusterStateUpdate(RestClient client) throws IOException {
        createIndexWithSettings(
            client,
            "dummy-index",
            Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .putNull(DataTier.TIER_PREFERENCE)
        );
    }

    public static Template getTemplate(String policyName) {
        return new Template(getLifecycleSettings(policyName), null, null);
    }

    public static Settings getLifecycleSettings(String policyName) {
        return Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyName).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).build();
    }
}

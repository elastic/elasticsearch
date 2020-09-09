/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ilm.CheckNotDataStreamWriteIndexStep;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.FreezeAction;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.TimeSeriesRestDriver.createComposableTemplate;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createFullPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createSnapshotRepo;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.explainIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getOnlyIndexSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getStepKeyForIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.indexDocument;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.rolloverMaxOneDocCondition;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TimeSeriesDataStreamsIT extends ESRestTestCase {

    public void testRolloverAction() throws Exception {
        String policyName = "logs-policy";
        createNewSingletonPolicy(client(), policyName, "hot", new RolloverAction(null, null, 1L));

        createComposableTemplate(client(), "logs-template", "logs-foo*", getTemplate(policyName));

        String dataStream = "logs-foo";
        indexDocument(client(), dataStream, true);

        assertBusy(() -> assertTrue(indexExists(DataStream.getDefaultBackingIndexName(dataStream, 2))));
        assertBusy(() -> assertTrue(Boolean.parseBoolean((String) getIndexSettingsAsMap(
            DataStream.getDefaultBackingIndexName(dataStream, 2)).get("index.hidden"))));
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), DataStream.getDefaultBackingIndexName(dataStream, 1)),
            equalTo(PhaseCompleteStep.finalStep("hot").getKey())));
    }

    public void testShrinkActionInPolicyWithoutHotPhase() throws Exception {
        String policyName = "logs-policy";
        createNewSingletonPolicy(client(), policyName, "warm", new ShrinkAction(1));

        createComposableTemplate(client(), "logs-template", "logs-foo*", getTemplate(policyName));

        String dataStream = "logs-foo";
        indexDocument(client(), dataStream, true);

        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStream, 1);
        String shrunkenIndex = ShrinkAction.SHRUNKEN_INDEX_PREFIX + backingIndexName;
        assertBusy(() -> assertThat(
            "original index must wait in the " + CheckNotDataStreamWriteIndexStep.NAME + " until it is not the write index anymore",
            explainIndex(client(), backingIndexName).get("step"), is(CheckNotDataStreamWriteIndexStep.NAME)), 30, TimeUnit.SECONDS);

        // Manual rollover the original index such that it's not the write index in the data stream anymore
        rolloverMaxOneDocCondition(client(), dataStream);

        assertBusy(() -> assertTrue(indexExists(shrunkenIndex)), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), shrunkenIndex), equalTo(PhaseCompleteStep.finalStep("warm").getKey())));
        assertBusy(() -> assertThat("the original index must've been deleted", indexExists(backingIndexName), is(false)));
    }

    public void testShrinkAfterRollover() throws Exception {
        String policyName = "logs-policy";
        createFullPolicy(client(), policyName, TimeValue.ZERO);

        createComposableTemplate(client(), "logs-template", "logs-foo*", getTemplate(policyName));

        String dataStream = "logs-foo";
        indexDocument(client(), dataStream, true);

        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStream, 1);
        String rolloverIndex = DataStream.getDefaultBackingIndexName(dataStream, 2);
        String shrunkenIndex = ShrinkAction.SHRUNKEN_INDEX_PREFIX + backingIndexName;
        assertBusy(() -> assertTrue("the rollover action created the rollover index", indexExists(rolloverIndex)));
        assertBusy(() -> assertFalse("the original index was deleted by the shrink action", indexExists(backingIndexName)),
            60, TimeUnit.SECONDS);
        assertBusy(() -> assertFalse("the shrunken index was deleted by the delete action", indexExists(shrunkenIndex)),
            30, TimeUnit.SECONDS);
    }

    public void testSearchableSnapshotAction() throws Exception {
        String snapshotRepo = randomAlphaOfLengthBetween(5, 10);
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        String policyName = "logs-policy";
        createNewSingletonPolicy(client(), policyName, "cold", new SearchableSnapshotAction(snapshotRepo));

        createComposableTemplate(client(), "logs-template", "logs-foo*", getTemplate(policyName));
        String dataStream = "logs-foo";
        indexDocument(client(), dataStream, true);

        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStream, 1);
        String restoredIndexName = SearchableSnapshotAction.RESTORED_INDEX_PREFIX + backingIndexName;

        assertBusy(() -> assertThat(
            "original index must wait in the " + CheckNotDataStreamWriteIndexStep.NAME + " until it is not the write index anymore",
            explainIndex(client(), backingIndexName).get("step"), is(CheckNotDataStreamWriteIndexStep.NAME)),
            30, TimeUnit.SECONDS);

        // Manual rollover the original index such that it's not the write index in the data stream anymore
        rolloverMaxOneDocCondition(client(), dataStream);

        assertBusy(() -> assertThat(indexExists(restoredIndexName), is(true)));
        assertBusy(() -> assertFalse(indexExists(backingIndexName)), 60, TimeUnit.SECONDS);
        assertBusy(() -> assertThat(explainIndex(client(), restoredIndexName).get("step"), is(PhaseCompleteStep.NAME)), 30,
            TimeUnit.SECONDS);
    }

    public void testReadOnlyAction() throws Exception {
        String policyName = "logs-policy";
        createNewSingletonPolicy(client(), policyName, "warm", new ReadOnlyAction());

        createComposableTemplate(client(), "logs-template", "logs-foo*", getTemplate(policyName));
        String dataStream = "logs-foo";
        indexDocument(client(), dataStream, true);

        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStream, 1);
        assertBusy(() -> assertThat(
            "index must wait in the " + CheckNotDataStreamWriteIndexStep.NAME + " until it is not the write index anymore",
            explainIndex(client(), backingIndexName).get("step"), is(CheckNotDataStreamWriteIndexStep.NAME)),
            30, TimeUnit.SECONDS);

        // Manual rollover the original index such that it's not the write index in the data stream anymore
        rolloverMaxOneDocCondition(client(), dataStream);

        assertBusy(() -> assertThat(explainIndex(client(), backingIndexName).get("step"), is(PhaseCompleteStep.NAME)), 30,
            TimeUnit.SECONDS);
        assertThat(getOnlyIndexSettings(client(), backingIndexName).get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()),
            equalTo("true"));
    }

    public void testFreezeAction() throws Exception {
        String policyName = "logs-policy";
        createNewSingletonPolicy(client(), policyName, "cold", new FreezeAction());

        createComposableTemplate(client(), "logs-template", "logs-foo*", getTemplate(policyName));
        String dataStream = "logs-foo";
        indexDocument(client(), dataStream, true);

        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStream, 1);
        assertBusy(() -> assertThat(
            "index must wait in the " + CheckNotDataStreamWriteIndexStep.NAME + " until it is not the write index anymore",
            explainIndex(client(), backingIndexName).get("step"), is(CheckNotDataStreamWriteIndexStep.NAME)),
            30, TimeUnit.SECONDS);

        // Manual rollover the original index such that it's not the write index in the data stream anymore
        rolloverMaxOneDocCondition(client(), dataStream);

        assertBusy(() -> assertThat(explainIndex(client(), backingIndexName).get("step"), is(PhaseCompleteStep.NAME)), 30,
            TimeUnit.SECONDS);

        Map<String, Object> settings = getOnlyIndexSettings(client(), backingIndexName);
        assertThat(settings.get(IndexMetadata.SETTING_BLOCKS_WRITE), equalTo("true"));
        assertThat(settings.get(IndexSettings.INDEX_SEARCH_THROTTLED.getKey()), equalTo("true"));
        assertThat(settings.get("index.frozen"), equalTo("true"));
    }

    public void testForceMergeAction() throws Exception {
        String policyName = "logs-policy";
        createNewSingletonPolicy(client(), policyName, "warm", new ForceMergeAction(1, null));

        createComposableTemplate(client(), "logs-template", "logs-foo*", getTemplate(policyName));
        String dataStream = "logs-foo";
        indexDocument(client(), dataStream, true);

        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStream, 1);
        assertBusy(() -> assertThat(
            "index must wait in the " + CheckNotDataStreamWriteIndexStep.NAME + " until it is not the write index anymore",
            explainIndex(client(), backingIndexName).get("step"), is(CheckNotDataStreamWriteIndexStep.NAME)),
            30, TimeUnit.SECONDS);

        // Manual rollover the original index such that it's not the write index in the data stream anymore
        rolloverMaxOneDocCondition(client(), dataStream);

        assertBusy(() -> assertThat(explainIndex(client(), backingIndexName).get("step"), is(PhaseCompleteStep.NAME)), 30,
            TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    public void testGetDataStreamReturnsILMPolicy() throws Exception {
        String policyName = "logs-policy";
        createComposableTemplate(client(), "logs-template", "logs-foo*", getTemplate(policyName));
        String dataStream = "logs-foo";
        indexDocument(client(), dataStream, true);

        Request explainRequest = new Request("GET",   "/_data_stream/logs-foo");
        Response response = client().performRequest(explainRequest);
        Map<String, Object> responseMap;
        try (InputStream is = response.getEntity().getContent()) {
            responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
        }

        List<Object> dataStreams = (List<Object>) responseMap.get("data_streams");
        assertThat(dataStreams.size(), is(1));
        Map<String, Object> logsDataStream = (Map<String, Object>) dataStreams.get(0);
        assertThat(logsDataStream.get("ilm_policy"), is(policyName));
    }

    private static Template getTemplate(String policyName) throws IOException {
        return new Template(getLifecycleSettings(policyName), null, null);
    }

    private static Settings getLifecycleSettings(String policyName) {
        return Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_NAME, policyName)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .build();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ilm.CheckNotDataStreamWriteIndexStep;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.DeleteStep;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.FreezeAction;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep;
import org.junit.Before;

import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.TimeSeriesRestDriver.createComposableTemplate;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createSnapshotRepo;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.explainIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getOnlyIndexSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getStepKeyForIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getTemplate;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.indexDocument;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.rolloverMaxOneDocCondition;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.waitAndGetShrinkIndexName;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class TimeSeriesDataStreamsIT extends ESRestTestCase {

    private String policyName;
    private String dataStream;
    private String template;

    @Before
    public void refreshAbstractions() {
        policyName = "policy-" + randomAlphaOfLength(5);
        dataStream = "logs-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        template = "template-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        logger.info(
            "--> running [{}] with data stream [{}], template [{}] and policy [{}]",
            getTestName(),
            dataStream,
            template,
            policyName
        );
    }

    public void testRolloverAction() throws Exception {
        createNewSingletonPolicy(client(), policyName, "hot", new RolloverAction(null, null, null, 1L, null, null, null, null, null, null));

        createComposableTemplate(client(), template, dataStream + "*", getTemplate(policyName));

        indexDocument(client(), dataStream, true);

        assertBusy(() -> {
            final var backingIndices = getDataStreamBackingIndexNames(dataStream);
            assertEquals(2, backingIndices.size());
            assertTrue(Boolean.parseBoolean((String) getIndexSettingsAsMap(backingIndices.get(1)).get("index.hidden")));
        });
        assertBusy(() -> {
            final var backingIndices = getDataStreamBackingIndexNames(dataStream);
            assertEquals(PhaseCompleteStep.finalStep("hot").getKey(), getStepKeyForIndex(client(), backingIndices.get(0)));
        });
    }

    public void testRolloverIsSkippedOnManualDataStreamRollover() throws Exception {
        createNewSingletonPolicy(client(), policyName, "hot", new RolloverAction(null, null, null, 2L, null, null, null, null, null, null));

        createComposableTemplate(client(), template, dataStream + "*", getTemplate(policyName));

        indexDocument(client(), dataStream, true);

        String firstGenerationIndex = getDataStreamBackingIndexNames(dataStream).get(0);
        assertBusy(
            () -> assertThat(getStepKeyForIndex(client(), firstGenerationIndex).name(), equalTo(WaitForRolloverReadyStep.NAME)),
            30,
            TimeUnit.SECONDS
        );

        rolloverMaxOneDocCondition(client(), dataStream);
        assertBusy(() -> {
            final var backingIndices = getDataStreamBackingIndexNames(dataStream);
            assertEquals(2, backingIndices.size());
        }, 30, TimeUnit.SECONDS);

        // even though the first index doesn't have 2 documents to fulfill the rollover condition, it should complete the rollover action
        // because it's not the write index anymore
        assertBusy(
            () -> assertThat(getStepKeyForIndex(client(), firstGenerationIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey())),
            30,
            TimeUnit.SECONDS
        );
    }

    public void testShrinkActionInPolicyWithoutHotPhase() throws Exception {
        createNewSingletonPolicy(client(), policyName, "warm", new ShrinkAction(1, null, false));
        createComposableTemplate(client(), template, dataStream + "*", getTemplate(policyName));
        indexDocument(client(), dataStream, true);

        String backingIndexName = getDataStreamBackingIndexNames(dataStream).get(0);
        assertBusy(
            () -> assertThat(
                "original index must wait in the " + CheckNotDataStreamWriteIndexStep.NAME + " until it is not the write index anymore",
                explainIndex(client(), backingIndexName).get("step"),
                is(CheckNotDataStreamWriteIndexStep.NAME)
            ),
            30,
            TimeUnit.SECONDS
        );

        // Manual rollover the original index such that it's not the write index in the data stream anymore
        rolloverMaxOneDocCondition(client(), dataStream);
        // Wait for rollover to happen
        assertBusy(
            () -> assertEquals("the rollover action created the rollover index", 2, getDataStreamBackingIndexNames(dataStream).size()),
            30,
            TimeUnit.SECONDS
        );

        String shrunkenIndex = waitAndGetShrinkIndexName(client(), backingIndexName);
        awaitIndexExists(shrunkenIndex, TimeValue.timeValueSeconds(30));
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), shrunkenIndex), equalTo(PhaseCompleteStep.finalStep("warm").getKey())));
        assertBusy(() -> assertThat("the original index must've been deleted", indexExists(backingIndexName), is(false)));
    }

    public void testSearchableSnapshotAction() throws Exception {
        String snapshotRepo = randomAlphaOfLengthBetween(5, 10);
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createNewSingletonPolicy(client(), policyName, "cold", new SearchableSnapshotAction(snapshotRepo));

        createComposableTemplate(client(), template, dataStream + "*", getTemplate(policyName));
        indexDocument(client(), dataStream, true);

        String backingIndexName = getDataStreamBackingIndexNames(dataStream).get(0);
        String restoredIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + backingIndexName;

        assertBusy(
            () -> assertThat(
                "original index must wait in the " + CheckNotDataStreamWriteIndexStep.NAME + " until it is not the write index anymore",
                explainIndex(client(), backingIndexName).get("step"),
                is(CheckNotDataStreamWriteIndexStep.NAME)
            ),
            30,
            TimeUnit.SECONDS
        );

        // Manual rollover the original index such that it's not the write index in the data stream anymore
        rolloverMaxOneDocCondition(client(), dataStream);

        awaitIndexExists(restoredIndexName);
        awaitIndexDoesNotExist(backingIndexName, TimeValue.timeValueSeconds(60));
        assertBusy(
            () -> assertThat(explainIndex(client(), restoredIndexName).get("step"), is(PhaseCompleteStep.NAME)),
            30,
            TimeUnit.SECONDS
        );
    }

    public void testReadOnlyAction() throws Exception {
        createNewSingletonPolicy(client(), policyName, "warm", new ReadOnlyAction());

        createComposableTemplate(client(), template, dataStream + "*", getTemplate(policyName));
        indexDocument(client(), dataStream, true);

        String backingIndexName = getDataStreamBackingIndexNames(dataStream).get(0);
        assertBusy(
            () -> assertThat(
                "index must wait in the " + CheckNotDataStreamWriteIndexStep.NAME + " until it is not the write index anymore",
                explainIndex(client(), backingIndexName).get("step"),
                is(CheckNotDataStreamWriteIndexStep.NAME)
            ),
            30,
            TimeUnit.SECONDS
        );

        // Manual rollover the original index such that it's not the write index in the data stream anymore
        rolloverMaxOneDocCondition(client(), dataStream);

        assertBusy(() -> {
            assertThat(explainIndex(client(), backingIndexName).get("step"), is(PhaseCompleteStep.NAME));
            assertThat(
                getOnlyIndexSettings(client(), backingIndexName).get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()),
                equalTo("true")
            );
        }, 30, TimeUnit.SECONDS);
    }

    public void testFreezeAction() throws Exception {
        createNewSingletonPolicy(client(), policyName, "cold", FreezeAction.INSTANCE);
        createComposableTemplate(client(), template, dataStream + "*", getTemplate(policyName));
        indexDocument(client(), dataStream, true);

        String backingIndexName = getDataStreamBackingIndexNames(dataStream).get(0);
        assertBusy(
            () -> assertThat(
                "index must wait in the " + CheckNotDataStreamWriteIndexStep.NAME + " until it is not the write index anymore",
                explainIndex(client(), backingIndexName).get("step"),
                is(CheckNotDataStreamWriteIndexStep.NAME)
            ),
            30,
            TimeUnit.SECONDS
        );

        // Manual rollover the original index such that it's not the write index in the data stream anymore
        rolloverMaxOneDocCondition(client(), dataStream);

        assertBusy(() -> {
            assertThat(explainIndex(client(), backingIndexName).get("step"), is(PhaseCompleteStep.NAME));
            Map<String, Object> settings = getOnlyIndexSettings(client(), backingIndexName);
            assertNull(settings.get("index.frozen"));
        }, 30, TimeUnit.SECONDS);

    }

    public void checkForceMergeAction(String codec) throws Exception {
        createNewSingletonPolicy(client(), policyName, "warm", new ForceMergeAction(1, codec));
        createComposableTemplate(client(), template, dataStream + "*", getTemplate(policyName));
        indexDocument(client(), dataStream, true);

        String backingIndexName = getDataStreamBackingIndexNames(dataStream).get(0);
        assertBusy(
            () -> assertThat(
                "index must wait in the " + CheckNotDataStreamWriteIndexStep.NAME + " until it is not the write index anymore",
                explainIndex(client(), backingIndexName).get("step"),
                is(CheckNotDataStreamWriteIndexStep.NAME)
            ),
            30,
            TimeUnit.SECONDS
        );

        // Manual rollover the original index such that it's not the write index in the data stream anymore
        rolloverMaxOneDocCondition(client(), dataStream);

        assertBusy(() -> {
            assertThat(explainIndex(client(), backingIndexName).get("step"), is(PhaseCompleteStep.NAME));
            Map<String, Object> settings = getOnlyIndexSettings(client(), backingIndexName);
            assertThat(settings.get(EngineConfig.INDEX_CODEC_SETTING.getKey()), equalTo(codec));
            assertThat(settings.containsKey(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo(false));
        }, 30, TimeUnit.SECONDS);
    }

    public void testForceMergeAction() throws Exception {
        checkForceMergeAction(null);
    }

    public void testForceMergeActionWithCompressionCodec() throws Exception {
        checkForceMergeAction("best_compression");
    }

    @SuppressWarnings("unchecked")
    public void testGetDataStreamReturnsILMPolicy() throws Exception {
        createComposableTemplate(client(), template, dataStream + "*", getTemplate(policyName));
        indexDocument(client(), dataStream, true);

        Request explainRequest = new Request("GET", "/_data_stream/" + dataStream);
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

    public void testDeleteOnlyIndexInDataStreamDeletesDataStream() throws Exception {
        createNewSingletonPolicy(client(), policyName, "delete", DeleteAction.NO_SNAPSHOT_DELETE);
        createComposableTemplate(client(), template, dataStream + "*", getTemplate(policyName));
        indexDocument(client(), dataStream, true);

        assertBusy(() -> {
            Request r = new Request("GET", "/_data_stream/" + dataStream);
            Exception e = expectThrows(Exception.class, () -> client().performRequest(r));
            assertThat(e.getMessage(), containsString("no such index [" + dataStream + "]"));
        });
    }

    @SuppressWarnings("unchecked")
    public void testDataStreamWithMultipleIndicesAndWriteIndexInDeletePhase() throws Exception {
        createComposableTemplate(client(), template, dataStream + "*", Template.builder().build());
        indexDocument(client(), dataStream, true);

        createNewSingletonPolicy(client(), policyName, "delete", DeleteAction.NO_SNAPSHOT_DELETE);
        // let's update the index template so the new write index (after rollover) is managed by an ILM policy that sents it to the
        // delete step - note that we'll have here a data stream with generation 000001 not managed and the write index 000002 in the
        // delete phase (the write index in this case, being not the only backing index must NOT be deleted).
        createComposableTemplate(client(), template, dataStream + "*", getTemplate(policyName));

        client().performRequest(new Request("POST", dataStream + "/_rollover"));
        indexDocument(client(), dataStream, true);

        String secondGenerationIndex = getDataStreamBackingIndexNames(dataStream).get(1);
        assertBusy(() -> {
            Request explainRequest = new Request("GET", "/_data_stream/" + dataStream);
            Response response = client().performRequest(explainRequest);
            Map<String, Object> responseMap;
            try (InputStream is = response.getEntity().getContent()) {
                responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            }

            List<Object> dataStreams = (List<Object>) responseMap.get("data_streams");
            assertThat(dataStreams.size(), is(1));
            Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);

            List<Object> indices = (List<Object>) dataStream.get("indices");
            // no index should be deleted
            assertThat(indices.size(), is(2));

            Map<String, Object> explainIndex = explainIndex(client(), secondGenerationIndex);
            assertThat(explainIndex.get("action"), is(DeleteAction.NAME));
            assertThat(explainIndex.get("step"), oneOf(DeleteStep.NAME, ErrorStep.NAME));
            assertThat((Integer) explainIndex.get("failed_step_retry_count"), is(greaterThan(1)));
        });

        // rolling the data stream again would see 000002 not be the write index anymore and should be deleted automatically
        client().performRequest(new Request("POST", dataStream + "/_rollover"));

        assertBusy(() -> assertThat(indexExists(secondGenerationIndex), is(false)));
    }

}

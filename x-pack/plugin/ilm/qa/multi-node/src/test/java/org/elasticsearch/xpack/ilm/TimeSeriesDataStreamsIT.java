/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.ReplaceDataStreamBackingIndexStep;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.TimeSeriesRestDriver.createComposableTemplate;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createFullPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createSnapshotRepo;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.explainIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getStepKeyForIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.indexDocument;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.rolloverMaxOneDocCondition;
import static org.elasticsearch.xpack.core.ilm.ShrinkAction.CONDITIONAL_SKIP_SHRINK_STEP;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class TimeSeriesDataStreamsIT extends ESRestTestCase {

    private static final String FAILED_STEP_RETRY_COUNT_FIELD = "failed_step_retry_count";

    public void testRolloverAction() throws Exception {
        String policyName = "logs-policy";
        createNewSingletonPolicy(client(), policyName, "hot", new RolloverAction(null, null, 1L));

        String mapping = "{\n" +
            "      \"properties\": {\n" +
            "        \"@timestamp\": {\n" +
            "          \"type\": \"date\"\n" +
            "        }\n" +
            "      }\n" +
            "    }";
        Settings lifecycleNameSetting = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyName).build();
        Template template = new Template(lifecycleNameSetting, new CompressedXContent(mapping), null);
        createComposableTemplate(client(), "logs-template", "logs-foo*", template);

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

        String mapping = "{\n" +
            "      \"properties\": {\n" +
            "        \"@timestamp\": {\n" +
            "          \"type\": \"date\"\n" +
            "        }\n" +
            "      }\n" +
            "    }";
        Settings settings = Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_NAME, policyName)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .build();
        Template template = new Template(settings, new CompressedXContent(mapping), null);
        createComposableTemplate(client(), "logs-template", "logs-foo*", template);

        String dataStream = "logs-foo";
        indexDocument(client(), dataStream, true);

        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStream, 1);
        String shrunkenIndex = ShrinkAction.SHRUNKEN_INDEX_PREFIX + backingIndexName;
        assertBusy(() -> assertThat(
            "original index must wait in the " + CONDITIONAL_SKIP_SHRINK_STEP + " until it is not the write index anymore",
            (Integer) explainIndex(client(), backingIndexName).get(FAILED_STEP_RETRY_COUNT_FIELD), greaterThanOrEqualTo(1)),
            30, TimeUnit.SECONDS);

        // Manual rollover the original index such that it's not the write index in the data stream anymore
        rolloverMaxOneDocCondition(client(), dataStream);

        assertBusy(() -> assertTrue(indexExists(shrunkenIndex)), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), shrunkenIndex), equalTo(PhaseCompleteStep.finalStep("warm").getKey())));
        assertThat("the original index must've been deleted", indexExists(backingIndexName), is(false));
    }

    public void testShrinkAfterRollover() throws Exception {
        String policyName = "logs-policy";
        createFullPolicy(client(), policyName, TimeValue.ZERO);

        String mapping = "{\n" +
            "      \"properties\": {\n" +
            "        \"@timestamp\": {\n" +
            "          \"type\": \"date\"\n" +
            "        }\n" +
            "      }\n" +
            "    }";
        Settings settings = Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_NAME, policyName)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .build();
        Template template = new Template(settings, new CompressedXContent(mapping), null);
        createComposableTemplate(client(), "logs-template", "logs-foo*", template);

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

        String mapping = "{\n" +
            "      \"properties\": {\n" +
            "        \"@timestamp\": {\n" +
            "          \"type\": \"date\"\n" +
            "        }\n" +
            "      }\n" +
            "    }";
        Settings settings = Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_NAME, policyName)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .build();
        Template template = new Template(settings, new CompressedXContent(mapping), null);
        createComposableTemplate(client(), "logs-template", "logs-foo*", template);
        String dataStream = "logs-foo";
        indexDocument(client(), dataStream, true);

        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStream, 1);
        String restoredIndexName = SearchableSnapshotAction.RESTORED_INDEX_PREFIX + backingIndexName;

        assertBusy(() -> assertThat(indexExists(restoredIndexName), is(true)));
        assertBusy(() -> assertThat(
            "original index must wait in the " + ReplaceDataStreamBackingIndexStep.NAME + " until it is not the write index anymore",
            (Integer) explainIndex(client(), backingIndexName).get(FAILED_STEP_RETRY_COUNT_FIELD), greaterThanOrEqualTo(1)),
            30, TimeUnit.SECONDS);

        // Manual rollover the original index such that it's not the write index in the data stream anymore
        rolloverMaxOneDocCondition(client(), dataStream);

        assertBusy(() -> assertFalse(indexExists(backingIndexName)), 60, TimeUnit.SECONDS);
        assertBusy(() -> assertThat(explainIndex(client(), restoredIndexName).get("step"), is(PhaseCompleteStep.NAME)), 30,
            TimeUnit.SECONDS);
    }
}

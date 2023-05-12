/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.dlm.ExplainIndexDataLifecycle;
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
import org.elasticsearch.dlm.DataLifecyclePlugin;
import org.elasticsearch.dlm.DataLifecycleService;
import org.elasticsearch.dlm.action.ExplainDataLifecycleAction;
import org.elasticsearch.dlm.action.PutDataLifecycleAction;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleRequest;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleResponse;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleExplainResponse;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.DataStream.getDefaultBackingIndexName;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class DataAndIndexLifecycleMixingTests extends ESIntegTestCase {

    private String policy;
    private String dataStreamName;
    private String indexTemplateName;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class, DataLifecyclePlugin.class, DataStreamsPlugin.class);
    }

    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(DataLifecycleService.DLM_POLL_INTERVAL, "1s");
        settings.put(DataLifecycle.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=1,max_docs=1");
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        settings.put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s");
        settings.put(LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING.getKey(), false);
        settings.put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, false);
        return settings.build();
    }

    @Before
    public void refreshAbstractions() {
        policy = "policy-" + randomAlphaOfLength(5);
        dataStreamName = "datastream-" + randomAlphaOfLengthBetween(10, 15).toLowerCase(Locale.ROOT);
        indexTemplateName = "indextemplate-" + randomAlphaOfLengthBetween(10, 15).toLowerCase(Locale.ROOT);
    }

    public void testIndexTemplateSwapsILMForDLM() throws Exception {
        // ILM rolls over every 2 documents
        RolloverAction rolloverIlmAction = new RolloverAction(RolloverConditions.newBuilder().addMaxIndexDocsCondition(2L).build());
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, Map.of(rolloverIlmAction.getWriteableName(), rolloverIlmAction));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, Map.of("hot", hotPhase));
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());

        putComposableIndexTemplate(
            indexTemplateName,
            null,
            List.of(dataStreamName + "*"),
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).build(),
            null,
            null
        );
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 2);

        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
            List<Index> backingIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
            assertThat(backingIndices.size(), equalTo(2));
            String backingIndex = backingIndices.get(0).getName();
            assertThat(backingIndex, backingIndexEqualTo(dataStreamName, 1));
            String writeIndex = backingIndices.get(1).getName();
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
        });

        // data stream was rolled over and has 2 indices managed by ILM
        assertBusy(() -> {
            String firstGenerationIndex = getDefaultBackingIndexName(dataStreamName, 1);
            String secondGenerationIndex = getDefaultBackingIndexName(dataStreamName, 2);
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(firstGenerationIndex, secondGenerationIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE, explainRequest).get();

            IndexLifecycleExplainResponse firstGenerationExplain = explainResponse.getIndexResponses().get(firstGenerationIndex);
            assertThat(firstGenerationExplain.managedByILM(), is(true));
            assertThat(firstGenerationExplain.getPhase(), is("hot"));
            assertThat(firstGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            IndexLifecycleExplainResponse secondGenerationExplain = explainResponse.getIndexResponses().get(secondGenerationIndex);
            assertThat(secondGenerationExplain.managedByILM(), is(true));
            assertThat(secondGenerationExplain.getPhase(), is("hot"));
            assertThat(secondGenerationExplain.getStep(), is(WaitForRolloverReadyStep.NAME));
        });

        // let's update the index template to remove the ILM configuration and configured DLM
        // note that this index template change will NOT configure DLM on the data stream, only for **new** data streams

        // to transition this existing data stream we'll need to use the PUT _lifecycle API

        // we'll rollover the data stream by indexing 2 documents (like ILM expects) and assert that the rollover happens once so the
        // data stream has 3 backing indices, two managed by ILM and one will be UNMANAGED
        putComposableIndexTemplate(indexTemplateName, null, List.of(dataStreamName + "*"), Settings.EMPTY, null, new DataLifecycle());

        indexDocs(dataStreamName, 2);

        // data stream was rolled over and has 3 indices, two managed by ILM and one will be UNMANAGED
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(3));

        });

        assertBusy(() -> {
            String firstGenerationIndex = getDefaultBackingIndexName(dataStreamName, 1);
            String secondGenerationIndex = getDefaultBackingIndexName(dataStreamName, 2);
            String thirdGenerationIndex = getDefaultBackingIndexName(dataStreamName, 3);
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(
                firstGenerationIndex,
                secondGenerationIndex,
                thirdGenerationIndex
            );
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE, explainRequest).get();

            IndexLifecycleExplainResponse firstGenerationExplain = explainResponse.getIndexResponses().get(firstGenerationIndex);
            assertThat(firstGenerationExplain.managedByILM(), is(true));
            assertThat(firstGenerationExplain.getPhase(), is("hot"));
            assertThat(firstGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            IndexLifecycleExplainResponse secondGenerationExplain = explainResponse.getIndexResponses().get(secondGenerationIndex);
            assertThat(secondGenerationExplain.managedByILM(), is(true));
            assertThat(secondGenerationExplain.getPhase(), is("hot"));
            assertThat(secondGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            // the write index is unmanaged
            IndexLifecycleExplainResponse thirdGenerationExplain = explainResponse.getIndexResponses().get(thirdGenerationIndex);
            assertThat(thirdGenerationExplain.managedByILM(), is(false));

            ExplainDataLifecycleAction.Response dlmExplainResponse = client().execute(
                ExplainDataLifecycleAction.INSTANCE,
                new ExplainDataLifecycleAction.Request(new String[] { thirdGenerationIndex })
            ).actionGet();
            assertThat(dlmExplainResponse.getIndices().size(), is(1));
            ExplainIndexDataLifecycle writeIndexDLMExplain = dlmExplainResponse.getIndices().get(0);
            assertThat(writeIndexDLMExplain.isManagedByDLM(), is(false));
        });

        // let's migrate this data stream to use DLM starting with the next generation
        client().execute(
            PutDataLifecycleAction.INSTANCE,
            new PutDataLifecycleAction.Request(new String[] { dataStreamName }, TimeValue.timeValueDays(90))
        );

        // at this point we should be able to rollover the data stream by indexing only one document (as DLM is configured to)
        indexDocs(dataStreamName, 1);

        // data stream was rolled over and has 4 indices, 2 managed by ILM, the previous write index that was UNMANAGED will now be
        // managed by DLM and the new write index managed by DLM
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(4));

        });

        assertBusy(() -> {
            String firstGenerationIndex = getDefaultBackingIndexName(dataStreamName, 1);
            String secondGenerationIndex = getDefaultBackingIndexName(dataStreamName, 2);
            String thirdGenerationIndex = getDefaultBackingIndexName(dataStreamName, 3);
            String writeIndex = getDefaultBackingIndexName(dataStreamName, 4);
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(
                firstGenerationIndex,
                secondGenerationIndex,
                thirdGenerationIndex,
                writeIndex
            );
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE, explainRequest).get();

            IndexLifecycleExplainResponse firstGenerationExplain = explainResponse.getIndexResponses().get(firstGenerationIndex);
            assertThat(firstGenerationExplain.managedByILM(), is(true));
            assertThat(firstGenerationExplain.getPhase(), is("hot"));
            assertThat(firstGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            IndexLifecycleExplainResponse secondGenerationExplain = explainResponse.getIndexResponses().get(secondGenerationIndex);
            assertThat(secondGenerationExplain.managedByILM(), is(true));
            assertThat(secondGenerationExplain.getPhase(), is("hot"));
            assertThat(secondGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            IndexLifecycleExplainResponse thirdGenerationExplain = explainResponse.getIndexResponses().get(thirdGenerationIndex);
            assertThat(thirdGenerationExplain.managedByILM(), is(false));

            // the write index is managed by DLM
            ExplainDataLifecycleAction.Response dlmExplainResponse = client().execute(
                ExplainDataLifecycleAction.INSTANCE,
                new ExplainDataLifecycleAction.Request(new String[] { thirdGenerationIndex, writeIndex })
            ).actionGet();
            assertThat(dlmExplainResponse.getIndices().size(), is(2));
            for (ExplainIndexDataLifecycle index : dlmExplainResponse.getIndices()) {
                assertThat(index.isManagedByDLM(), is(true));
            }
        });
    }

    public void testUpdateIndexTemplateFromILMtoBothILMAndDLM() throws Exception {
        // ILM rolls over every 2 documents
        RolloverAction rolloverIlmAction = new RolloverAction(RolloverConditions.newBuilder().addMaxIndexDocsCondition(2L).build());
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, Map.of(rolloverIlmAction.getWriteableName(), rolloverIlmAction));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, Map.of("hot", hotPhase));
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());

        putComposableIndexTemplate(
            indexTemplateName,
            null,
            List.of(dataStreamName + "*"),
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).build(),
            null,
            null
        );
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 2);

        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
            List<Index> backingIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
            assertThat(backingIndices.size(), equalTo(2));
        });

        // data stream was rolled over and has 2 indices managed by ILM
        assertBusy(() -> {
            String firstGenerationIndex = getDefaultBackingIndexName(dataStreamName, 1);
            String secondGenerationIndex = getDefaultBackingIndexName(dataStreamName, 2);
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(firstGenerationIndex, secondGenerationIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE, explainRequest).get();

            IndexLifecycleExplainResponse firstGenerationExplain = explainResponse.getIndexResponses().get(firstGenerationIndex);
            assertThat(firstGenerationExplain.managedByILM(), is(true));
            assertThat(firstGenerationExplain.getPhase(), is("hot"));
            assertThat(firstGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            IndexLifecycleExplainResponse secondGenerationExplain = explainResponse.getIndexResponses().get(secondGenerationIndex);
            assertThat(secondGenerationExplain.managedByILM(), is(true));
            assertThat(secondGenerationExplain.getPhase(), is("hot"));
            assertThat(secondGenerationExplain.getStep(), is(WaitForRolloverReadyStep.NAME));
        });

        // let's update the index template to add the DLM configuration next to the ILM configuration
        // note that this index template change will NOT configure DLM on the data stream, only for **new** data streams

        // to transition this existing data stream we'll need to use the PUT _lifecycle API AND update the index template to either:
        // * remove the ILM configuration so ILM is not configured of the next backing indices anymore
        // * leave the ILM configuration in the index template and change the value of the prefer_ilm setting to false
        // We'll implement the 2nd option in this test, namely changing the prefer_ilm setting to false

        // we'll rollover the data stream by indexing 2 documents (like ILM expects) and assert that the rollover happens once so the
        // data stream has 3 backing indices, all 3 managed by ILM
        putComposableIndexTemplate(
            indexTemplateName,
            null,
            List.of(dataStreamName + "*"),
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).build(),
            null,
            new DataLifecycle()
        );

        indexDocs(dataStreamName, 2);

        // data stream was rolled over and has 3 indices, ALL managed by ILM
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(3));

        });

        assertBusy(() -> {
            String firstGenerationIndex = getDefaultBackingIndexName(dataStreamName, 1);
            String secondGenerationIndex = getDefaultBackingIndexName(dataStreamName, 2);
            String thirdGenerationIndex = getDefaultBackingIndexName(dataStreamName, 3);
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(
                firstGenerationIndex,
                secondGenerationIndex,
                thirdGenerationIndex
            );
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE, explainRequest).get();

            IndexLifecycleExplainResponse firstGenerationExplain = explainResponse.getIndexResponses().get(firstGenerationIndex);
            assertThat(firstGenerationExplain.managedByILM(), is(true));
            assertThat(firstGenerationExplain.getPhase(), is("hot"));
            assertThat(firstGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            IndexLifecycleExplainResponse secondGenerationExplain = explainResponse.getIndexResponses().get(secondGenerationIndex);
            assertThat(secondGenerationExplain.managedByILM(), is(true));
            assertThat(secondGenerationExplain.getPhase(), is("hot"));
            assertThat(secondGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            IndexLifecycleExplainResponse thirdGenerationExplain = explainResponse.getIndexResponses().get(thirdGenerationIndex);
            assertThat(thirdGenerationExplain.managedByILM(), is(true));
            assertThat(thirdGenerationExplain.getStep(), is(WaitForRolloverReadyStep.NAME));
        });

        // let's migrate this data stream to use DLM starting with the next generation
        client().execute(
            PutDataLifecycleAction.INSTANCE,
            new PutDataLifecycleAction.Request(new String[] { dataStreamName }, TimeValue.timeValueDays(90))
        );

        putComposableIndexTemplate(
            indexTemplateName,
            null,
            List.of(dataStreamName + "*"),
            Settings.builder().put(IndexSettings.PREFER_ILM, false).put(LifecycleSettings.LIFECYCLE_NAME, policy).build(),
            null,
            new DataLifecycle()
        );

        // note that all indices now are still managed by ILM, so we index 2 documents. the new write index will be managed by DLM
        indexDocs(dataStreamName, 2);

        // data stream was rolled over and has 4 indices, 3 managed by ILM, and the write index managed by DLM
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(4));

        });

        assertBusy(() -> {
            String firstGenerationIndex = getDefaultBackingIndexName(dataStreamName, 1);
            String secondGenerationIndex = getDefaultBackingIndexName(dataStreamName, 2);
            String thirdGenerationIndex = getDefaultBackingIndexName(dataStreamName, 3);
            String writeIndex = getDefaultBackingIndexName(dataStreamName, 4);
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(
                firstGenerationIndex,
                secondGenerationIndex,
                thirdGenerationIndex,
                writeIndex
            );
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE, explainRequest).get();

            IndexLifecycleExplainResponse firstGenerationExplain = explainResponse.getIndexResponses().get(firstGenerationIndex);
            assertThat(firstGenerationExplain.managedByILM(), is(true));
            assertThat(firstGenerationExplain.getPhase(), is("hot"));
            assertThat(firstGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            IndexLifecycleExplainResponse secondGenerationExplain = explainResponse.getIndexResponses().get(secondGenerationIndex);
            assertThat(secondGenerationExplain.managedByILM(), is(true));
            assertThat(secondGenerationExplain.getPhase(), is("hot"));
            assertThat(secondGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            IndexLifecycleExplainResponse thirdGenerationExplain = explainResponse.getIndexResponses().get(thirdGenerationIndex);
            assertThat(thirdGenerationExplain.managedByILM(), is(true));
            assertThat(thirdGenerationExplain.getPhase(), is("hot"));
            assertThat(thirdGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            // the write index is managed by DLM
            ExplainDataLifecycleAction.Response dlmExplainResponse = client().execute(
                ExplainDataLifecycleAction.INSTANCE,
                new ExplainDataLifecycleAction.Request(new String[] { writeIndex })
            ).actionGet();
            assertThat(dlmExplainResponse.getIndices().size(), is(1));
            ExplainIndexDataLifecycle dlmExplain = dlmExplainResponse.getIndices().get(0);
            assertThat(dlmExplain.isManagedByDLM(), is(true));
            assertThat(dlmExplain.getIndex(), is(writeIndex));
        });
    }

    public void testUpdateIndexTemplateToDLMPreference() throws Exception {
        // ILM rolls over every 2 documents
        RolloverAction rolloverIlmAction = new RolloverAction(RolloverConditions.newBuilder().addMaxIndexDocsCondition(2L).build());
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, Map.of(rolloverIlmAction.getWriteableName(), rolloverIlmAction));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, Map.of("hot", hotPhase));
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());

        putComposableIndexTemplate(
            indexTemplateName,
            null,
            List.of(dataStreamName + "*"),
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).build(),
            null,
            null
        );
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 2);

        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
            List<Index> backingIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
            assertThat(backingIndices.size(), equalTo(2));
        });

        // data stream was rolled over and has 2 indices managed by ILM
        assertBusy(() -> {
            String firstGenerationIndex = getDefaultBackingIndexName(dataStreamName, 1);
            String secondGenerationIndex = getDefaultBackingIndexName(dataStreamName, 2);
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(firstGenerationIndex, secondGenerationIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE, explainRequest).get();

            IndexLifecycleExplainResponse firstGenerationExplain = explainResponse.getIndexResponses().get(firstGenerationIndex);
            assertThat(firstGenerationExplain.managedByILM(), is(true));
            assertThat(firstGenerationExplain.getPhase(), is("hot"));
            assertThat(firstGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            IndexLifecycleExplainResponse secondGenerationExplain = explainResponse.getIndexResponses().get(secondGenerationIndex);
            assertThat(secondGenerationExplain.managedByILM(), is(true));
            assertThat(secondGenerationExplain.getPhase(), is("hot"));
            assertThat(secondGenerationExplain.getStep(), is(WaitForRolloverReadyStep.NAME));
        });

        // let's update the index template to add the DLM configuration next to the ILM configuration, and configure the management
        // preference to be DLM using the prefer_ilm setting
        // note that this index template change will NOT configure DLM on the data stream, only for **new** data streams

        // to transition this existing data stream we'll need to use the PUT _lifecycle API

        // we'll rollover the data stream by indexing 2 documents (like ILM expects) and assert that the rollover happens once so the
        // data stream has 3 backing indices, all 3 managed by ILM
        putComposableIndexTemplate(
            indexTemplateName,
            null,
            List.of(dataStreamName + "*"),
            Settings.builder().put(IndexSettings.PREFER_ILM, false).put(LifecycleSettings.LIFECYCLE_NAME, policy).build(),
            null,
            new DataLifecycle()
        );

        indexDocs(dataStreamName, 2);

        // data stream was rolled over and has 3 indices, ALL managed by ILM, however the 3rd index has prefer_ilm: false now (so if a user
        // configures DLM for the data stream this index will not be managed by ILM anymore, but will be picked up by DLM)
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(3));

        });

        assertBusy(() -> {
            String firstGenerationIndex = getDefaultBackingIndexName(dataStreamName, 1);
            String secondGenerationIndex = getDefaultBackingIndexName(dataStreamName, 2);
            String thirdGenerationIndex = getDefaultBackingIndexName(dataStreamName, 3);
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(
                firstGenerationIndex,
                secondGenerationIndex,
                thirdGenerationIndex
            );
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE, explainRequest).get();

            IndexLifecycleExplainResponse firstGenerationExplain = explainResponse.getIndexResponses().get(firstGenerationIndex);
            assertThat(firstGenerationExplain.managedByILM(), is(true));
            assertThat(firstGenerationExplain.getPhase(), is("hot"));
            assertThat(firstGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            IndexLifecycleExplainResponse secondGenerationExplain = explainResponse.getIndexResponses().get(secondGenerationIndex);
            assertThat(secondGenerationExplain.managedByILM(), is(true));
            assertThat(secondGenerationExplain.getPhase(), is("hot"));
            assertThat(secondGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            IndexLifecycleExplainResponse thirdGenerationExplain = explainResponse.getIndexResponses().get(thirdGenerationIndex);
            assertThat(thirdGenerationExplain.managedByILM(), is(true));
            assertThat(thirdGenerationExplain.getStep(), is(WaitForRolloverReadyStep.NAME));
        });

        // let's migrate this data stream to use DLM starting with the current generation (note that the 3rd generation index was created
        // after we updated the index template to have prefer_ilm: false, so the 3rd generation index will no receive both ILM and DLM
        // configurations and because its prefer_ilm: false setting, it will switch from being managed by ILM to DLM)
        client().execute(
            PutDataLifecycleAction.INSTANCE,
            new PutDataLifecycleAction.Request(new String[] { dataStreamName }, TimeValue.timeValueDays(90))
        );

        // at this point, the write index of the data stream is managed by DLM and not by ILM anymore so we can just index one document
        // to trigger the rollover
        indexDocs(dataStreamName, 1);

        // data stream was rolled over and has 4 indices, 2 managed by ILM, and 2 managed by DLM
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(4));

        });

        assertBusy(() -> {
            String firstGenerationIndex = getDefaultBackingIndexName(dataStreamName, 1);
            String secondGenerationIndex = getDefaultBackingIndexName(dataStreamName, 2);
            String thirdGenerationIndex = getDefaultBackingIndexName(dataStreamName, 3);
            String writeIndex = getDefaultBackingIndexName(dataStreamName, 4);
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(
                firstGenerationIndex,
                secondGenerationIndex,
                thirdGenerationIndex,
                writeIndex
            );
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE, explainRequest).get();

            IndexLifecycleExplainResponse firstGenerationExplain = explainResponse.getIndexResponses().get(firstGenerationIndex);
            assertThat(firstGenerationExplain.managedByILM(), is(true));
            assertThat(firstGenerationExplain.getPhase(), is("hot"));
            assertThat(firstGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            IndexLifecycleExplainResponse secondGenerationExplain = explainResponse.getIndexResponses().get(secondGenerationIndex);
            assertThat(secondGenerationExplain.managedByILM(), is(true));
            assertThat(secondGenerationExplain.getPhase(), is("hot"));
            assertThat(secondGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            // ILM is not managing this index anymore
            IndexLifecycleExplainResponse thirdGenerationExplain = explainResponse.getIndexResponses().get(thirdGenerationIndex);
            assertThat(thirdGenerationExplain.managedByILM(), is(false));

            // the write index is managed by DLM
            ExplainDataLifecycleAction.Response dlmExplainResponse = client().execute(
                ExplainDataLifecycleAction.INSTANCE,
                new ExplainDataLifecycleAction.Request(new String[] { thirdGenerationIndex, writeIndex })
            ).actionGet();
            assertThat(dlmExplainResponse.getIndices().size(), is(2));
            for (ExplainIndexDataLifecycle index : dlmExplainResponse.getIndices()) {
                assertThat(index.isManagedByDLM(), is(true));
            }
        });
    }

    public void testUpdateIndexTemplateToMigrateFromDLMToILM() throws Exception {
        // starting with a data stream managed by DLM (rolling over every 1 doc)
        putComposableIndexTemplate(indexTemplateName, null, List.of(dataStreamName + "*"), null, null, new DataLifecycle());

        // this will create the data stream and trigger a rollover so we will end up with a data stream with 2 backing indices
        indexDocs(dataStreamName, 1);

        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(2));

        });

        assertBusy(() -> {
            String firstGenerationIndex = getDefaultBackingIndexName(dataStreamName, 1);
            String writeIndex = getDefaultBackingIndexName(dataStreamName, 2);

            // let's check the indices are managed by DLM
            ExplainDataLifecycleAction.Response dlmExplainResponse = client().execute(
                ExplainDataLifecycleAction.INSTANCE,
                new ExplainDataLifecycleAction.Request(new String[] { firstGenerationIndex, writeIndex })
            ).actionGet();
            assertThat(dlmExplainResponse.getIndices().size(), is(2));
            for (ExplainIndexDataLifecycle index : dlmExplainResponse.getIndices()) {
                assertThat(index.isManagedByDLM(), is(true));
            }
        });

        // ILM rolls over every 2 documents - create the policy
        RolloverAction rolloverIlmAction = new RolloverAction(RolloverConditions.newBuilder().addMaxIndexDocsCondition(2L).build());
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, Map.of(rolloverIlmAction.getWriteableName(), rolloverIlmAction));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, Map.of("hot", hotPhase));
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());

        // let's update the index template to remove the DLM configuration and replace it with an ILM configuration
        // note that this change will apply to new backing indices only. the write index will continue to be managed by DLM
        // so we'll trigger a rollover by indexing one document (the next write index, and all subsequent new generations will start being
        // managed by ILM)

        // note that simply removing the DLM configuration from the index template does NOT remove it from the data stream, however the
        // default value for the prefer_ilm setting is `true` so even though the new indices of the data stream will have both the DLM
        // lifecycle configuration (by virtue of being part of a data stream that has `lifecycle` configured) and the ILM lifecycle
        // configuration provided by the index template, ILM will take priority

        putComposableIndexTemplate(
            indexTemplateName,
            null,
            List.of(dataStreamName + "*"),
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).build(),
            null,
            null
        );

        indexDocs(dataStreamName, 1);

        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(3));

        });

        assertBusy(() -> {
            String firstGenerationIndex = getDefaultBackingIndexName(dataStreamName, 1);
            String secondGenerationIndex = getDefaultBackingIndexName(dataStreamName, 2);
            String writeIndex = getDefaultBackingIndexName(dataStreamName, 3);

            // let's check the previous indices are managed by DLM
            ExplainDataLifecycleAction.Response dlmExplainResponse = client().execute(
                ExplainDataLifecycleAction.INSTANCE,
                new ExplainDataLifecycleAction.Request(new String[] { firstGenerationIndex, secondGenerationIndex })
            ).actionGet();
            assertThat(dlmExplainResponse.getIndices().size(), is(2));
            for (ExplainIndexDataLifecycle index : dlmExplainResponse.getIndices()) {
                assertThat(index.isManagedByDLM(), is(true));
            }

            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(writeIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE, explainRequest).get();

            IndexLifecycleExplainResponse writeIndexExplain = explainResponse.getIndexResponses().get(writeIndex);
            assertThat(writeIndexExplain.managedByILM(), is(true));
            assertThat(writeIndexExplain.getPhase(), is("hot"));
            assertThat(writeIndexExplain.getStep(), is(WaitForRolloverReadyStep.NAME));
        });

        // rollover should now happen when indexing 2 documents (as configured in ILM)
        indexDocs(dataStreamName, 2);
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(new String[] { dataStreamName });
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(4));

        });

        // the new write index is also managed by ILM
        assertBusy(() -> {
            String thirdGenerationIndex = getDefaultBackingIndexName(dataStreamName, 3);
            String writeIndex = getDefaultBackingIndexName(dataStreamName, 4);

            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(thirdGenerationIndex, writeIndex);
            ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE, explainRequest).get();

            IndexLifecycleExplainResponse thirdGenerationExplain = explainResponse.getIndexResponses().get(thirdGenerationIndex);
            assertThat(thirdGenerationExplain.managedByILM(), is(true));
            assertThat(thirdGenerationExplain.getPhase(), is("hot"));
            assertThat(thirdGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            IndexLifecycleExplainResponse writeIndexExplain = explainResponse.getIndexResponses().get(writeIndex);
            assertThat(writeIndexExplain.managedByILM(), is(true));
            assertThat(writeIndexExplain.getPhase(), is("hot"));
            assertThat(writeIndexExplain.getStep(), is(WaitForRolloverReadyStep.NAME));
        });
    }

    static void indexDocs(String dataStream, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            bulkRequest.add(
                new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
                    .source(String.format(Locale.ROOT, "{\"%s\":\"%s\"}", DEFAULT_TIMESTAMP_FIELD, value), XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        String backingIndexPrefix = DataStream.BACKING_INDEX_PREFIX + dataStream;
        for (BulkItemResponse itemResponse : bulkResponse) {
            assertThat(itemResponse.getFailureMessage(), nullValue());
            assertThat(itemResponse.status(), equalTo(RestStatus.CREATED));
            assertThat(itemResponse.getIndex(), startsWith(backingIndexPrefix));
        }
        client().admin().indices().refresh(new RefreshRequest(dataStream)).actionGet();
    }

    static void putComposableIndexTemplate(
        String name,
        @Nullable String mappings,
        List<String> patterns,
        @Nullable Settings settings,
        @Nullable Map<String, Object> metadata,
        @Nullable DataLifecycle lifecycle
    ) throws IOException {
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(name);
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
}

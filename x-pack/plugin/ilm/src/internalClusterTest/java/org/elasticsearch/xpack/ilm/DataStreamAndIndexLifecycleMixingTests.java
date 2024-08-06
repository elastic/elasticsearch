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
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction.Response.ManagedBy;
import org.elasticsearch.action.datastreams.lifecycle.ExplainDataStreamLifecycleAction;
import org.elasticsearch.action.datastreams.lifecycle.ExplainIndexDataStreamLifecycle;
import org.elasticsearch.action.datastreams.lifecycle.PutDataStreamLifecycleAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
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
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class DataStreamAndIndexLifecycleMixingTests extends ESIntegTestCase {

    private String policy;
    private String dataStreamName;
    private String indexTemplateName;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class, DataStreamsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, "1s");
        settings.put(DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getKey(), "min_docs=1,max_docs=1");
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        settings.put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s");
        settings.put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, false);
        return settings.build();
    }

    @Before
    public void refreshAbstractions() {
        policy = "policy-" + randomAlphaOfLength(5);
        dataStreamName = "datastream-" + randomAlphaOfLengthBetween(10, 15).toLowerCase(Locale.ROOT);
        indexTemplateName = "indextemplate-" + randomAlphaOfLengthBetween(10, 15).toLowerCase(Locale.ROOT);
    }

    public void testIndexTemplateSwapsILMForDataStreamLifecycle() throws Exception {
        // ILM rolls over every 2 documents
        RolloverAction rolloverIlmAction = new RolloverAction(RolloverConditions.newBuilder().addMaxIndexDocsCondition(2L).build());
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, Map.of(rolloverIlmAction.getWriteableName(), rolloverIlmAction));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, Map.of("hot", hotPhase));
        PutLifecycleRequest putLifecycleRequest = new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, lifecyclePolicy);
        assertAcked(client().execute(ILMActions.PUT, putLifecycleRequest).get());

        putComposableIndexTemplate(
            indexTemplateName,
            null,
            List.of(dataStreamName + "*"),
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).build(),
            null,
            null
        );
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 2);

        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
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
            List<String> backingIndices = getBackingIndices(dataStreamName);
            String firstGenerationIndex = backingIndices.get(0);
            String secondGenerationIndex = backingIndices.get(1);
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

        // let's update the index template to remove the ILM configuration and configured data stream lifecycle
        // note that this index template change will NOT configure a data stream lifecycle on the data stream, only for **new** data streams

        // All existing data streams will fallback to their default data stream lifecycle

        // we'll rollover the data stream by indexing 2 documents (like ILM expects) and assert that the rollover happens once so the
        // data stream has 3 backing indices, two managed by ILM and one will be managed by the data stream lifecycle
        DataStreamLifecycle customLifecycle = customEnabledLifecycle();
        putComposableIndexTemplate(indexTemplateName, null, List.of(dataStreamName + "*"), Settings.EMPTY, null, customLifecycle);

        indexDocs(dataStreamName, 2);

        // data stream was rolled over and has 3 indices, two managed by ILM and the write index will be unmanaged
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(3));

        });

        assertBusy(() -> {
            List<String> backingIndices = getBackingIndices(dataStreamName);
            String firstGenerationIndex = backingIndices.get(0);
            String secondGenerationIndex = backingIndices.get(1);
            String writeIndex = backingIndices.get(2);
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(
                firstGenerationIndex,
                secondGenerationIndex,
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

            // the write index is not managed by ILM
            IndexLifecycleExplainResponse thirdGenerationExplain = explainResponse.getIndexResponses().get(writeIndex);
            assertThat(thirdGenerationExplain.managedByILM(), is(false));

            // the write index is not managed by DSL either
            ExplainDataStreamLifecycleAction.Response dataStreamLifecycleExplainResponse = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                new ExplainDataStreamLifecycleAction.Request(TEST_REQUEST_TIMEOUT, new String[] { writeIndex })
            ).actionGet();
            assertThat(dataStreamLifecycleExplainResponse.getIndices().size(), is(1));
            ExplainIndexDataStreamLifecycle writeIndexDataStreamLifecycleExplain = dataStreamLifecycleExplainResponse.getIndices().get(0);
            assertThat(writeIndexDataStreamLifecycleExplain.isManagedByLifecycle(), is(false));
        });

        // let's make the write index of the data stream managed by DSL
        client().execute(
            PutDataStreamLifecycleAction.INSTANCE,
            new PutDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName },
                new DataStreamLifecycle()
            )
        ).actionGet();

        // at this point we should be able to rollover the data stream by indexing only one document (as the data stream lifecycle is
        // configured to)
        indexDocs(dataStreamName, 1);

        // data stream was rolled over and has 4 indices, 2 managed by ILM, and the latest 2 generations managed by the data stream
        // lifecycle
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(4));

        });

        // let's migrate this data stream to use the custom data stream lifecycle
        client().execute(
            PutDataStreamLifecycleAction.INSTANCE,
            new PutDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName },
                customLifecycle.getDataStreamRetention()
            )
        ).actionGet();

        assertBusy(() -> {
            List<String> backingIndices = getBackingIndices(dataStreamName);
            String firstGenerationIndex = backingIndices.get(0);
            String secondGenerationIndex = backingIndices.get(1);
            String thirdGenerationIndex = backingIndices.get(2);
            String writeIndex = backingIndices.get(3);
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

            IndexLifecycleExplainResponse writeIndexExplain = explainResponse.getIndexResponses().get(writeIndex);
            assertThat(writeIndexExplain.managedByILM(), is(false));

            // the third generation and the write index are managed based on the custom lifecycle
            ExplainDataStreamLifecycleAction.Response dataStreamLifecycleExplainResponse = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                new ExplainDataStreamLifecycleAction.Request(TEST_REQUEST_TIMEOUT, new String[] { thirdGenerationIndex, writeIndex })
            ).actionGet();
            assertThat(dataStreamLifecycleExplainResponse.getIndices().size(), is(2));
            for (ExplainIndexDataStreamLifecycle index : dataStreamLifecycleExplainResponse.getIndices()) {
                assertThat(index.isManagedByLifecycle(), is(true));
                assertThat(index.getLifecycle(), equalTo(customLifecycle));
            }
        });
    }

    public void testUpdateIndexTemplateFromILMtoBothILMAndDataStreamLifecycle() throws Exception {
        // ILM rolls over every 2 documents
        RolloverAction rolloverIlmAction = new RolloverAction(RolloverConditions.newBuilder().addMaxIndexDocsCondition(2L).build());
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, Map.of(rolloverIlmAction.getWriteableName(), rolloverIlmAction));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, Map.of("hot", hotPhase));
        PutLifecycleRequest putLifecycleRequest = new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, lifecyclePolicy);
        assertAcked(client().execute(ILMActions.PUT, putLifecycleRequest).get());

        putComposableIndexTemplate(
            indexTemplateName,
            null,
            List.of(dataStreamName + "*"),
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).build(),
            null,
            null
        );
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 2);
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
            List<Index> backingIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
            assertThat(backingIndices.size(), equalTo(2));
        });
        // data stream was rolled over and has 2 indices managed by ILM
        assertBusy(() -> {
            List<String> backingIndices = getBackingIndices(dataStreamName);
            String firstGenerationIndex = backingIndices.get(0);
            String secondGenerationIndex = backingIndices.get(1);
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

        // let's update the index template to add the data stream lifecycle configuration next to the ILM configuration
        // note that this index template change will NOT configure the data stream lifecycle on the data stream, only for **new** data
        // streams

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
            new DataStreamLifecycle()
        );

        indexDocs(dataStreamName, 2);

        // data stream was rolled over and has 3 indices, ALL managed by ILM
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(3));

        });
        assertBusy(() -> {
            List<String> backingIndices = getBackingIndices(dataStreamName);
            String firstGenerationIndex = backingIndices.get(0);
            String secondGenerationIndex = backingIndices.get(1);
            String thirdGenerationIndex = backingIndices.get(2);
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
        // let's migrate this data stream to use the data stream lifecycle starting with the next generation
        client().execute(
            PutDataStreamLifecycleAction.INSTANCE,
            new PutDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName },
                TimeValue.timeValueDays(90)
            )
        );

        putComposableIndexTemplate(
            indexTemplateName,
            null,
            List.of(dataStreamName + "*"),
            Settings.builder().put(IndexSettings.PREFER_ILM, false).put(LifecycleSettings.LIFECYCLE_NAME, policy).build(),
            null,
            new DataStreamLifecycle()
        );

        // note that all indices now are still managed by ILM, so we index 2 documents. the new write index will be managed by the data
        // stream lifecycle
        indexDocs(dataStreamName, 2);

        // data stream was rolled over and has 4 indices, 3 managed by ILM, and the write index managed by the data stream lifecycle
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(4));

        });

        assertBusy(() -> {
            List<String> backingIndices = getBackingIndices(dataStreamName);
            String firstGenerationIndex = backingIndices.get(0);
            String secondGenerationIndex = backingIndices.get(1);
            String thirdGenerationIndex = backingIndices.get(2);
            String writeIndex = backingIndices.get(3);
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

            // the write index is managed by the data stream lifecycle
            ExplainDataStreamLifecycleAction.Response dataStreamLifecycleExplainResponse = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                new ExplainDataStreamLifecycleAction.Request(TEST_REQUEST_TIMEOUT, new String[] { writeIndex })
            ).actionGet();
            assertThat(dataStreamLifecycleExplainResponse.getIndices().size(), is(1));
            ExplainIndexDataStreamLifecycle dataStreamLifecycleExplain = dataStreamLifecycleExplainResponse.getIndices().get(0);
            assertThat(dataStreamLifecycleExplain.isManagedByLifecycle(), is(true));
            assertThat(dataStreamLifecycleExplain.getIndex(), is(writeIndex));
        });
    }

    public void testUpdateIndexTemplateToDataStreamLifecyclePreference() throws Exception {
        // ILM rolls over every 2 documents
        RolloverAction rolloverIlmAction = new RolloverAction(RolloverConditions.newBuilder().addMaxIndexDocsCondition(2L).build());
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, Map.of(rolloverIlmAction.getWriteableName(), rolloverIlmAction));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, Map.of("hot", hotPhase));
        PutLifecycleRequest putLifecycleRequest = new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, lifecyclePolicy);
        assertAcked(client().execute(ILMActions.PUT, putLifecycleRequest).get());

        putComposableIndexTemplate(
            indexTemplateName,
            null,
            List.of(dataStreamName + "*"),
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).build(),
            null,
            null
        );
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 2);

        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStreamName));
            List<Index> backingIndices = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices();
            assertThat(backingIndices.size(), equalTo(2));
        });

        // data stream was rolled over and has 2 indices managed by ILM
        assertBusy(() -> {
            List<String> backingIndices = getBackingIndices(dataStreamName);
            String firstGenerationIndex = backingIndices.get(0);
            String secondGenerationIndex = backingIndices.get(1);
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

        // let's update the index template to configure the management preference to be data stream lifecycle using the prefer_ilm setting
        // note that this index template change will NOT affect existing indices but only the new ones after a rollover.

        // we'll rollover the data stream by indexing 2 documents (like ILM expects) and assert that the rollover happens once so the
        // data stream has 3 backing indices, 2 managed by ILM and 1 by the default data stream lifecycle
        DataStreamLifecycle customLifecycle = customEnabledLifecycle();
        putComposableIndexTemplate(
            indexTemplateName,
            null,
            List.of(dataStreamName + "*"),
            Settings.builder().put(IndexSettings.PREFER_ILM, false).put(LifecycleSettings.LIFECYCLE_NAME, policy).build(),
            null,
            customLifecycle
        );

        indexDocs(dataStreamName, 2);

        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(3));

        });

        assertBusy(() -> {
            List<String> backingIndices = getBackingIndices(dataStreamName);
            String firstGenerationIndex = backingIndices.get(0);
            String secondGenerationIndex = backingIndices.get(1);
            String writeIndex = backingIndices.get(2);
            ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest().indices(
                firstGenerationIndex,
                secondGenerationIndex,
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

            // the write index is unmanaged
            ExplainDataStreamLifecycleAction.Response dataStreamLifecycleExplainResponse = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                new ExplainDataStreamLifecycleAction.Request(TEST_REQUEST_TIMEOUT, new String[] { writeIndex })
            ).actionGet();
            assertThat(dataStreamLifecycleExplainResponse.getIndices().size(), is(1));
            ExplainIndexDataStreamLifecycle dataStreamLifecycleExplain = dataStreamLifecycleExplainResponse.getIndices().get(0);
            assertThat(dataStreamLifecycleExplain.isManagedByLifecycle(), is(false));
        });

        // let's make the write index of the data stream managed by DSL
        client().execute(
            PutDataStreamLifecycleAction.INSTANCE,
            new PutDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName },
                new DataStreamLifecycle()
            )
        ).actionGet();

        // at this point, the write index of the data stream is managed by data stream lifecycle and not by ILM anymore so we can just index
        // one document to trigger the rollover
        indexDocs(dataStreamName, 1);

        // let's migrate this data stream to use the custom data stream lifecycle
        client().execute(
            PutDataStreamLifecycleAction.INSTANCE,
            new PutDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName },
                customLifecycle.getDataStreamRetention()
            )
        ).actionGet();

        // data stream was rolled over and has 4 indices, 2 managed by ILM, and 2 managed by the custom data stream lifecycle
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(4));

        });

        assertBusy(() -> {
            List<String> backingIndices = getBackingIndices(dataStreamName);
            String firstGenerationIndex = backingIndices.get(0);
            String secondGenerationIndex = backingIndices.get(1);
            String thirdGenerationIndex = backingIndices.get(2);
            String writeIndex = backingIndices.get(3);
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

            // ILM is not managing these indices anymore
            IndexLifecycleExplainResponse thirdGenerationExplain = explainResponse.getIndexResponses().get(thirdGenerationIndex);
            assertThat(thirdGenerationExplain.managedByILM(), is(false));

            IndexLifecycleExplainResponse writeIndexExplain = explainResponse.getIndexResponses().get(writeIndex);
            assertThat(writeIndexExplain.managedByILM(), is(false));

            // the third generation and the write index are managed by the data stream lifecycle
            ExplainDataStreamLifecycleAction.Response dataStreamLifecycleExplainResponse = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                new ExplainDataStreamLifecycleAction.Request(TEST_REQUEST_TIMEOUT, new String[] { thirdGenerationIndex, writeIndex })
            ).actionGet();
            assertThat(dataStreamLifecycleExplainResponse.getIndices().size(), is(2));
            for (ExplainIndexDataStreamLifecycle index : dataStreamLifecycleExplainResponse.getIndices()) {
                assertThat(index.isManagedByLifecycle(), is(true));
                assertThat(index.getLifecycle(), equalTo(customLifecycle));
            }
        });

        // Then we opt out the data stream lifecycle
        // let's migrate this data stream to disable using the data stream lifecycle
        client().execute(
            PutDataStreamLifecycleAction.INSTANCE,
            new PutDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName },
                TimeValue.timeValueDays(90),
                false
            )
        ).actionGet();

        // At this point the data stream is not managed by the data stream lifecycle and all the indices fallback to ILM.
        assertBusy(() -> {
            List<String> backingIndices = getBackingIndices(dataStreamName);
            String firstGenerationIndex = backingIndices.get(0);
            String secondGenerationIndex = backingIndices.get(1);
            String thirdGenerationIndex = backingIndices.get(2);
            String writeIndex = backingIndices.get(3);
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

            // ILM is not managing this index again
            IndexLifecycleExplainResponse thirdGenerationExplain = explainResponse.getIndexResponses().get(thirdGenerationIndex);
            assertThat(thirdGenerationExplain.managedByILM(), is(true));
            assertThat(thirdGenerationExplain.getPhase(), is("hot"));
            assertThat(thirdGenerationExplain.getStep(), is(PhaseCompleteStep.NAME));

            // the write index is managed by the ILM as well
            IndexLifecycleExplainResponse writeIndexExplain = explainResponse.getIndexResponses().get(writeIndex);
            assertThat(writeIndexExplain.managedByILM(), is(true));
            assertThat(writeIndexExplain.getPhase(), is("hot"));
            assertThat(writeIndexExplain.getStep(), is(WaitForRolloverReadyStep.NAME));
        });
    }

    public void testUpdateIndexTemplateToMigrateFromDataStreamLifecycleToIlm() throws Exception {
        // starting with a data stream managed by the data stream lifecycle (rolling over every 1 doc)
        putComposableIndexTemplate(indexTemplateName, null, List.of(dataStreamName + "*"), null, null, new DataStreamLifecycle());

        // this will create the data stream and trigger a rollover so we will end up with a data stream with 2 backing indices
        indexDocs(dataStreamName, 1);

        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(2));

        });

        assertBusy(() -> {
            List<String> backingIndices = getBackingIndices(dataStreamName);
            String firstGenerationIndex = backingIndices.get(0);
            String writeIndex = backingIndices.get(1);

            // let's check the indices are managed by the data stream lifecycle
            ExplainDataStreamLifecycleAction.Response dataStreamLifecycleExplainResponse = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                new ExplainDataStreamLifecycleAction.Request(TEST_REQUEST_TIMEOUT, new String[] { firstGenerationIndex, writeIndex })
            ).actionGet();
            assertThat(dataStreamLifecycleExplainResponse.getIndices().size(), is(2));
            for (ExplainIndexDataStreamLifecycle index : dataStreamLifecycleExplainResponse.getIndices()) {
                assertThat(index.isManagedByLifecycle(), is(true));
            }
        });

        // ILM rolls over every 2 documents - create the policy
        RolloverAction rolloverIlmAction = new RolloverAction(RolloverConditions.newBuilder().addMaxIndexDocsCondition(2L).build());
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, Map.of(rolloverIlmAction.getWriteableName(), rolloverIlmAction));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, Map.of("hot", hotPhase));
        PutLifecycleRequest putLifecycleRequest = new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, lifecyclePolicy);
        assertAcked(client().execute(ILMActions.PUT, putLifecycleRequest).get());

        // let's update the index template to remove the data stream lifecycle configuration and replace it with an ILM configuration
        // note that this change will apply to new backing indices only. The write index will continue to be managed by the data stream
        // lifecycle so we'll trigger a rollover by indexing one document (the next write index, and all subsequent new generations will
        // start being managed by ILM)

        // note that simply removing the data stream lifecycle configuration from the index template does NOT remove it from the data
        // stream, however the default value for the prefer_ilm setting is `true` so even though the new indices of the data stream will
        // have both the data stream lifecycle configuration (by virtue of being part of a data stream that has `lifecycle` configured) and
        // the ILM lifecycle configuration provided by the index template, ILM will take priority

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
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(3));

        });

        assertBusy(() -> {
            List<String> backingIndices = getBackingIndices(dataStreamName);
            String firstGenerationIndex = backingIndices.get(0);
            String secondGenerationIndex = backingIndices.get(1);
            String writeIndex = backingIndices.get(2);

            // let's check the previous indices are managed by the data stream lifecycle
            ExplainDataStreamLifecycleAction.Response dataStreamLifecycleExplainResponse = client().execute(
                ExplainDataStreamLifecycleAction.INSTANCE,
                new ExplainDataStreamLifecycleAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    new String[] { firstGenerationIndex, secondGenerationIndex }
                )
            ).actionGet();
            assertThat(dataStreamLifecycleExplainResponse.getIndices().size(), is(2));
            for (ExplainIndexDataStreamLifecycle index : dataStreamLifecycleExplainResponse.getIndices()) {
                assertThat(index.isManagedByLifecycle(), is(true));
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
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(4));

        });

        // the new write index is also managed by ILM
        assertBusy(() -> {
            List<String> backingIndices = getBackingIndices(dataStreamName);
            String thirdGenerationIndex = backingIndices.get(2);
            String writeIndex = backingIndices.get(3);

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

    public void testGetDataStreamResponse() throws Exception {
        // ILM rolls over every 2 documents
        RolloverAction rolloverIlmAction = new RolloverAction(RolloverConditions.newBuilder().addMaxIndexDocsCondition(2L).build());
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, Map.of(rolloverIlmAction.getWriteableName(), rolloverIlmAction));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, Map.of("hot", hotPhase));
        PutLifecycleRequest putLifecycleRequest = new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, lifecyclePolicy);
        assertAcked(client().execute(ILMActions.PUT, putLifecycleRequest).get());

        putComposableIndexTemplate(
            indexTemplateName,
            null,
            List.of(dataStreamName + "*"),
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).build(),
            null,
            null
        );
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        indexDocs(dataStreamName, 2);

        // wait to rollover
        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), is(2));
        });

        // prefer_ilm false in the index template
        putComposableIndexTemplate(
            indexTemplateName,
            null,
            List.of(dataStreamName + "*"),
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).put(IndexSettings.PREFER_ILM, false).build(),
            null,
            null
        );

        client().execute(
            PutDataStreamLifecycleAction.INSTANCE,
            new PutDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName },
                TimeValue.timeValueDays(90)
            )
        ).actionGet();

        // rollover again - at this point this data stream should have 2 backing indices managed by ILM and the write index managed by
        // data stream lifecycle
        indexDocs(dataStreamName, 2);

        assertBusy(() -> {
            GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName }
            );
            GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
                .actionGet();
            assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
            GetDataStreamAction.Response.DataStreamInfo dataStreamInfo = getDataStreamResponse.getDataStreams().get(0);
            List<Index> indices = dataStreamInfo.getDataStream().getIndices();
            assertThat(indices.size(), is(3));

            // the prefer_ilm value from the template should be reflected in the response at the top level
            assertThat(dataStreamInfo.templatePreferIlmValue(), is(false));
            // the template ILM policy should still be reflected at the top level
            assertThat(dataStreamInfo.getIlmPolicy(), is(policy));

            List<String> backingIndices = getBackingIndices(dataStreamName);
            String firstGenerationIndex = backingIndices.get(0);
            String secondGenerationIndex = backingIndices.get(1);
            String writeIndex = backingIndices.get(2);
            assertThat(
                indices.stream().map(i -> i.getName()).toList(),
                containsInAnyOrder(firstGenerationIndex, secondGenerationIndex, writeIndex)
            );

            Function<String, Optional<Index>> backingIndexSupplier = indexName -> indices.stream()
                .filter(index -> index.getName().equals(indexName))
                .findFirst();

            // let's assert the policy is reported for all indices (as it's present in the index template) and the value of the
            // prefer_ilm setting remains true for the first 2 generations and is false for the write index (the generation after rollover)
            Optional<Index> firstGenSettings = backingIndexSupplier.apply(firstGenerationIndex);
            assertThat(firstGenSettings.isPresent(), is(true));
            assertThat(dataStreamInfo.getIndexSettingsValues().get(firstGenSettings.get()).preferIlm(), is(true));
            assertThat(dataStreamInfo.getIndexSettingsValues().get(firstGenSettings.get()).ilmPolicyName(), is(policy));
            assertThat(dataStreamInfo.getIndexSettingsValues().get(firstGenSettings.get()).managedBy(), is(ManagedBy.ILM));
            Optional<Index> secondGenSettings = backingIndexSupplier.apply(secondGenerationIndex);
            assertThat(secondGenSettings.isPresent(), is(true));
            assertThat(dataStreamInfo.getIndexSettingsValues().get(secondGenSettings.get()).preferIlm(), is(true));
            assertThat(dataStreamInfo.getIndexSettingsValues().get(secondGenSettings.get()).ilmPolicyName(), is(policy));
            assertThat(dataStreamInfo.getIndexSettingsValues().get(secondGenSettings.get()).managedBy(), is(ManagedBy.ILM));
            Optional<Index> writeIndexSettings = backingIndexSupplier.apply(writeIndex);
            assertThat(writeIndexSettings.isPresent(), is(true));
            assertThat(dataStreamInfo.getIndexSettingsValues().get(writeIndexSettings.get()).preferIlm(), is(false));
            assertThat(dataStreamInfo.getIndexSettingsValues().get(writeIndexSettings.get()).ilmPolicyName(), is(policy));
            assertThat(dataStreamInfo.getIndexSettingsValues().get(writeIndexSettings.get()).managedBy(), is(ManagedBy.LIFECYCLE));

            // with the current configuratino, the next generation index will be managed by DSL
            assertThat(dataStreamInfo.getNextGenerationManagedBy(), is(ManagedBy.LIFECYCLE));
        });

        // remove ILM policy and prefer_ilm from template
        putComposableIndexTemplate(indexTemplateName, null, List.of(dataStreamName + "*"), Settings.builder().build(), null, null);
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            new String[] { dataStreamName }
        );
        GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
        GetDataStreamAction.Response.DataStreamInfo dataStreamInfo = getDataStreamResponse.getDataStreams().get(0);
        // since the ILM related settings are gone from the index template, this data stream should now be managed by lifecycle
        assertThat(dataStreamInfo.getNextGenerationManagedBy(), is(ManagedBy.LIFECYCLE));

        // disable data stream lifecycle on the data stream. the future generations will be UNMANAGED
        client().execute(
            PutDataStreamLifecycleAction.INSTANCE,
            new PutDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                new String[] { dataStreamName },
                TimeValue.timeValueDays(90),
                false
            )
        ).actionGet();

        getDataStreamRequest = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { dataStreamName });
        getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest).actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
        dataStreamInfo = getDataStreamResponse.getDataStreams().get(0);
        // since the ILM related settings are gone from the index template and the lifeclcye is disabled, this data stream should now be
        // managed unmanaged
        assertThat(dataStreamInfo.getNextGenerationManagedBy(), is(ManagedBy.UNMANAGED));
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
        indicesAdmin().refresh(new RefreshRequest(dataStream)).actionGet();
    }

    static void putComposableIndexTemplate(
        String name,
        @Nullable String mappings,
        List<String> patterns,
        @Nullable Settings settings,
        @Nullable Map<String, Object> metadata,
        @Nullable DataStreamLifecycle lifecycle
    ) throws IOException {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(name);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(patterns)
                .template(new Template(settings, mappings == null ? null : CompressedXContent.fromJSON(mappings), null, lifecycle))
                .metadata(metadata)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
    }

    private static DataStreamLifecycle customEnabledLifecycle() {
        return DataStreamLifecycle.newBuilder().dataRetention(TimeValue.timeValueMillis(randomMillisUpToYear9999())).build();
    }

    private List<String> getBackingIndices(String dataStreamName) {
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            new String[] { dataStreamName }
        );
        GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        return getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().stream().map(Index::getName).toList();
    }
}

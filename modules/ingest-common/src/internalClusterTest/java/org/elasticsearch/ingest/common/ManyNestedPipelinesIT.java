/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.ingest.SimulateDocumentBaseResult;
import org.elasticsearch.action.ingest.SimulateDocumentResult;
import org.elasticsearch.action.ingest.SimulateDocumentVerboseResult;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.action.ingest.SimulateProcessorResult;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Strings;
import org.elasticsearch.ingest.GraphStructureException;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
/*
 * This test is meant to make sure that we can handle ingesting a document with a reasonably large number of nested pipeline processors.
 */
public class ManyNestedPipelinesIT extends ESIntegTestCase {
    private final int manyPipelinesCount = randomIntBetween(2, 50);
    private final int tooManyPipelinesCount = IngestDocument.MAX_PIPELINES + 1;
    private static final String MANY_PIPELINES_PREFIX = "many_";
    private static final String TOO_MANY_PIPELINES_PREFIX = "too_many_";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestCommonPlugin.class);
    }

    @Before
    public void loadManyPipelines() {
        internalCluster().ensureAtLeastNumDataNodes(1);
        internalCluster().startMasterOnlyNode();
        createManyChainedPipelines();
    }

    public void testIngestManyPipelines() {
        String index = "index";
        DocWriteResponse response = prepareIndex(index).setSource(Map.of("foo", "bar"))
            .setPipeline(MANY_PIPELINES_PREFIX + "pipeline_0")
            .get();
        assertThat(response.getResult(), equalTo(DocWriteResponse.Result.CREATED));
        GetResponse getREsponse = client().prepareGet(index, response.getId()).get();
        assertThat(getREsponse.getSource().get("foo"), equalTo("baz"));
    }

    public void testSimulateManyPipelines() throws IOException {
        List<SimulateDocumentResult> results = executeSimulateManyPipelines(false);
        assertThat(results.size(), equalTo(1));
        assertThat(results.get(0), instanceOf(SimulateDocumentBaseResult.class));
        SimulateDocumentBaseResult result = (SimulateDocumentBaseResult) results.get(0);
        assertNull(result.getFailure());
        IngestDocument resultDoc = result.getIngestDocument();
        assertThat(resultDoc.getFieldValue("foo", String.class), equalTo("baz"));
    }

    public void testSimulateVerboseManyPipelines() throws IOException {
        List<SimulateDocumentResult> results = executeSimulateManyPipelines(true);
        assertThat(results.size(), equalTo(1));
        assertThat(results.get(0), instanceOf(SimulateDocumentVerboseResult.class));
        SimulateDocumentVerboseResult result = (SimulateDocumentVerboseResult) results.get(0);
        assertThat(result.getProcessorResults().size(), equalTo(manyPipelinesCount));
        List<SimulateProcessorResult> simulateProcessorResults = result.getProcessorResults();
        SimulateProcessorResult lastResult = simulateProcessorResults.get(simulateProcessorResults.size() - 1);
        IngestDocument resultDoc = lastResult.getIngestDocument();
        assertThat(resultDoc.getFieldValue("foo", String.class), equalTo("baz"));
    }

    public void testTooManyPipelines() throws IOException {
        /*
         * Logically, this test method contains three tests (too many pipelines for ingest, simulate, and simulate verbose). But creating
         * pipelines is so slow that they are lumped into this one method.
         */
        createTooManyChainedPipelines();
        expectThrows(
            GraphStructureException.class,
            () -> prepareIndex("foo").setSource(Map.of("foo", "bar")).setPipeline(TOO_MANY_PIPELINES_PREFIX + "pipeline_0").get()
        );
        {
            List<SimulateDocumentResult> results = executeSimulateTooManyPipelines(false);
            assertThat(results.size(), equalTo(1));
            assertThat(results.get(0), instanceOf(SimulateDocumentBaseResult.class));
            SimulateDocumentBaseResult result = (SimulateDocumentBaseResult) results.get(0);
            assertNotNull(result.getFailure());
            assertNotNull(result.getFailure().getCause());
            assertThat(result.getFailure().getCause(), instanceOf(GraphStructureException.class));
        }
        {
            List<SimulateDocumentResult> results = executeSimulateTooManyPipelines(true);
            assertThat(results.size(), equalTo(1));
            assertThat(results.get(0), instanceOf(SimulateDocumentVerboseResult.class));
            SimulateDocumentVerboseResult result = (SimulateDocumentVerboseResult) results.get(0);
            assertNotNull(result);
            assertNotNull(result.getProcessorResults().get(0).getFailure());
            assertThat(result.getProcessorResults().get(0).getFailure().getCause(), instanceOf(GraphStructureException.class));
        }
    }

    private List<SimulateDocumentResult> executeSimulateManyPipelines(boolean verbose) throws IOException {
        return executeSimulatePipelines(MANY_PIPELINES_PREFIX, verbose);
    }

    private List<SimulateDocumentResult> executeSimulateTooManyPipelines(boolean verbose) throws IOException {
        return executeSimulatePipelines(TOO_MANY_PIPELINES_PREFIX, verbose);
    }

    private List<SimulateDocumentResult> executeSimulatePipelines(String prefix, boolean verbose) throws IOException {
        BytesReference simulateRequestBytes = BytesReference.bytes(
            jsonBuilder().startObject()
                .startArray("docs")
                .startObject()
                .field("_index", "foo")
                .field("_id", "id")
                .startObject("_source")
                .field("foo", "bar")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );
        SimulatePipelineResponse simulatePipelineResponse = clusterAdmin().prepareSimulatePipeline(simulateRequestBytes, XContentType.JSON)
            .setId(prefix + "pipeline_0")
            .setVerbose(verbose)
            .get();
        return simulatePipelineResponse.getResults();
    }

    private void createManyChainedPipelines() {
        createChainedPipelines(MANY_PIPELINES_PREFIX, manyPipelinesCount);
    }

    private void createTooManyChainedPipelines() {
        createChainedPipelines(TOO_MANY_PIPELINES_PREFIX, tooManyPipelinesCount);
    }

    private void createChainedPipelines(String prefix, int count) {
        for (int i = 0; i < count - 1; i++) {
            createChainedPipeline(prefix, i);
        }
        createLastPipeline(prefix, count - 1);
    }

    private void createChainedPipeline(String prefix, int number) {
        String pipelineId = prefix + "pipeline_" + number;
        String nextPipelineId = prefix + "pipeline_" + (number + 1);
        String pipelineTemplate = """
            {
                "processors": [
                    {
                        "pipeline": {
                            "name": "%s"
                        }
                    }
                ]
            }
            """;
        String pipeline = Strings.format(pipelineTemplate, nextPipelineId);
        clusterAdmin().preparePutPipeline(pipelineId, new BytesArray(pipeline), XContentType.JSON).get();
    }

    private void createLastPipeline(String prefix, int number) {
        String pipelineId = prefix + "pipeline_" + number;
        String pipeline = """
            {
                "processors": [
                    {
                       "set": {
                          "field": "foo",
                          "value": "baz"
                       }
                    }
                ]
            }
            """;
        clusterAdmin().preparePutPipeline(pipelineId, new BytesArray(pipeline), XContentType.JSON).get();
    }
}

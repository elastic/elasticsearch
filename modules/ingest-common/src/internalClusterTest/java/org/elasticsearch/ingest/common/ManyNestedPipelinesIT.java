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
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
/*
 * This test is meant to make sure that we can handle ingesting a document with a reasonably large number of nested pipeline processors.
 */
public class ManyNestedPipelinesIT extends ESIntegTestCase {
    private final int manyPipelinesCount = randomIntBetween(2, 50);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IngestCommonPlugin.class);
    }

    @Before
    public void loadManyPipelines() {
        internalCluster().ensureAtLeastNumDataNodes(1);
        internalCluster().startMasterOnlyNode();
        createChainedPipelines(manyPipelinesCount);
    }

    public void testIngestManyPipelines() {
        String index = "index";
        DocWriteResponse response = client().prepareIndex(index, "_doc")
            .setSource(Collections.singletonMap("foo", "bar"))
            .setPipeline("pipeline_0")
            .get();
        assertThat(response.getResult(), equalTo(DocWriteResponse.Result.CREATED));
        GetResponse getREsponse = client().prepareGet(index, "_doc", response.getId()).get();
        assertThat(getREsponse.getSource().get("foo"), equalTo("baz"));
    }

    public void testSimulateManyPipelines() throws IOException {
        List<SimulateDocumentResult> results = executeSimulate(false);
        assertThat(results.size(), equalTo(1));
        assertThat(results.get(0), instanceOf(SimulateDocumentBaseResult.class));
        SimulateDocumentBaseResult result = (SimulateDocumentBaseResult) results.get(0);
        assertNull(result.getFailure());
        IngestDocument resultDoc = result.getIngestDocument();
        assertThat(resultDoc.getFieldValue("foo", String.class), equalTo("baz"));
    }

    public void testSimulateVerboseManyPipelines() throws IOException {
        List<SimulateDocumentResult> results = executeSimulate(true);
        assertThat(results.size(), equalTo(1));
        assertThat(results.get(0), instanceOf(SimulateDocumentVerboseResult.class));
        SimulateDocumentVerboseResult result = (SimulateDocumentVerboseResult) results.get(0);
        assertThat(result.getProcessorResults().size(), equalTo(manyPipelinesCount));
        List<SimulateProcessorResult> simulateProcessorResults = result.getProcessorResults();
        SimulateProcessorResult lastResult = simulateProcessorResults.get(simulateProcessorResults.size() - 1);
        IngestDocument resultDoc = lastResult.getIngestDocument();
        assertThat(resultDoc.getFieldValue("foo", String.class), equalTo("baz"));
    }

    private List<SimulateDocumentResult> executeSimulate(boolean verbose) throws IOException {
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
            .setId("pipeline_0")
            .setVerbose(verbose)
            .get();
        return simulatePipelineResponse.getResults();
    }

    private void createChainedPipelines(int count) {
        for (int i = 0; i < count - 1; i++) {
            createChainedPipeline(i);
        }
        createLastPipeline(count - 1);
    }

    private void createChainedPipeline(int number) {
        String pipelineId = "pipeline_" + number;
        String nextPipelineId = "pipeline_" + (number + 1);
        String pipelineTemplate = "{\n"
            + "                \"processors\": [\n"
            + "                    {\n"
            + "                        \"pipeline\": {\n"
            + "                            \"name\": \"%s\"\n"
            + "                        }\n"
            + "                    }\n"
            + "                ]\n"
            + "            }";
        String pipeline = String.format(Locale.ROOT, pipelineTemplate, nextPipelineId);
        clusterAdmin().preparePutPipeline(pipelineId, new BytesArray(pipeline), XContentType.JSON).get();
    }

    private void createLastPipeline(int number) {
        String pipelineId = "pipeline_" + number;
        String pipeline = "            {\n"
            + "                \"processors\": [\n"
            + "                    {\n"
            + "                       \"set\": {\n"
            + "                          \"field\": \"foo\",\n"
            + "                          \"value\": \"baz\"\n"
            + "                       }\n"
            + "                    }\n"
            + "                ]\n"
            + "            }";
        clusterAdmin().preparePutPipeline(pipelineId, new BytesArray(pipeline), XContentType.JSON).get();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.ingest;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class IngestStatsNamesAndTypesIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(CustomIngestTestPlugin.class, CustomScriptPlugin.class);
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @SuppressWarnings("unchecked")
    public void testIngestStatsNamesAndTypes() throws IOException {
        String pipeline1 = org.elasticsearch.core.Strings.format("""
            {
              "processors": [
                {
                  "set": {
                    "tag": "set-a",
                    "field": "a",
                    "value": "1"
                  }
                },
                {
                  "set": {
                    "tag": "set-b",
                    "field": "b",
                    "value": "2",
                    "if": {
                      "lang": "%s",
                      "source": "false_script"
                    }
                  }
                },
                {
                  "set": {
                    "tag": "set-c",
                    "field": "c",
                    "value": "3",
                    "ignore_failure": true
                  }
                },
                {
                  "set": {
                    "tag": "set-d",
                    "field": "d",
                    "value": "4",
                    "if": {
                      "lang": "%s",
                      "source": "true_script"
                    },
                    "ignore_failure": true
                  }
                }
              ]
            }
            """, MockScriptEngine.NAME, MockScriptEngine.NAME);
        BytesReference pipeline1Reference = new BytesArray(pipeline1);
        client().admin().cluster().putPipeline(new PutPipelineRequest("pipeline1", pipeline1Reference, XContentType.JSON)).actionGet();

        // index a single document through the pipeline
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("index1").id("1").source("{}", XContentType.JSON).setPipeline("pipeline1"));
        client().bulk(bulkRequest).actionGet();

        {
            NodesStatsResponse nodesStatsResponse = clusterAdmin().nodesStats(new NodesStatsRequest().addMetric("ingest")).actionGet();
            assertThat(nodesStatsResponse.getNodes().size(), equalTo(1));

            NodeStats stats = nodesStatsResponse.getNodes().get(0);
            assertThat(stats.getIngestStats().totalStats().ingestCount(), equalTo(1L));
            assertThat(stats.getIngestStats().pipelineStats().size(), equalTo(1));

            IngestStats.PipelineStat pipelineStat = stats.getIngestStats().pipelineStats().get(0);
            assertThat(pipelineStat.pipelineId(), equalTo("pipeline1"));
            assertThat(pipelineStat.stats().ingestCount(), equalTo(1L));

            List<IngestStats.ProcessorStat> processorStats = stats.getIngestStats().processorStats().get("pipeline1");
            assertThat(processorStats.size(), equalTo(4));

            IngestStats.ProcessorStat setA = processorStats.get(0);
            assertThat(setA.name(), equalTo("set:set-a"));
            assertThat(setA.type(), equalTo("set"));
            assertThat(setA.stats().ingestCount(), equalTo(1L));

            IngestStats.ProcessorStat setB = processorStats.get(1);
            assertThat(setB.name(), equalTo("set:set-b"));
            assertThat(setB.type(), equalTo("set"));
            assertThat(setB.stats().ingestCount(), equalTo(0L)); // see false_script above

            IngestStats.ProcessorStat setC = processorStats.get(2);
            assertThat(setC.name(), equalTo("set:set-c"));
            assertThat(setC.type(), equalTo("set"));
            assertThat(setC.stats().ingestCount(), equalTo(1L));

            IngestStats.ProcessorStat setD = processorStats.get(3);
            assertThat(setD.name(), equalTo("set:set-d"));
            assertThat(setD.type(), equalTo("set"));
            assertThat(setD.stats().ingestCount(), equalTo(1L));
        }

        {
            // the bits that we want to read from the cluster stats response aren't visible in java code (no getters,
            // non-public classes and methods), roundtrip through json so that we can read what we want
            ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            response.toXContent(builder, new ToXContent.MapParams(Map.of()));
            builder.endObject();
            Map<String, Object> stats = createParser(JsonXContent.jsonXContent, Strings.toString(builder)).map();

            int setProcessorCount = path(stats, "nodes.ingest.processor_stats.set.count");
            assertThat(setProcessorCount, equalTo(3));
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T path(Map<String, Object> map, String path) {
        String[] paths = path.split("\\.");
        String[] leading = Arrays.copyOfRange(paths, 0, paths.length - 1);
        String trailing = paths[paths.length - 1];
        for (String key : leading) {
            map = (Map<String, Object>) map.get(key);
        }
        return (T) map.get(trailing);
    }

    public static class CustomIngestTestPlugin extends IngestTestPlugin {
        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            Map<String, Processor.Factory> processors = new HashMap<>();
            processors.put("set", (factories, tag, description, config) -> {
                String field = (String) config.remove("field");
                String value = (String) config.remove("value");
                return new FakeProcessor("set", tag, description, (ingestDocument) -> ingestDocument.setFieldValue(field, value));
            });

            return processors;
        }
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Map.of("true_script", ctx -> true, "false_script", ctx -> false);
        }
    }
}

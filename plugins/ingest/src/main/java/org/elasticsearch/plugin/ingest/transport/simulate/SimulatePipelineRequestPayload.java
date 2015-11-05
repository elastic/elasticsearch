/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.plugin.ingest.transport.simulate;

import org.elasticsearch.ingest.Data;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.processor.ConfigurationUtils;
import org.elasticsearch.plugin.ingest.PipelineExecutionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SimulatePipelineRequestPayload {

    private final List<Data> documents;
    private final Pipeline pipeline;

    public SimulatePipelineRequestPayload(Pipeline pipeline, List<Data> documents) {
        this.pipeline = pipeline;
        this.documents = Collections.unmodifiableList(documents);
    }

    public String pipelineId() {
        return pipeline.getId();
    }

    public Pipeline pipeline() {
        return pipeline;
    }


    public Data getDocument(int i) {
        return documents.get(i);
    }

    public int size() {
        return documents.size();
    }

    public static class Factory {

        public SimulatePipelineRequestPayload create(String pipelineId, Map<String, Object> config, PipelineExecutionService executionService) throws IOException {
            Pipeline pipeline;
            // if pipeline `id` passed to request, fetch pipeline from store.
            if (pipelineId != null) {
                pipeline = executionService.getPipeline(pipelineId);
            } else {
                Map<String, Object> pipelineConfig = (Map<String, Object>) config.get("pipeline");
                pipeline = (new Pipeline.Factory()).create("_pipeline_id", pipelineConfig, executionService.getProcessorFactoryRegistry());
            }

            // distribute docs by shard key to SimulateShardPipelineResponse
            List<Map<String, Object>> docs = (List<Map<String, Object>>) config.get("docs");

            List<Data> dataList = new ArrayList<>();

            for (int i = 0; i < docs.size(); i++) {
                Map<String, Object> dataMap = docs.get(i);
                Map<String, Object> document = (Map<String, Object>) dataMap.get("_source");
                Data data = new Data(ConfigurationUtils.readStringProperty(dataMap, "_index", null),
                        ConfigurationUtils.readStringProperty(dataMap, "_type", null),
                        ConfigurationUtils.readStringProperty(dataMap, "_id", null),
                        document);
                dataList.add(data);
            }

            return new SimulatePipelineRequestPayload(pipeline, dataList);
        }
    }
}

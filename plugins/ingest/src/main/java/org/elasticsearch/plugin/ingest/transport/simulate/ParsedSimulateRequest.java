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
import org.elasticsearch.plugin.ingest.PipelineStore;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.plugin.ingest.transport.simulate.SimulatePipelineRequest.Fields;

public class ParsedSimulateRequest {
    private final List<Data> documents;
    private final Pipeline pipeline;
    private final boolean verbose;

    ParsedSimulateRequest(Pipeline pipeline, List<Data> documents, boolean verbose) {
        this.pipeline = pipeline;
        this.documents = Collections.unmodifiableList(documents);
        this.verbose = verbose;
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public List<Data> getDocuments() {
        return documents;
    }

    public boolean isVerbose() {
        return verbose;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ParsedSimulateRequest that = (ParsedSimulateRequest) o;
        return Objects.equals(verbose, that.verbose) &&
                Objects.equals(documents, that.documents) &&
                Objects.equals(pipeline, that.pipeline);
    }

    @Override
    public int hashCode() {
        return Objects.hash(documents, pipeline, verbose);
    }

    public static class Parser {
        private static final Pipeline.Factory PIPELINE_FACTORY = new Pipeline.Factory();
        public static final String SIMULATED_PIPELINE_ID = "_simulate_pipeline";

        private List<Data> parseDocs(Map<String, Object> config) {
            List<Map<String, Object>> docs = ConfigurationUtils.readList(config, Fields.DOCS);
            List<Data> dataList = new ArrayList<>();
            for (Map<String, Object> dataMap : docs) {
                Map<String, Object> document = ConfigurationUtils.readMap(dataMap, Fields.SOURCE);
                Data data = new Data(ConfigurationUtils.readStringProperty(dataMap, Fields.INDEX),
                        ConfigurationUtils.readStringProperty(dataMap, Fields.TYPE),
                        ConfigurationUtils.readStringProperty(dataMap, Fields.ID),
                        document);
                dataList.add(data);
            }
            return dataList;
        }

        public ParsedSimulateRequest parseWithPipelineId(String pipelineId, Map<String, Object> config, boolean verbose, PipelineStore pipelineStore) {
            if (pipelineId == null) {
                throw new IllegalArgumentException("param [pipeline] is null");
            }
            Pipeline pipeline = pipelineStore.get(pipelineId);
            List<Data> dataList = parseDocs(config);
            return new ParsedSimulateRequest(pipeline, dataList, verbose);

        }

        public ParsedSimulateRequest parse(Map<String, Object> config, boolean verbose, PipelineStore pipelineStore) throws IOException {
            Map<String, Object> pipelineConfig = ConfigurationUtils.readMap(config, Fields.PIPELINE);
            Pipeline pipeline = PIPELINE_FACTORY.create(SIMULATED_PIPELINE_ID, pipelineConfig, pipelineStore.getProcessorFactoryRegistry());
            List<Data> dataList = parseDocs(config);
            return new ParsedSimulateRequest(pipeline, dataList, verbose);
        }
    }
}

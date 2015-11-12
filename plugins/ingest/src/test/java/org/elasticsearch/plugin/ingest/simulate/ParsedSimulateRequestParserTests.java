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

package org.elasticsearch.plugin.ingest.simulate;

import org.elasticsearch.ingest.Data;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.ingest.processor.mutate.MutateProcessor;
import org.elasticsearch.plugin.ingest.PipelineStore;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ParsedSimulateRequestParserTests extends ESTestCase {
    private static final ParsedSimulateRequest.Parser PARSER = new ParsedSimulateRequest.Parser();

    private Map<String, Processor.Factory> processorRegistry;
    private PipelineStore store;
    private Processor processor;
    private Pipeline pipeline;
    private Data data;

    @Before
    public void init() throws IOException {
        List<String> uppercase = Collections.unmodifiableList(Collections.singletonList("foo"));
        processor = new MutateProcessor(null, null, null, null, null, null, null, null, uppercase, null);
        pipeline = new Pipeline(ParsedSimulateRequest.Parser.SIMULATED_PIPELINE_ID, null, Collections.unmodifiableList(Arrays.asList(processor)));
        data = new Data("_index", "_type", "_id", Collections.emptyMap());
        processorRegistry = new HashMap<>();
        processorRegistry.put("mutate", new MutateProcessor.Factory());
        store = mock(PipelineStore.class);
        when(store.get("_id")).thenReturn(pipeline);
        when(store.getProcessorFactoryRegistry()).thenReturn(processorRegistry);
    }

    public void testParse_UsingPipelineStore() throws Exception {
        ParsedSimulateRequest expectedRequest = new ParsedSimulateRequest(pipeline, Collections.singletonList(data), false);

        Map<String, Object> raw = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        Map<String, Object> doc = new HashMap<>();
        doc.put("_index", "_index");
        doc.put("_type", "_type");
        doc.put("_id", "_id");
        docs.add(doc);
        raw.put("docs", docs);

        ParsedSimulateRequest actualRequest = PARSER.parse("_id", raw, false, store);
        assertThat(actualRequest, equalTo(expectedRequest));
    }

    public void testParse_ProvidedPipeline() throws Exception {
        ParsedSimulateRequest expectedRequest = new ParsedSimulateRequest(pipeline, Collections.singletonList(data), false);

        Map<String, Object> raw = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        Map<String, Object> doc = new HashMap<>();
        doc.put("_index", "_index");
        doc.put("_type", "_type");
        doc.put("_id", "_id");
        docs.add(doc);

        Map<String, Object> processorConfig = new HashMap<>();
        processorConfig.put("uppercase", Arrays.asList("foo"));
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put("processors", Collections.singletonList(Collections.singletonMap("mutate", processorConfig)));

        raw.put("docs", docs);
        raw.put("pipeline", pipelineConfig);

        ParsedSimulateRequest actualRequest = PARSER.parse(null, raw, false, store);
        assertThat(actualRequest, equalTo(expectedRequest));
    }
}

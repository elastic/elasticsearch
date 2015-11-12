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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.Data;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.plugin.ingest.PipelineStore;
import org.elasticsearch.plugin.ingest.transport.simulate.SimulatePipelineResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.*;

public class SimulateExecutionServiceTests extends ESTestCase {

    private PipelineStore store;
    private ThreadPool threadPool;
    private SimulateExecutionService executionService;
    private Pipeline pipeline;
    private Processor processor;
    private Data data;

    @Before
    public void setup() {
        store = mock(PipelineStore.class);
        threadPool = new ThreadPool(
                Settings.builder()
                        .put("name", "_name")
                        .build()
        );
        executionService = new SimulateExecutionService(threadPool);
        processor = mock(Processor.class);
        when(processor.getType()).thenReturn("mock");
        pipeline = new Pipeline("_id", "_description", Arrays.asList(processor, processor));
        data = new Data("_index", "_type", "_id", Collections.emptyMap());
    }

    @After
    public void destroy() {
        threadPool.shutdown();
    }

    public void testExecuteVerboseItem() throws Exception {
        SimulatedItemResponse expectedItemResponse = new SimulatedItemResponse(
                Arrays.asList(new ProcessedData("processor[mock]-0", data), new ProcessedData("processor[mock]-1", data)));
        SimulatedItemResponse actualItemResponse = executionService.executeVerboseItem(pipeline, data);
        verify(processor, times(2)).execute(data);
        assertThat(actualItemResponse, equalTo(expectedItemResponse));
    }

    public void testExecuteItem_verboseSuccessful() throws Exception {
        SimulatedItemResponse expectedItemResponse = new SimulatedItemResponse(
                Arrays.asList(new ProcessedData("processor[mock]-0", data), new ProcessedData("processor[mock]-1", data)));
        SimulatedItemResponse actualItemResponse = executionService.executeItem(pipeline, data, true);
        verify(processor, times(2)).execute(data);
        assertThat(actualItemResponse, equalTo(expectedItemResponse));
    }

    public void testExecuteItem_Simple() throws Exception {
        SimulatedItemResponse expectedItemResponse = new SimulatedItemResponse(data);
        SimulatedItemResponse actualItemResponse = executionService.executeItem(pipeline, data, false);
        verify(processor, times(2)).execute(data);
        assertThat(actualItemResponse, equalTo(expectedItemResponse));
    }

    public void testExecuteItem_Failure() throws Exception {
        Exception e = new RuntimeException("processor failed");
        SimulatedItemResponse expectedItemResponse = new SimulatedItemResponse(e);
        doThrow(e).when(processor).execute(data);
        SimulatedItemResponse actualItemResponse = executionService.executeItem(pipeline, data, false);
        verify(processor, times(1)).execute(data);
        assertThat(actualItemResponse, equalTo(expectedItemResponse));
    }

    public void testExecute() throws Exception {
        SimulateExecutionService.Listener listener = mock(SimulateExecutionService.Listener.class);
        SimulatedItemResponse itemResponse = new SimulatedItemResponse(data);
        ParsedSimulateRequest request = new ParsedSimulateRequest(pipeline, Collections.singletonList(data), false);
        executionService.execute(request, listener);
        SimulatePipelineResponse response = new SimulatePipelineResponse("_id", Collections.singletonList(itemResponse));
        assertBusy(new Runnable() {
            @Override
            public void run() {
                verify(processor, times(2)).execute(data);
                verify(listener).onResponse(response);
            }
        });
    }

    public void testExecute_Verbose() throws Exception {
        SimulateExecutionService.Listener listener = mock(SimulateExecutionService.Listener.class);
        ParsedSimulateRequest request = new ParsedSimulateRequest(pipeline, Collections.singletonList(data), true);
        SimulatedItemResponse itemResponse = new SimulatedItemResponse(
                Arrays.asList(new ProcessedData("processor[mock]-0", data), new ProcessedData("processor[mock]-1", data)));
        executionService.execute(request, listener);
        SimulatePipelineResponse response = new SimulatePipelineResponse("_id", Collections.singletonList(itemResponse));
        assertBusy(new Runnable() {
            @Override
            public void run() {
                verify(processor, times(2)).execute(data);
                verify(listener).onResponse(response);
            }
        });
    }
}

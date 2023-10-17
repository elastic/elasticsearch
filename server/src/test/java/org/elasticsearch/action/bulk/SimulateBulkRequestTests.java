/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SimulateBulkRequestTests extends ESTestCase {

    public void testSerialization() throws Exception {
        Map<String, Object> pipelineSubstitutions = getTestPipelineSubstitutions();
        SimulateBulkRequest simulateBulkRequest = new SimulateBulkRequest();
        simulateBulkRequest.setPipelineSubstitutions(pipelineSubstitutions);
        /*
         * Note: SimulateBulkRequest does not implement equals or hashCode, so we can't test serialization in the usual way for a
         * Writable
         */
        SimulateBulkRequest copy = copyWriteable(simulateBulkRequest, null, SimulateBulkRequest::new);
        assertThat(copy.pipelineSubstitutions, equalTo(simulateBulkRequest.pipelineSubstitutions));
    }

    public void testGetPipelineSubstitutions() throws Exception {
        Map<String, Object> pipelineSubstitutions = getTestPipelineSubstitutions();
        SimulateBulkRequest simulateBulkRequest = new SimulateBulkRequest();
        simulateBulkRequest.setPipelineSubstitutions(pipelineSubstitutions);
        Map<String, Pipeline> returnedPipelineSubstitions = simulateBulkRequest.getPipelineSubstitutions(getTestIngestService());
        assertThat(returnedPipelineSubstitions.size(), equalTo(pipelineSubstitutions.size()));
        for (String pipelineId : pipelineSubstitutions.keySet()) {
            List<Processor> processors = returnedPipelineSubstitions.get(pipelineId).getProcessors();
        }
    }

    private Map<String, Object> getTestPipelineSubstitutions() {
        return new HashMap<>() {
            {
                put("pipeline1", new HashMap<>() {
                    {
                        put("processors", List.of(new HashMap<>() {
                            {
                                put("processor2", new HashMap<>());
                            }
                        }, new HashMap<>() {
                            {
                                put("processor3", new HashMap<>());
                            }
                        }));
                    }
                });
                put("pipeline2", new HashMap<>() {
                    {
                        put("processors", List.of(new HashMap<>() {
                            {
                                put("processor3", new HashMap<>());
                            }
                        }));
                    }
                });
            }
        };
    }

    private IngestService getTestIngestService() throws Exception {
        IngestService ingestService = mock(IngestService.class);
        Processor.Factory processorFactory = mock(Processor.Factory.class);
        ArgumentCaptor<String> messageCapture = ArgumentCaptor.forClass(String.class);
        when(processorFactory.create(any(), messageCapture.capture(), any(), any())).thenReturn(new Processor() {
            @Override
            public String getType() {
                return messageCapture.getValue();
            }

            @Override
            public String getTag() {
                return null;
            }

            @Override
            public String getDescription() {
                return null;
            }
        });
        when(ingestService.getProcessorFactories()).thenReturn(
            Map.of("processor1", processorFactory, "processor2", processorFactory, "processor3", processorFactory)
        );
        return ingestService;
    }
}

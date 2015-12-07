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

package org.elasticsearch.ingest.processor;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;

import static org.elasticsearch.mock.orig.Mockito.verify;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

public class CompoundProcessorTests extends ESTestCase {
    private IngestDocument ingestDocument;

    @Before
    public void init() {
        ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
    }

    public void testEmpty() throws Exception {
        CompoundProcessor processor = new CompoundProcessor();
        assertThat(processor.getProcessors().isEmpty(), is(true));
        assertThat(processor.getOnFailureProcessors().isEmpty(), is(true));
        processor.execute(ingestDocument);
    }

    public void testSingleProcessor() throws Exception {
        Processor processor = mock(Processor.class);
        CompoundProcessor compoundProcessor = new CompoundProcessor(processor);
        assertThat(compoundProcessor.getProcessors().size(), equalTo(1));
        assertThat(compoundProcessor.getProcessors().get(0), equalTo(processor));
        assertThat(compoundProcessor.getOnFailureProcessors().isEmpty(), is(true));
        compoundProcessor.execute(ingestDocument);
        verify(processor, times(1)).execute(ingestDocument);
    }

    public void testSingleProcessorWithException() throws Exception {
        Processor processor = mock(Processor.class);
        doThrow(new RuntimeException("error")).doNothing().when(processor).execute(ingestDocument);
        CompoundProcessor compoundProcessor = new CompoundProcessor(processor);
        assertThat(compoundProcessor.getProcessors().size(), equalTo(1));
        assertThat(compoundProcessor.getProcessors().get(0), equalTo(processor));
        assertThat(compoundProcessor.getOnFailureProcessors().isEmpty(), is(true));
        try {
            compoundProcessor.execute(ingestDocument);
            fail("should throw exception");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("error"));
        }
        verify(processor, times(1)).execute(ingestDocument);
    }

    public void testSingleProcessorWithOnFailureProcessor() throws Exception {
        Processor processor = mock(Processor.class);
        doThrow(new RuntimeException("error")).doNothing().when(processor).execute(ingestDocument);
        Processor processorNext = mock(Processor.class);
        CompoundProcessor compoundProcessor = new CompoundProcessor(Arrays.asList(processor), Arrays.asList(processorNext));
        assertThat(compoundProcessor.getProcessors().size(), equalTo(1));
        assertThat(compoundProcessor.getProcessors().get(0), equalTo(processor));
        assertThat(compoundProcessor.getOnFailureProcessors().size(), equalTo(1));
        assertThat(compoundProcessor.getOnFailureProcessors().get(0), equalTo(processorNext));
        compoundProcessor.execute(ingestDocument);
        verify(processor, times(1)).execute(ingestDocument);
        verify(processorNext, times(1)).execute(ingestDocument);
    }

    public void testSingleProcessorWithNestedFailures() throws Exception {
        Processor processor = mock(Processor.class);
        doThrow(new RuntimeException("error")).doNothing().when(processor).execute(ingestDocument);
        Processor processorToFail = mock(Processor.class);
        doThrow(new RuntimeException("error")).doNothing().when(processorToFail).execute(ingestDocument);
        Processor lastProcessor = mock(Processor.class);

        CompoundProcessor innerCompoundOnFailProcessor = new CompoundProcessor(Arrays.asList(processorToFail), Arrays.asList(lastProcessor));
        CompoundProcessor compoundOnFailProcessor = spy(innerCompoundOnFailProcessor);

        CompoundProcessor innerCompoundProcessor = new CompoundProcessor(Arrays.asList(processor), Arrays.asList(compoundOnFailProcessor));
        CompoundProcessor compoundProcessor = spy(innerCompoundProcessor);

        assertThat(compoundProcessor.getProcessors().size(), equalTo(1));
        assertThat(compoundProcessor.getProcessors().get(0), equalTo(processor));
        assertThat(compoundProcessor.getOnFailureProcessors().size(), equalTo(1));
        assertThat(compoundProcessor.getOnFailureProcessors().get(0), equalTo(compoundOnFailProcessor));
        compoundProcessor.execute(ingestDocument);
        verify(processor, times(1)).execute(ingestDocument);
        verify(compoundProcessor, times(1)).executeOnFailure(ingestDocument);
        verify(compoundOnFailProcessor, times(1)).execute(ingestDocument);
        verify(processorToFail, times(1)).execute(ingestDocument);
        verify(compoundOnFailProcessor, times(1)).executeOnFailure(ingestDocument);
        verify(lastProcessor, times(1)).execute(ingestDocument);
    }
}

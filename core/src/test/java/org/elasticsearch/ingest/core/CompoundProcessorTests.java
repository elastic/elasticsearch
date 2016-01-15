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

package org.elasticsearch.ingest.core;

import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;

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
        TestProcessor processor = new TestProcessor(ingestDocument -> {});
        CompoundProcessor compoundProcessor = new CompoundProcessor(processor);
        assertThat(compoundProcessor.getProcessors().size(), equalTo(1));
        assertThat(compoundProcessor.getProcessors().get(0), equalTo(processor));
        assertThat(compoundProcessor.getOnFailureProcessors().isEmpty(), is(true));
        compoundProcessor.execute(ingestDocument);
        assertThat(processor.getInvokedCounter(), equalTo(1));
    }

    public void testSingleProcessorWithException() throws Exception {
        TestProcessor processor = new TestProcessor(ingestDocument -> {throw new RuntimeException("error");});
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
        assertThat(processor.getInvokedCounter(), equalTo(1));
    }

    public void testSingleProcessorWithOnFailureProcessor() throws Exception {
        TestProcessor processor1 = new TestProcessor("id", "first", ingestDocument -> {throw new RuntimeException("error");});
        TestProcessor processor2 = new TestProcessor(ingestDocument -> {
            Map<String, String> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.size(), equalTo(2));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_FIELD), equalTo("first"));
        });

        CompoundProcessor compoundProcessor = new CompoundProcessor(Collections.singletonList(processor1), Collections.singletonList(processor2));
        compoundProcessor.execute(ingestDocument);

        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertThat(processor2.getInvokedCounter(), equalTo(1));
    }

    public void testSingleProcessorWithNestedFailures() throws Exception {
        TestProcessor processor = new TestProcessor("id", "first", ingestDocument -> {throw new RuntimeException("error");});
        TestProcessor processorToFail = new TestProcessor("id", "second", ingestDocument -> {
            Map<String, String> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.size(), equalTo(2));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_FIELD), equalTo("first"));
            throw new RuntimeException("error");
        });
        TestProcessor lastProcessor = new TestProcessor(ingestDocument -> {
            Map<String, String> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.size(), equalTo(2));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_FIELD), equalTo("second"));
        });
        CompoundProcessor compoundOnFailProcessor = new CompoundProcessor(Collections.singletonList(processorToFail), Collections.singletonList(lastProcessor));
        CompoundProcessor compoundProcessor = new CompoundProcessor(Collections.singletonList(processor), Collections.singletonList(compoundOnFailProcessor));
        compoundProcessor.execute(ingestDocument);

        assertThat(processorToFail.getInvokedCounter(), equalTo(1));
        assertThat(lastProcessor.getInvokedCounter(), equalTo(1));
    }
}

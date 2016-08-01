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

package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

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
        assertThat(compoundProcessor.getProcessors().get(0), sameInstance(processor));
        assertThat(compoundProcessor.getOnFailureProcessors().isEmpty(), is(true));
        compoundProcessor.execute(ingestDocument);
        assertThat(processor.getInvokedCounter(), equalTo(1));
    }

    public void testSingleProcessorWithException() throws Exception {
        TestProcessor processor = new TestProcessor(ingestDocument -> {throw new RuntimeException("error");});
        CompoundProcessor compoundProcessor = new CompoundProcessor(processor);
        assertThat(compoundProcessor.getProcessors().size(), equalTo(1));
        assertThat(compoundProcessor.getProcessors().get(0), sameInstance(processor));
        assertThat(compoundProcessor.getOnFailureProcessors().isEmpty(), is(true));
        try {
            compoundProcessor.execute(ingestDocument);
            fail("should throw exception");
        } catch (ElasticsearchException e) {
            assertThat(e.getRootCause().getMessage(), equalTo("error"));
        }
        assertThat(processor.getInvokedCounter(), equalTo(1));
    }

    public void testIgnoreFailure() throws Exception {
        TestProcessor processor1 = new TestProcessor(ingestDocument -> {throw new RuntimeException("error");});
        TestProcessor processor2 = new TestProcessor(ingestDocument -> {ingestDocument.setFieldValue("field", "value");});
        CompoundProcessor compoundProcessor = new CompoundProcessor(true, Arrays.asList(processor1, processor2), Collections.emptyList());
        compoundProcessor.execute(ingestDocument);
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertThat(processor2.getInvokedCounter(), equalTo(1));
        assertThat(ingestDocument.getFieldValue("field", String.class), equalTo("value"));
    }

    public void testSingleProcessorWithOnFailureProcessor() throws Exception {
        TestProcessor processor1 = new TestProcessor("id", "first", ingestDocument -> {throw new RuntimeException("error");});
        TestProcessor processor2 = new TestProcessor(ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.size(), equalTo(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("first"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("id"));
        });

        CompoundProcessor compoundProcessor = new CompoundProcessor(false, Collections.singletonList(processor1),
                Collections.singletonList(processor2));
        compoundProcessor.execute(ingestDocument);

        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertThat(processor2.getInvokedCounter(), equalTo(1));
    }

    public void testSingleProcessorWithNestedFailures() throws Exception {
        TestProcessor processor = new TestProcessor("id", "first", ingestDocument -> {throw new RuntimeException("error");});
        TestProcessor processorToFail = new TestProcessor("id2", "second", ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.size(), equalTo(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("first"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("id"));
            throw new RuntimeException("error");
        });
        TestProcessor lastProcessor = new TestProcessor(ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.size(), equalTo(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("second"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("id2"));
        });
        CompoundProcessor compoundOnFailProcessor = new CompoundProcessor(false, Collections.singletonList(processorToFail),
                Collections.singletonList(lastProcessor));
        CompoundProcessor compoundProcessor = new CompoundProcessor(false, Collections.singletonList(processor),
                Collections.singletonList(compoundOnFailProcessor));
        compoundProcessor.execute(ingestDocument);

        assertThat(processorToFail.getInvokedCounter(), equalTo(1));
        assertThat(lastProcessor.getInvokedCounter(), equalTo(1));
    }

    public void testCompoundProcessorExceptionFailWithoutOnFailure() throws Exception {
        TestProcessor firstProcessor = new TestProcessor("id1", "first", ingestDocument -> {throw new RuntimeException("error");});
        TestProcessor secondProcessor = new TestProcessor("id3", "second", ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.entrySet(), hasSize(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("first"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("id1"));
        });

        CompoundProcessor failCompoundProcessor = new CompoundProcessor(firstProcessor);

        CompoundProcessor compoundProcessor = new CompoundProcessor(false, Collections.singletonList(failCompoundProcessor),
            Collections.singletonList(secondProcessor));
        compoundProcessor.execute(ingestDocument);

        assertThat(firstProcessor.getInvokedCounter(), equalTo(1));
        assertThat(secondProcessor.getInvokedCounter(), equalTo(1));
    }

    public void testCompoundProcessorExceptionFail() throws Exception {
        TestProcessor firstProcessor = new TestProcessor("id1", "first", ingestDocument -> {throw new RuntimeException("error");});
        TestProcessor failProcessor =
                new TestProcessor("tag_fail", "fail", ingestDocument -> {throw new RuntimeException("custom error message");});
        TestProcessor secondProcessor = new TestProcessor("id3", "second", ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.entrySet(), hasSize(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("custom error message"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("fail"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("tag_fail"));
        });

        CompoundProcessor failCompoundProcessor = new CompoundProcessor(false, Collections.singletonList(firstProcessor),
            Collections.singletonList(failProcessor));

        CompoundProcessor compoundProcessor = new CompoundProcessor(false, Collections.singletonList(failCompoundProcessor),
            Collections.singletonList(secondProcessor));
        compoundProcessor.execute(ingestDocument);

        assertThat(firstProcessor.getInvokedCounter(), equalTo(1));
        assertThat(secondProcessor.getInvokedCounter(), equalTo(1));
    }

    public void testCompoundProcessorExceptionFailInOnFailure() throws Exception {
        TestProcessor firstProcessor = new TestProcessor("id1", "first", ingestDocument -> {throw new RuntimeException("error");});
        TestProcessor failProcessor =
                new TestProcessor("tag_fail", "fail", ingestDocument -> {throw new RuntimeException("custom error message");});
        TestProcessor secondProcessor = new TestProcessor("id3", "second", ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.entrySet(), hasSize(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("custom error message"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("fail"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("tag_fail"));
        });

        CompoundProcessor failCompoundProcessor = new CompoundProcessor(false, Collections.singletonList(firstProcessor),
            Collections.singletonList(new CompoundProcessor(failProcessor)));

        CompoundProcessor compoundProcessor = new CompoundProcessor(false, Collections.singletonList(failCompoundProcessor),
            Collections.singletonList(secondProcessor));
        compoundProcessor.execute(ingestDocument);

        assertThat(firstProcessor.getInvokedCounter(), equalTo(1));
        assertThat(secondProcessor.getInvokedCounter(), equalTo(1));
    }

    public void testBreakOnFailure() throws Exception {
        TestProcessor firstProcessor = new TestProcessor("id1", "first", ingestDocument -> {throw new RuntimeException("error1");});
        TestProcessor secondProcessor = new TestProcessor("id2", "second", ingestDocument -> {throw new RuntimeException("error2");});
        TestProcessor onFailureProcessor = new TestProcessor("id2", "on_failure", ingestDocument -> {});
        CompoundProcessor pipeline = new CompoundProcessor(false, Arrays.asList(firstProcessor, secondProcessor),
            Collections.singletonList(onFailureProcessor));
        pipeline.execute(ingestDocument);
        assertThat(firstProcessor.getInvokedCounter(), equalTo(1));
        assertThat(secondProcessor.getInvokedCounter(), equalTo(0));
        assertThat(onFailureProcessor.getInvokedCounter(), equalTo(1));

    }
}

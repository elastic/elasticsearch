/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.ingest.processor.conditional;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.test.ESTestCase;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class ConditionalProcessorTests extends ESTestCase {

    public void testIf() throws Exception {
        Map<String, Object> doc = new HashMap<>();
        doc.put("foo", 20);
        IngestDocument ingestDocument = new IngestDocument("index", "type", "id", doc);

        Processor matchProcessor = mock(Processor.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                IngestDocument i = (IngestDocument) invocation.getArguments()[0];
                i.setFieldValue("foo", 10);
                return null;
            }
        }).when(matchProcessor).execute(ingestDocument);

        ConditionalProcessor processor = new ConditionalProcessor(new RelationalExpression("foo", "gte", 13), Collections.singletonList(matchProcessor), Collections.EMPTY_LIST);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("foo", Integer.class), equalTo(10));
    }

    public void testElse() throws Exception {
        Map<String, Object> doc = new HashMap<>();
        doc.put("foo", 0);
        IngestDocument ingestDocument = new IngestDocument("index", "type", "id", doc);

        Processor matchProcessor = mock(Processor.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                IngestDocument i = (IngestDocument) invocation.getArguments()[0];
                i.setFieldValue("foo", 10);
                return null;
            }
        }).when(matchProcessor).execute(ingestDocument);

        ConditionalProcessor processor = new ConditionalProcessor(new RelationalExpression("foo", "gte", 13), Collections.EMPTY_LIST, Collections.singletonList(matchProcessor));
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("foo", Integer.class), equalTo(10));
    }
}

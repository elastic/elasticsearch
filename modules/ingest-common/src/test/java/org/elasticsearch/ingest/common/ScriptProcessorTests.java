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

package org.elasticsearch.ingest.common;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScriptProcessorTests extends ESTestCase {

    public void testScripting() throws Exception {
        int randomBytesIn = randomInt();
        int randomBytesOut = randomInt();
        int randomBytesTotal = randomBytesIn + randomBytesOut;

        ScriptService scriptService = mock(ScriptService.class);
        Script script = mockScript("_script");
        ExecutableScript.Factory factory = mock(ExecutableScript.Factory.class);
        ExecutableScript executableScript = mock(ExecutableScript.class);
        when(scriptService.compile(script, ExecutableScript.INGEST_CONTEXT)).thenReturn(factory);
        when(factory.newInstance(any())).thenReturn(executableScript);

        Map<String, Object> document = new HashMap<>();
        document.put("bytes_in", randomInt());
        document.put("bytes_out", randomInt());
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        doAnswer(invocationOnMock ->  {
            ingestDocument.setFieldValue("bytes_total", randomBytesTotal);
            return null;
        }).when(executableScript).run();

        ScriptProcessor processor = new ScriptProcessor(randomAlphaOfLength(10), script, scriptService);

        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_in"));
        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_out"));
        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_total"));
        assertThat(ingestDocument.getSourceAndMetadata().get("bytes_total"), is(randomBytesTotal));
    }
}

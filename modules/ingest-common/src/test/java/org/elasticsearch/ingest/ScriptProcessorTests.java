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

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScriptProcessorTests extends ESTestCase {

    public void testScripting() throws Exception {
        int randomInt = randomInt();
        ScriptService scriptService = mock(ScriptService.class);
        ClusterService clusterService = mock(ClusterService.class);
        CompiledScript compiledScript = mock(CompiledScript.class);
        Script script = mock(Script.class);
        when(scriptService.compile(any(), any(), any(), any())).thenReturn(compiledScript);
        ExecutableScript executableScript = mock(ExecutableScript.class);
        when(scriptService.executable(any(), any())).thenReturn(executableScript);
        when(executableScript.run()).thenReturn(randomInt);

        ScriptProcessor processor = new ScriptProcessor(randomAsciiOfLength(10), script,
            scriptService, clusterService, "bytes_total");

        Map<String, Object> document = new HashMap<>();
        document.put("bytes_in", 1234);
        document.put("bytes_out", 4321);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_in"));
        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_out"));
        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_total"));
        assertThat(ingestDocument.getSourceAndMetadata().get("bytes_total"), is(randomInt));
    }
}

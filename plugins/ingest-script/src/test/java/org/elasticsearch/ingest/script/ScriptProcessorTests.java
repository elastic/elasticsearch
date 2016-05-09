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

package org.elasticsearch.ingest.script;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScriptProcessorTests extends ESTestCase {

    public void testThatScriptingWorks() throws Exception {
        int randomInt = randomInt();
        ClusterService clusterService = mock(ClusterService.class);

        ScriptService scriptService = mock(ScriptService.class);
        CompiledScript compiledScript = mock(CompiledScript.class);
        when(scriptService.compile(any(), any(), any(), any())).thenReturn(compiledScript);
        ExecutableScript executableScript = mock(ExecutableScript.class);
        when(scriptService.executable(any(), any())).thenReturn(executableScript);
        when(executableScript.run()).thenReturn(randomInt);

        ScriptProcessor processor = new ScriptProcessor(randomAsciiOfLength(10), "bytes_total", mock(Script.class),
                scriptService, clusterService);

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

    public void testFactoryValidationForMultipleScriptingTypes() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        String randomType = randomFrom("id", "inline", "file");
        String otherRandomType = randomFrom("id", "inline", "file");
        while (randomType.equals(otherRandomType)) {
            otherRandomType = randomFrom("id", "inline", "file");
        }

        configMap.put(randomType, "foo");
        configMap.put(otherRandomType, "bar");
        configMap.put("field", "my_field");

        ScriptProcessor.Factory factory = new ScriptProcessor.Factory(mock(Node.class));
        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> factory.doCreate(randomAsciiOfLength(10), configMap));

        assertThat(exception.getMessage(), is("Only one of [file], [id] or [inline] may be configured"));
    }

    public void testFactoryValidationAtLeastOneScriptingType() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("field", "my_field");

        ScriptProcessor.Factory factory = new ScriptProcessor.Factory(mock(Node.class));
        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> factory.doCreate(randomAsciiOfLength(10), configMap));

        assertThat(exception.getMessage(), is("Need [file], [id] or [inline] parameter to refer to scripts"));
    }
}

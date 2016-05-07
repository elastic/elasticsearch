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

import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.ingest.core.TemplateService;
import org.elasticsearch.test.ESTestCase;

public class ScriptProcessorTests extends ESTestCase {

    public void testMockScript() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String script = "5";
        Processor processor = createSetProcessor(script);
        processor.execute(ingestDocument);
    }

    private static Processor createSetProcessor(String script) {
        TemplateService templateService = TestTemplateService.instance();
        return new ScriptProcessor(randomAsciiOfLength(10),
            templateService.compile(script),templateService.getScriptService(), "mockscript", "newField");
    }
}

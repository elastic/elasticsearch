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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;

public class ConditionalProcessorTests extends ESTestCase {

    public void testChecksCondition() throws Exception {
        String conditionalField = "field1";
        String scriptSource = "conditionalScript";
        String trueValue = "truthy";
        ScriptService scriptService = new ScriptService(Settings.builder().build(),
            Collections.singletonMap(
                Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(
                    Script.DEFAULT_SCRIPT_LANG,
                    Collections.singletonMap(
                        scriptSource, ctx -> trueValue.equals(ctx.get(conditionalField))
                    )
                )
            ),
            new HashMap<>(ScriptModule.CORE_CONTEXTS)
        );
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> setConfig = new HashMap<>();
        setConfig.put("field", "foo");
        setConfig.put("value", "bar");
        Map<String, Object> config = new HashMap<>();
        config.put("processor", Collections.singletonMap("set", setConfig));
        config.put("script", Collections.singletonMap("source", scriptSource));
        ConditionalProcessor processor = new ConditionalProcessor.Factory(scriptService).create(
            Collections.singletonMap("set", new SetProcessor.Factory(scriptService)),
            ConditionalProcessor.TYPE, config
        );

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument.setFieldValue(conditionalField, trueValue);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getSourceAndMetadata().get(conditionalField), is(trueValue));
        assertThat(ingestDocument.getSourceAndMetadata().get("foo"), is("bar"));

        String falseValue = "falsy";
        ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument.setFieldValue(conditionalField, falseValue);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getSourceAndMetadata().get(conditionalField), is(falseValue));
        assertThat(ingestDocument.getSourceAndMetadata(), not(hasKey("foo")));
    }
}

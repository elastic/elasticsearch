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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;

public class ConditionalProcessorTests extends ESTestCase {

    public void testChecksCondition() throws Exception {
        String conditionalField = "field1";
        String scriptName = "conditionalScript";
        String trueValue = "truthy";
        ScriptService scriptService = new ScriptService(Settings.builder().build(),
            Collections.singletonMap(
                Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(
                    Script.DEFAULT_SCRIPT_LANG,
                    Collections.singletonMap(
                        scriptName, ctx -> trueValue.equals(ctx.get(conditionalField))
                    )
                )
            ),
            new HashMap<>(ScriptModule.CORE_CONTEXTS)
        );
        Map<String, Object> document = new HashMap<>();
        ConditionalProcessor processor = new ConditionalProcessor(
            randomAlphaOfLength(10),
            new Script(
                ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                scriptName, Collections.emptyMap()), scriptService,
            new Processor() {
                @Override
                public void execute(final IngestDocument ingestDocument) throws Exception {
                    ingestDocument.setFieldValue("foo", "bar");
                }

                @Override
                public String getType() {
                    return null;
                }

                @Override
                public String getTag() {
                    return null;
                }
            });

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

    @SuppressWarnings("unchecked")
    public void testActsOnImmutableData() throws Exception {
        assertMutatingCtxThrows(ctx -> ctx.remove("foo"));
        assertMutatingCtxThrows(ctx -> ctx.put("foo", "bar"));
        assertMutatingCtxThrows(ctx -> ((List<Object>)ctx.get("listField")).add("bar"));
        assertMutatingCtxThrows(ctx -> ((List<Object>)ctx.get("listField")).remove("bar"));
    }

    private static void assertMutatingCtxThrows(Consumer<Map<String, Object>> mutation) throws Exception {
        String scriptName = "conditionalScript";
        CompletableFuture<Exception> expectedException = new CompletableFuture<>();
        ScriptService scriptService = new ScriptService(Settings.builder().build(),
            Collections.singletonMap(
                Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(
                    Script.DEFAULT_SCRIPT_LANG,
                    Collections.singletonMap(
                        scriptName, ctx -> {
                            try {
                                mutation.accept(ctx);
                            } catch (Exception e) {
                                expectedException.complete(e);
                            }
                            return false;
                        }
                    )
                )
            ),
            new HashMap<>(ScriptModule.CORE_CONTEXTS)
        );
        Map<String, Object> document = new HashMap<>();
        ConditionalProcessor processor = new ConditionalProcessor(
            randomAlphaOfLength(10),
            new Script(
                ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                scriptName, Collections.emptyMap()), scriptService, null
        );
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);
        ingestDocument.setFieldValue("listField", new ArrayList<>());
        processor.execute(ingestDocument);
        Exception e = expectedException.get();
        assertThat(e, instanceOf(UnsupportedOperationException.class));
        assertEquals("Mutating ingest documents in conditionals is not supported", e.getMessage());
    }
}

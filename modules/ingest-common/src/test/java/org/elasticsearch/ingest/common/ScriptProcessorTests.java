/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.script.IngestScript;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ScriptProcessorTests extends ESTestCase {

    private ScriptService scriptService;
    private Script script;
    private IngestScript.Factory ingestScriptFactory;

    @Before
    public void setupScripting() {
        String scriptName = "script";
        scriptService = scriptService(scriptName, ctx -> {
            Integer bytesIn = (Integer) ctx.get("bytes_in");
            Integer bytesOut = (Integer) ctx.get("bytes_out");
            ctx.put("bytes_total", bytesIn + bytesOut);
            ctx.put("_dynamic_templates", Map.of("foo", "bar"));
            return null;
        });
        script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Map.of());
        ingestScriptFactory = scriptService.compile(script, IngestScript.CONTEXT);
    }

    public void testScriptingWithoutPrecompiledScriptFactory() throws Exception {
        ScriptProcessor processor = new ScriptProcessor(randomAlphaOfLength(10), null, script, null, scriptService);
        IngestDocument ingestDocument = randomDocument();
        processor.execute(ingestDocument);
        assertIngestDocument(ingestDocument);
    }

    public void testScriptingWithPrecompiledIngestScript() {
        ScriptProcessor processor = new ScriptProcessor(randomAlphaOfLength(10), null, script, ingestScriptFactory, scriptService);
        IngestDocument ingestDocument = randomDocument();
        processor.execute(ingestDocument);
        assertIngestDocument(ingestDocument);
    }

    @SuppressWarnings("unchecked")
    public void testScriptCanAddArbitraryIngestMetadata() {
        String scriptName = "arbitrary_ingest_key";
        ScriptService scriptService = scriptService(scriptName, ctx -> {
            ((Map<String, Object>) ctx.get("_ingest")).put("foo", "bar");
            return null;
        });
        Script script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Map.of());
        IngestScript.Factory factory = scriptService.compile(script, IngestScript.CONTEXT);
        ScriptProcessor processor = new ScriptProcessor(randomAlphaOfLength(10), null, script, factory, scriptService);
        IngestDocument ingestDocument = randomDocument();

        processor.execute(ingestDocument);

        assertThat(ingestDocument.getIngestMetadata().get("foo"), equalTo("bar"));
    }

    public void testScriptAssignmentToIngestKeyWritesSourceField() {
        String scriptName = "replace_ingest";
        ScriptService scriptService = scriptService(scriptName, ctx -> {
            ctx.put("_ingest", "source-value");
            ctx.put("ingest_value_after_assignment", ((Map<?, ?>) ctx.get("_ingest")).get("_value"));
            return null;
        });
        Script script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Map.of());
        IngestScript.Factory factory = scriptService.compile(script, IngestScript.CONTEXT);
        ScriptProcessor processor = new ScriptProcessor(randomAlphaOfLength(10), null, script, factory, scriptService);
        IngestDocument ingestDocument = randomDocument();
        ingestDocument.getIngestMetadata().put("_value", "metadata-value");

        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSource().get("_ingest"), equalTo("source-value"));
        assertThat(ingestDocument.getSource().get("ingest_value_after_assignment"), equalTo("metadata-value"));
        assertThat(ingestDocument.getIngestMetadata(), not(hasKey("_ingest")));
    }

    public void testFieldApiAccessesSourceNotIngestMetadata() {
        IngestScript.Factory factory = (params, ctx) -> new IngestScript(params, ctx) {
            @Override
            public void execute() {
                field("_ingest").set("source-value");
            }
        };
        ScriptProcessor processor = new ScriptProcessor(randomAlphaOfLength(10), null, script, factory, scriptService);
        IngestDocument ingestDocument = randomDocument();

        processor.execute(ingestDocument);

        assertThat(ingestDocument.getSource().get("_ingest"), equalTo("source-value"));
        assertThat(ingestDocument.getIngestMetadata(), not(hasKey("_ingest")));
    }

    private ScriptService scriptService(String scriptName, java.util.function.Function<Map<String, Object>, Object> script) {
        return new ScriptService(
            Settings.builder().build(),
            Map.of(Script.DEFAULT_SCRIPT_LANG, new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Map.of(scriptName, script), Map.of())),
            new HashMap<>(ScriptModule.CORE_CONTEXTS),
            () -> 1L,
            TestProjectResolvers.singleProject(randomProjectIdOrDefault())
        );
    }

    private IngestDocument randomDocument() {
        Map<String, Object> document = new HashMap<>();
        document.put("bytes_in", randomInt());
        document.put("bytes_out", randomInt());
        return RandomDocumentPicks.randomIngestDocument(random(), document);
    }

    private void assertIngestDocument(IngestDocument ingestDocument) {
        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_in"));
        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_out"));
        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_total"));
        int bytesTotal = ingestDocument.getFieldValue("bytes_in", Integer.class) + ingestDocument.getFieldValue("bytes_out", Integer.class);
        assertThat(ingestDocument.getSourceAndMetadata().get("bytes_total"), is(bytesTotal));
        assertThat(ingestDocument.getSourceAndMetadata().get("_dynamic_templates"), equalTo(Map.of("foo", "bar")));
    }
}

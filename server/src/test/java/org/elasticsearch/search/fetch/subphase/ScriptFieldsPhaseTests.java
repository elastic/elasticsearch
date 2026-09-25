/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// Covers which script results land on a hit. The heap they retain is charged once per hit by FetchPhase.
public class ScriptFieldsPhaseTests extends ESTestCase {

    private static final String FIELD_NAME = "scripted";

    public void testSuccessfulScriptSetsField() throws Exception {
        try (TestRun run = new TestRun(false)) {
            DocumentField field = run.processHit();
            assertNotNull(field);
            assertEquals(run.scriptPayload, field.getValues());
        }
    }

    public void testScriptExceptionPropagates() throws Exception {
        try (TestRun run = new TestRun(false)) {
            run.throwOnExecute = true;
            RuntimeException thrown = expectThrows(RuntimeException.class, run::processHit);
            assertEquals("boom", thrown.getMessage());
        }
    }

    public void testIgnoredScriptExceptionLeavesNoField() throws Exception {
        try (TestRun run = new TestRun(true)) {
            run.throwOnExecute = true;
            assertNull(run.processHit());
        }
    }

    public void testPreExistingFieldIsNotOverwritten() throws Exception {
        try (TestRun run = new TestRun(false)) {
            DocumentField existing = new DocumentField(FIELD_NAME, List.of("pre-existing"));
            SearchHit hit = SearchHit.unpooled(0, null);
            hit.setDocumentField(existing);
            run.processor.process(new HitContext(hit, run.leafReaderContext, 0, Map.of(), Source.empty(null), null));
            assertSame(existing, hit.field(FIELD_NAME));
        }
    }

    private static final class TestRun implements AutoCloseable {
        final FetchSubPhaseProcessor processor;
        final LeafReaderContext leafReaderContext;
        final TestSearchContext searchContext;
        final List<Object> scriptPayload = buildPayload(50);
        boolean throwOnExecute = false;

        TestRun(boolean ignoreException) throws Exception {
            ScriptFieldsContext scriptFieldsContext = new ScriptFieldsContext();
            scriptFieldsContext.add(new ScriptFieldsContext.ScriptField(FIELD_NAME, ctx -> new TestFieldScript(this), ignoreException));

            // TestSearchContext is used because FetchContext requires a SearchContext; scriptFields() is
            // overridden so ScriptFieldsPhase can read the configured script.
            searchContext = new TestSearchContext((SearchExecutionContext) null) {
                @Override
                public boolean hasScriptFields() {
                    return true;
                }

                @Override
                public ScriptFieldsContext scriptFields() {
                    return scriptFieldsContext;
                }
            };

            MemoryIndex index = new MemoryIndex();
            leafReaderContext = index.createSearcher().getIndexReader().leaves().get(0);

            processor = new ScriptFieldsPhase().getProcessor(new FetchContext(searchContext, null));
            assertNotNull(processor);
            processor.setNextReader(leafReaderContext);
        }

        DocumentField processHit() throws IOException {
            SearchHit hit = SearchHit.unpooled(0, null);
            processor.process(new HitContext(hit, leafReaderContext, 0, Map.of(), Source.empty(null), null));
            return hit.field(FIELD_NAME);
        }

        @Override
        public void close() throws Exception {
            searchContext.close();
        }
    }

    private static List<Object> buildPayload(int entries) {
        List<Object> values = new ArrayList<>(entries);
        for (int i = 0; i < entries; i++) {
            values.add("entry-" + i);
        }
        return values;
    }

    private static final class TestFieldScript extends FieldScript {
        private final TestRun run;

        TestFieldScript(TestRun run) {
            super();
            this.run = run;
        }

        @Override
        public Object execute() {
            if (run.throwOnExecute) {
                throw new RuntimeException("boom");
            }
            return run.scriptPayload;
        }
    }
}

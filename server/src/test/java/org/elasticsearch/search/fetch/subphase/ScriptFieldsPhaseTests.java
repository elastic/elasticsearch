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

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for {@link ScriptFieldsPhase} behaviour (field population, exception handling).
 * <p>
 * Circuit-breaker accounting for script fields is now done by {@code FetchPhase#nextDoc} after all
 * sub-phases have run, using {@link org.elasticsearch.search.SearchHitRamUsageEstimator#estimateDocumentFields}.
 * The byte-level accounting assertions formerly in this class have moved to
 * {@code FetchPhaseFieldAccountingTests}.
 */
public class ScriptFieldsPhaseTests extends ESTestCase {

    private static final String FIELD_NAME = "scripted";

    /** A successful script execution must populate the field on the hit. */
    public void testSuccessfulScriptPopulatesField() throws Exception {
        try (TestRun run = new TestRun(false)) {
            run.processHit();
            assertThat(run.lastHit().field(FIELD_NAME), notNullValue());
        }
    }

    /** If a script throws and ignoreException is false the exception must propagate. */
    public void testScriptExceptionPropagates() throws Exception {
        try (TestRun run = new TestRun(false)) {
            run.throwOnExecute = true;
            expectThrows(RuntimeException.class, run::processHit);
        }
    }

    /** If a script throws and ignoreException is true the field must not appear on the hit. */
    public void testIgnoredScriptExceptionLeavesFieldAbsent() throws Exception {
        try (TestRun run = new TestRun(true)) {
            run.throwOnExecute = true;
            run.processHit();
            assertThat(run.lastHit().field(FIELD_NAME), nullValue());
        }
    }

    /** A pre-existing field must not be replaced by the script output. */
    public void testPreExistingFieldIsNotOverwrittenAndNoBytesCharged() throws Exception {
        try (TestRun run = new TestRun(false)) {
            DocumentField existing = new DocumentField(FIELD_NAME, List.of("pre-existing"));
            SearchHit hit = SearchHit.unpooled(0, null);
            hit.setDocumentField(existing);
            HitContext hitContext = new HitContext(hit, run.leafReaderContext, 0, Map.of(), Source.empty(null), null);
            run.processor.process(hitContext);
            assertSame(existing, hit.field(FIELD_NAME));
        }
    }

    /**
     * ScriptFieldsPhase must NOT charge any bytes directly to the configured checker.
     * Bytes are now charged by FetchPhase#nextDoc after all sub-phases have run.
     */
    public void testScriptFieldPhaseDoesNotChargeBytes() throws Exception {
        List<Long> received = new ArrayList<>();
        // Wire in a checker that would capture any stray charges; it should stay empty.
        try (TestRun run = new TestRun(false, received)) {
            run.processHit();
            assertThat("ScriptFieldsPhase must not charge bytes directly", received, is(empty()));
        }
    }

    // -------------------------------------------------------------------------

    private static final class TestRun implements AutoCloseable {
        final FetchSubPhaseProcessor processor;
        final LeafReaderContext leafReaderContext;
        final TestSearchContext searchContext;
        Object scriptPayload = buildPayload(50);
        boolean throwOnExecute = false;
        private SearchHit lastHit;

        TestRun(boolean ignoreException) throws Exception {
            this(ignoreException, new ArrayList<>());
        }

        TestRun(boolean ignoreException, List<Long> byteReceiver) throws Exception {
            ScriptFieldsContext scriptFieldsContext = new ScriptFieldsContext();
            scriptFieldsContext.add(new ScriptFieldsContext.ScriptField(FIELD_NAME, ctx -> new TestFieldScript(this), ignoreException));

            // TestSearchContext is used because FetchContext requires a SearchContext; we override
            // scriptFields() so ScriptFieldsPhase can read the configured script.
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
            FetchContext fetchContext = new FetchContext(searchContext, null);
            // Wire the inner-hits checker so any accidental chargeInnerHitsBytes calls would be captured.
            fetchContext.setInnerHitsByteChecker(byteReceiver::add);

            MemoryIndex index = new MemoryIndex();
            leafReaderContext = index.createSearcher().getIndexReader().leaves().get(0);

            processor = new ScriptFieldsPhase().getProcessor(fetchContext);
            assertNotNull(processor);
            processor.setNextReader(leafReaderContext);
        }

        void processHit() throws IOException {
            lastHit = SearchHit.unpooled(0, null);
            HitContext hitContext = new HitContext(lastHit, leafReaderContext, 0, Map.of(), Source.empty(null), null);
            processor.process(hitContext);
        }

        SearchHit lastHit() {
            return lastHit;
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

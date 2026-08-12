/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class FetchSubPhaseProcessorCloseIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "close-tracking-test";

    @Before
    public void resetCounters() {
        ClosableFetchSubPhasePlugin.PROCESS_COUNT.set(0);
        ClosableFetchSubPhasePlugin.CLOSE_COUNT.set(0);
        ClosableFetchSubPhasePlugin.FAIL_ON_PROCESS.set(false);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ClosableFetchSubPhasePlugin.class);
    }

    public void testProcessorClosedOnSuccessfulFetch() {
        createAndPopulateIndex();

        assertNoFailuresAndResponse(prepareSearch(INDEX_NAME), response -> assertEquals(1L, response.getHits().getTotalHits().value()));

        assertThat(ClosableFetchSubPhasePlugin.PROCESS_COUNT.get(), equalTo(1));
        assertThat(
            "close() must be called once the fetch phase completes successfully",
            ClosableFetchSubPhasePlugin.CLOSE_COUNT.get(),
            equalTo(1)
        );
    }

    public void testProcessorClosedWhenFetchFails() {
        createAndPopulateIndex();
        ClosableFetchSubPhasePlugin.FAIL_ON_PROCESS.set(true);

        expectThrows(SearchPhaseExecutionException.class, () -> prepareSearch(INDEX_NAME).get());

        assertThat(ClosableFetchSubPhasePlugin.PROCESS_COUNT.get(), equalTo(1));
        assertThat(
            "close() must still be called when the fetch phase fails, not just on success",
            ClosableFetchSubPhasePlugin.CLOSE_COUNT.get(),
            equalTo(1)
        );
    }

    private void createAndPopulateIndex() {
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        );
        prepareIndex(INDEX_NAME).setId("1").setSource("field", "value").get();
        refresh(INDEX_NAME);
    }

    public static class ClosableFetchSubPhasePlugin extends Plugin implements SearchPlugin {
        static final AtomicInteger PROCESS_COUNT = new AtomicInteger();
        static final AtomicInteger CLOSE_COUNT = new AtomicInteger();
        static final AtomicBoolean FAIL_ON_PROCESS = new AtomicBoolean(false);

        @Override
        public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
            return Collections.singletonList(fetchContext -> new FetchSubPhaseProcessor() {
                @Override
                public void setNextReader(LeafReaderContext readerContext) {}

                @Override
                public StoredFieldsSpec storedFieldsSpec() {
                    return StoredFieldsSpec.NO_REQUIREMENTS;
                }

                @Override
                public void process(FetchSubPhase.HitContext hitContext) throws IOException {
                    PROCESS_COUNT.incrementAndGet();
                    if (FAIL_ON_PROCESS.get()) {
                        throw new IOException("simulated fetch sub-phase failure");
                    }
                }

                @Override
                public void close() {
                    CLOSE_COUNT.incrementAndGet();
                }
            });
        }
    }
}

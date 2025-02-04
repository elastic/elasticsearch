/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.commits.HollowShardsService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.store.Store;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class HollowIndexEngineTests extends EngineTestCase {

    public void testReadOnly() throws IOException {
        IOUtils.close(engine, store);
        var globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            store.createEmpty();
            try (
                var hollowIndexEngine = new HollowIndexEngine(
                    config,
                    Mockito.mock(StatelessCommitService.class),
                    Mockito.mock(HollowShardsService.class)
                )
            ) {
                var exception = LuceneTestCase.TEST_ASSERTS_ENABLED ? AssertionError.class : UnsupportedOperationException.class;
                expectThrows(exception, () -> hollowIndexEngine.index(null));
                expectThrows(exception, () -> hollowIndexEngine.delete(null));
                expectThrows(exception, () -> hollowIndexEngine.noOp(null));
            }
        }
    }
}

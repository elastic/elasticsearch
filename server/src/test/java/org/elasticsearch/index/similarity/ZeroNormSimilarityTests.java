/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.similarity;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collection;
import java.util.List;

/**
 * Regression test: a similarity returning {@code 0} from {@code computeNorm} for a non-empty
 * field corrupts the segment and fails the shard on flush.
 *
 * <p>A similarity that returns {@code 0} from {@code computeNorm} for a non-empty field used
 * to throw an {@code IllegalStateException} inside {@code IndexingChain.PerField#finish}. That
 * exception escaped the {@code processDocument} finally-block before
 * {@code StoredFieldsConsumer.finishDocument} ran, leaving a stored-fields frame open and
 * desynchronising the writer's doc count. The segment then failed to flush with
 * {@code "Wrote N docs, finish called with numDocs=M"}, corrupting the shard.
 *
 * <p>After the fix, {@link NonZeroNormSimilarity} wraps every configured similarity and clamps a
 * zero {@code computeNorm} return to {@code 1}, so affected documents are accepted and the flush
 * completes normally.
 */
public class ZeroNormSimilarityTests extends ESSingleNodeTestCase {

    private static final String INDEX = "test";

    /**
     * A similarity that always returns {@code 0} from {@code computeNorm}, regardless of field
     * state. This mimics the customer scenario where an analysis chain produces only
     * overlap tokens ({@code positionIncrement == 0}), causing BM25 with
     * {@code discountOverlaps=true} to compute {@code numTerms = length - numOverlap = 0}.
     */
    static final class ZeroNormSimilarity extends BM25Similarity {
        @Override
        public long computeNorm(FieldInvertState state) {
            return 0L;
        }

        @Override
        public String toString() {
            return "ZeroNorm";
        }
    }

    public static final class ZeroNormSimPlugin extends Plugin {
        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSimilarity(
                "zero_norm",
                (Settings settings, IndexVersion version, ScriptService scriptService) -> new ZeroNormSimilarity()
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(ZeroNormSimPlugin.class);
    }

    /**
     * Before the fix, the sequence below resulted in a shard failure:
     * <ol>
     *   <li>Index a document whose {@code content} field produces at least one token, so
     *       {@code invertState.length > 0} and Lucene calls {@code computeNorm}.</li>
     *   <li>{@code computeNorm} returns {@code 0}; Lucene throws {@code IllegalStateException}.</li>
     *   <li>The exception exits the {@code processDocument} finally-block before
     *       {@code finishStoredFields} runs, leaving the stored-fields frame open.</li>
     *   <li>A subsequent flush sees {@code docBase != maxDoc} and throws
     *       {@code RuntimeException: "Wrote N docs, finish called with numDocs=M"}.</li>
     * </ol>
     * After the fix, step 2 returns {@code 1} instead of {@code 0}, no exception fires,
     * the document is accepted, and the flush succeeds.
     */
    public void testZeroNormDocumentIndexedAndFlushSucceeds() {
        indicesAdmin().prepareCreate(INDEX)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.refresh_interval", "-1")
                    .put("index.similarity.zero_norm.type", "zero_norm")
            )
            .setMapping("id", "type=keyword", "content", "type=text,similarity=zero_norm,norms=true")
            .get();
        ensureGreen(INDEX);

        // Index one document that will trigger computeNorm (content is non-empty) and one
        // that will not (no content field), matching the customer's two-document pattern.
        BulkResponse bulk = client().prepareBulk()
            .add(new IndexRequest(INDEX).id("1").source("{\"id\":\"1\",\"content\":\"hello\"}", XContentType.JSON))
            .add(new IndexRequest(INDEX).id("2").source("{\"id\":\"2\"}", XContentType.JSON))
            .get();

        assertFalse("all documents should index without failure: " + bulk.buildFailureMessage(), bulk.hasFailures());

        // Before the fix this threw RuntimeException: "Wrote 1 docs, finish called with numDocs=2"
        var flushResponse = indicesAdmin().flush(new FlushRequest(INDEX).force(true).waitIfOngoing(true)).actionGet();
        assertEquals("flush should not produce shard failures", 0, flushResponse.getFailedShards());
    }
}

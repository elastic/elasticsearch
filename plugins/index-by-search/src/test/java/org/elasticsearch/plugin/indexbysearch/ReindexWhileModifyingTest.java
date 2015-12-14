package org.elasticsearch.plugin.indexbysearch;

import static org.apache.lucene.util.TestUtil.randomSimpleString;
import static org.elasticsearch.plugin.indexbysearch.ReindexInPlaceRequest.ReindexVersionType.INFER;
import static org.elasticsearch.plugin.indexbysearch.ReindexInPlaceRequest.ReindexVersionType.INTERNAL;
import static org.elasticsearch.plugin.indexbysearch.ReindexInPlaceRequest.ReindexVersionType.REINDEX;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.plugin.indexbysearch.ReindexInPlaceRequest.ReindexVersionType;

/**
 * Mutates a document while reindexing it and asserts that the mutation always
 * sticks. Reindex should never revert.
 */

public class ReindexWhileModifyingTest extends ReindexTestCase {
    private static final int MAX_MUTATIONS = 50;

    public void testUpdateWhileReindexingUsingReindexVersionType() throws Exception {
        updateWhileReindexingTestCase(REINDEX);
    }

    public void testUpdateWhileReindexingUsingInternalVersionType() throws Exception {
        updateWhileReindexingTestCase(INTERNAL);
    }

    public void testUpdateWhileReindexingUsingInferVersionType() throws Exception {
        updateWhileReindexingTestCase(INFER);
    }

    private void updateWhileReindexingTestCase(ReindexVersionType versionType) throws Exception {
        AtomicReference<String> value = new AtomicReference<>(randomSimpleString(random()));
        indexRandom(true, client().prepareIndex("test", "test", "test").setSource("test", value.get()));

        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean keepReindexing = new AtomicBoolean(true);
        Thread reindexer = new Thread(new Runnable() {
            @Override
            public void run() {
                while (keepReindexing.get()) {
                    try {
                        ReindexInPlaceRequestBuilder reindex = reindex("test");
                        reindex.versionType(versionType);
                        assertThat(reindex.get(), responseMatcher().updated(either(equalTo(0L)).or(equalTo(1L)))
                                .versionConflicts(either(equalTo(0L)).or(equalTo(1L))));
                        client().admin().indices().prepareRefresh("test").get();
                    } catch (Throwable t) {
                        failure.set(t);
                    }
                }
            }
        });
        reindexer.start();

        try {
            for (int i = 0; i < MAX_MUTATIONS; i++) {
                GetResponse get = client().prepareGet("test", "test", "test").get();
                assertEquals(value.get(), get.getSource().get("test"));
                value.set(randomSimpleString(random()));
                IndexRequestBuilder index = client().prepareIndex("test", "test", "test").setSource("test", value.get())
                        .setRefresh(true);
                if (versionType == INTERNAL) {
                    /*
                     * Internal versioning on the reindex increments the version
                     * number concurrent indexes might get version conflict
                     * exceptions. So we just retry until it goes through.
                     *
                     * The infer version type doesn't do that because we aren't
                     * setting a script on the reindex.
                     */
                    while (true) {
                        try {
                            index.setVersion(get.getVersion()).get();
                            break;
                        } catch (VersionConflictEngineException e) {
                            logger.info(
                                    "Caught expected version conflict trying to perform mutation number {} with version {}. Retrying.",
                                    i, get.getVersion());
                            get = client().prepareGet("test", "test", "test").get();
                            continue;
                        }
                    }
                } else {
                    index.setVersion(get.getVersion()).get();
                }
            }
        } finally {
            keepReindexing.set(false);
            reindexer.join(TimeUnit.SECONDS.toMillis(10));
            if (failure.get() != null) {
                throw new RuntimeException(failure.get());
            }
        }
    }

}

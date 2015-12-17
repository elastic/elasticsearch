package org.elasticsearch.plugin.reindex;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

import org.elasticsearch.plugin.reindex.ReindexInPlaceRequestBuilder;
import org.elasticsearch.plugin.reindex.ReindexInPlaceRequest.ReindexVersionType;

import java.util.function.Supplier;

public class ReindexBasicTests extends ReindexTestCase {
    public void testReindexVersionType() throws Exception {
        basicTestCase(true);
    }

    public void testInternalVersionType() throws Exception {
        basicTestCase(false);
    }

    private void basicTestCase(boolean reindexVersionType) throws Exception {
        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("foo", "a"),
                client().prepareIndex("test", "test", "2").setSource("foo", "a"),
                client().prepareIndex("test", "test", "3").setSource("foo", "b"),
                client().prepareIndex("test", "test", "4").setSource("foo", "c"));
        assertHitCount(client().prepareSearch("test").setTypes("test").setSize(0).get(), 4);
        assertEquals(1, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(1, client().prepareGet("test", "test", "4").get().getVersion());

        Supplier<ReindexInPlaceRequestBuilder> reindex = () -> {
            ReindexInPlaceRequestBuilder request = reindex().source("test");
            if (reindexVersionType) {
                if (rarely()) {
                    request.versionType(ReindexVersionType.REINDEX);
                }
            } else {
                request.versionType(ReindexVersionType.INTERNAL);
            }
            return request;
        };

        // Reindex all the docs
        assertThat(reindex.get().get(), responseMatcher().updated(4));
        assertEquals(reindexVersionType ? 1 : 2, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(reindexVersionType ? 1 : 2, client().prepareGet("test", "test", "4").get().getVersion());

        // Now none of them
        refresh();
        assertThat(reindex.get().filter(termQuery("foo", "no_match")).get(), responseMatcher().updated(0));
        assertEquals(reindexVersionType ? 1 : 2, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(reindexVersionType ? 1 : 2, client().prepareGet("test", "test", "4").get().getVersion());

        // Now half of them
        refresh();
        assertThat(reindex.get().filter(termQuery("foo", "a")).get(), responseMatcher().updated(2));
        refresh();
        assertEquals(reindexVersionType ? 1 : 3, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(reindexVersionType ? 1 : 2, client().prepareGet("test", "test", "4").get().getVersion());

        // Limit with size
        refresh();
        assertThat(reindex.get().size(1).get(), responseMatcher().updated(1));
        // We can't assert anything about versions here because we don't know which one was updated
    }

    // NOCOMMIT - add a test for abortOnVersionConflict
}

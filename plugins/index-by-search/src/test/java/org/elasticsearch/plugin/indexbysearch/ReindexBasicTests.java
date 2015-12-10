package org.elasticsearch.plugin.indexbysearch;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

import java.util.function.Supplier;

public class ReindexBasicTests extends ReindexTestCase {
    public void testReindexVersionType() throws Exception {
        basicTestCase(true);
    }

    public void testInternalVersionType() throws Exception {
        basicTestCase(false);
    }

    private void basicTestCase(boolean reindexOpType) throws Exception {
        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("foo", "a"),
                client().prepareIndex("test", "test", "2").setSource("foo", "a"),
                client().prepareIndex("test", "test", "3").setSource("foo", "b"),
                client().prepareIndex("test", "test", "4").setSource("foo", "c"));
        assertHitCount(client().prepareSearch("test").setTypes("test").setSize(0).get(), 4);
        assertEquals(1, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(1, client().prepareGet("test", "test", "4").get().getVersion());

        Supplier<ReindexInPlaceRequestBuilder> reindexTest = () -> {
            ReindexInPlaceRequestBuilder reindex = reindex("test");
            if (reindexOpType) {
                if (rarely()) {
                    reindex.useReindexVersionType(true);
                }
            } else {
                reindex.useReindexVersionType(false);
            }
            return reindex;
        };

        // Reindex all the docs
        ReindexInPlaceRequestBuilder reindex = reindexTest.get();
        assertThat(reindex.get(), responseMatcher().updated(4));
        assertEquals(reindexOpType ? 1 : 2, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(reindexOpType ? 1 : 2, client().prepareGet("test", "test", "4").get().getVersion());

        // Now none of them
        refresh();
        reindex = reindexTest.get();
        reindex.search().setQuery(termQuery("foo", "no_match"));
        assertThat(reindex.get(), responseMatcher().updated(0));
        assertEquals(reindexOpType ? 1 : 2, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(reindexOpType ? 1 : 2, client().prepareGet("test", "test", "4").get().getVersion());

        // Now half of them
        refresh();
        reindex = reindexTest.get();
        reindex.search().setQuery(termQuery("foo", "a"));
        assertThat(reindex.get(), responseMatcher().updated(2));
        refresh();
        assertEquals(reindexOpType ? 1 : 3, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(reindexOpType ? 1 : 2, client().prepareGet("test", "test", "4").get().getVersion());

        // Limit with size
        refresh();
        reindex = reindexTest.get();
        reindex.size(1);
        assertThat(reindex.get(), responseMatcher().updated(1));
        // We can't assert anything about versions here because we don't know which one was updated
    }

}

package org.elasticsearch.plugin.reindex;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class UpdateByQueryBasicTests extends UpdateByQueryTestCase {
    public void testBasics() throws Exception {
        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("foo", "a"),
                client().prepareIndex("test", "test", "2").setSource("foo", "a"),
                client().prepareIndex("test", "test", "3").setSource("foo", "b"),
                client().prepareIndex("test", "test", "4").setSource("foo", "c"));
        assertHitCount(client().prepareSearch("test").setTypes("test").setSize(0).get(), 4);
        assertEquals(1, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(1, client().prepareGet("test", "test", "4").get().getVersion());

        // Reindex all the docs
        assertThat(request().source("test").get(), responseMatcher().updated(4));
        assertEquals(2, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(2, client().prepareGet("test", "test", "4").get().getVersion());

        // Now none of them
        refresh();
        assertThat(request().source("test").filter(termQuery("foo", "no_match")).get(), responseMatcher().updated(0));
        assertEquals(2, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(2, client().prepareGet("test", "test", "4").get().getVersion());

        // Now half of them
        refresh();
        assertThat(request().source("test").filter(termQuery("foo", "a")).get(), responseMatcher().updated(2));
        refresh();
        assertEquals(3, client().prepareGet("test", "test", "1").get().getVersion());
        assertEquals(2, client().prepareGet("test", "test", "4").get().getVersion());

        // Limit with size
        refresh();
        assertThat(request().source("test").size(1).get(), responseMatcher().updated(1));
        // We can't assert anything about versions here because we don't know which one was updated
    }

    // NOCOMMIT - add a test for abortOnVersionConflict
}

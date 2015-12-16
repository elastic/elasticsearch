package org.elasticsearch.plugin.indexbysearch;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.plugin.indexbysearch.ReindexInPlaceRequest.ReindexVersionType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;

public class ReindexUpdateByQueryTests extends ReindexTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(SetBarScript.RegistrationPlugin.class);
        return plugins;
    }

    public void testBasicUpdateByQuery() throws Exception {
        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("foo", "a"));

        ReindexInPlaceRequestBuilder update = reindex().source("test")
                .script(new Script("set-bar", ScriptType.INLINE, "native", singletonMap("to", "cat")));
        assertThat(update.get(), responseMatcher().updated(1));
        refresh();

        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("bar", "cat")).get(), "1");
    }

    public void testReindexVersionType() throws Exception {
        updateByQueryVersionTypeTestCase(true);
    }

    public void testInternalVersionType() throws Exception {
        updateByQueryVersionTypeTestCase(false);
    }

    public void updateByQueryVersionTypeTestCase(boolean reindexVersionType) throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings("index.refresh_interval", -1).get());
        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("foo", "a"));

        /*
         * Index the doc again without refreshing so the search is pointed at
         * the old doc.
         */
        indexRandom(false, client().prepareIndex("test", "test", "1").setSource("foo", "b"));

        ReindexInPlaceRequestBuilder update = reindex().source("test")
                .script(new Script("set-bar", ScriptType.INLINE, "native", singletonMap("to", "cat")));
        if (reindexVersionType) {
            update.versionType(ReindexVersionType.REINDEX);
        } else {
            // Default is false if script is there
            if (random().nextBoolean()) {
                update.versionType(ReindexVersionType.INTERNAL);
            }
        }

        // All version_types get a conflict here
        assertThat(update.get(), responseMatcher().versionConflicts(1));

        // Lets try again, this time with a refresh first
        refresh();
        assertThat(update.get(), responseMatcher().updated(1));
        refresh();
        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("bar", "cat")).get(), "1");
        assertEquals(reindexVersionType ? 2 : 3, client().prepareGet("test", "test", "1").get().getVersion());
    }
}

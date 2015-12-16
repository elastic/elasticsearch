package org.elasticsearch.plugin.indexbysearch;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;

/**
 * Tests index-by-search with a script modifying the documents.
 */
public class IndexBySearchTransformedTests extends IndexBySearchTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(SetBarScript.RegistrationPlugin.class);
        return plugins;
    }

    public void testTransformed() throws Exception {
        indexRandom(true, client().prepareIndex("src", "test", "1").setSource("foo", "a"));

        IndexBySearchRequestBuilder copy = newIndexBySearch().source("src").destination("dest")
                .script(new Script("set-bar", ScriptType.INLINE, "native", singletonMap("to", "cat")));
        assertThat(copy.get(), responseMatcher().created(1));
        refresh();

        assertSearchHits(client().prepareSearch("dest").setQuery(matchQuery("bar", "cat")).get(), "1");
    }
}
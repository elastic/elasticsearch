package org.elasticsearch.plugin.reindex;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;

public class UpdateByQueryWithQueryTests extends UpdateByQueryTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(SetBarScript.RegistrationPlugin.class);
        return plugins;
    }

    public void testBasicUpdateByQuery() throws Exception {
        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("foo", "a"));

        UpdateByQueryRequestBuilder update = request().source("test")
                .script(new Script("set-bar", ScriptType.INLINE, "native", singletonMap("to", "cat")));
        assertThat(update.get(), responseMatcher().updated(1));
        refresh();

        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("bar", "cat")).get(), "1");
    }
}

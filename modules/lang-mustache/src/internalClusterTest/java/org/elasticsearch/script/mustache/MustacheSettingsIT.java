/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.script.mustache;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class MustacheSettingsIT extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(MustachePlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(MustacheScriptEngine.MUSTACHE_RESULT_SIZE_LIMIT.getKey(), "10b").build();
    }

    public void testResultSizeLimit() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "type", "1").setSource(jsonBuilder().startObject().field("text", "value1").endObject()).get();
        client().admin().indices().prepareRefresh().get();

        String query = "{ \"query\": {\"match_all\": {}}, \"size\" : \"{{my_size}}\"  }";
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("test");
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> new SearchTemplateRequestBuilder(client()).setRequest(searchRequest)
                .setScript(query)
                .setScriptType(ScriptType.INLINE)
                .setScriptParams(Collections.singletonMap("my_size", 1))
                .get()
        );
        assertThat(e.getMessage(), equalTo("Mustache script result size limit exceeded"));
    }
}

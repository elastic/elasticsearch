/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;

//TODO: please convert to unit tests!
public class StoredExpressionIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        builder.put("script.allowed_contexts", "update");
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(ExpressionPlugin.class);
    }

    public void testAllOpsDisabledIndexedScripts() throws IOException {
        client().admin().cluster().preparePutStoredScript()
                .setId("script1")
                .setContent(new BytesArray("{\"script\": {\"lang\": \"expression\", \"source\": \"2\"} }"), XContentType.JSON)
                .get();
        client().prepareIndex("test").setId("1").setSource("{\"theField\":\"foo\"}", XContentType.JSON).get();
        try {
            client().prepareUpdate("test", "1")
                    .setScript(new Script(ScriptType.STORED, null, "script1", Collections.emptyMap())).get();
            fail("update script should have been rejected");
        } catch(Exception e) {
            assertThat(e.getMessage(), containsString("failed to execute script"));
            assertThat(e.getCause().getMessage(), containsString("Failed to compile stored script [script1] using lang [expression]"));
        }
        try {
            client().prepareSearch()
                    .setSource(new SearchSourceBuilder().scriptField("test1",
                            new Script(ScriptType.STORED, null, "script1", Collections.emptyMap())))
                    .setIndices("test").get();
            fail("search script should have been rejected");
        } catch(Exception e) {
            assertThat(e.toString(), containsString("cannot execute scripts using [field] context"));
        }
        try {
            client().prepareSearch("test")
                    .setSource(
                            new SearchSourceBuilder().aggregation(AggregationBuilders.terms("test").script(
                                    new Script(ScriptType.STORED, null, "script1", Collections.emptyMap())))).get();
        } catch (Exception e) {
            assertThat(e.toString(), containsString("cannot execute scripts using [aggs] context"));
        }
    }
}

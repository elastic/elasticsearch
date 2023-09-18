/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class EsqlTimeoutIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() throws IOException {

        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("the_field");
            {
                mapping.field("type", "long");
                mapping.startObject("script").field("source", "").field("lang", "sleep").endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        client().admin().indices().prepareCreate("test").setSettings(Map.of("number_of_shards", 1)).setMapping(mapping.endObject()).get();

        client().prepareBulk()
            .add(client().prepareIndex("test").setId("0").setSource("foo", 0))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), SleepFieldPlugin.class);
    }

    public void testTimeout() {
        ElasticsearchTimeoutException re = expectThrows(ElasticsearchTimeoutException.class, () -> {
            EsqlQueryRequest request = new EsqlQueryRequest();
            request.query("from test");
            request.timeout(new TimeValue(500, TimeUnit.MILLISECONDS));
            client().execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
        });
        assertThat(re.getMessage(), containsString("ESQL query timed out after 500ms"));
    }

    public void testNoTimeout() {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query("from test");
        request.timeout(new TimeValue(1500, TimeUnit.MILLISECONDS));
        EsqlQueryResponse a = client().execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
        assertThat(a.columns().size(), equalTo(2));
    }

    public static class SleepFieldPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override
                public String getType() {
                    return "sleep";
                }

                @Override
                @SuppressWarnings("unchecked")
                public <FactoryType> FactoryType compile(
                    String name,
                    String code,
                    ScriptContext<FactoryType> context,
                    Map<String, String> params
                ) {
                    return (FactoryType) (LongFieldScript.Factory) (
                        fieldName,
                        params1,
                        searchLookup,
                        onScriptError) -> ctx -> new LongFieldScript(fieldName, params1, searchLookup, onScriptError, ctx) {
                            @Override
                            public void execute() {
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                emit(34);
                            }
                        };
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(LongFieldScript.CONTEXT);
                }
            };
        }
    }
}

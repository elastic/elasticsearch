/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * Make sure the failures on the data node come back as failures over the wire.
 */
@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class EsqlNodeFailureIT extends AbstractEsqlIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), FailingFieldPlugin.class);
    }

    /**
     * Use a runtime field that fails when loading field values to fail the entire query.
     */
    public void testFailureLoadingFields() throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("fail_me");
            {
                mapping.field("type", "long");
                mapping.startObject("script").field("source", "").field("lang", "fail").endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        client().admin().indices().prepareCreate("fail").setSettings(indexSettings(1, 0)).setMapping(mapping.endObject()).get();

        int docCount = 100;
        List<IndexRequestBuilder> docs = new ArrayList<>(docCount);
        for (int d = 0; d < docCount; d++) {
            docs.add(client().prepareIndex("ok").setSource("foo", d));
        }
        docs.add(client().prepareIndex("fail").setSource("foo", 0));
        indexRandom(true, docs);

        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> run("FROM fail,ok | LIMIT 100").close());
        assertThat(e.getMessage(), equalTo("test failure"));
    }

    public static class FailingFieldPlugin extends Plugin implements ScriptPlugin {

        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override
                public String getType() {
                    return "fail";
                }

                @Override
                @SuppressWarnings("unchecked")
                public <FactoryType> FactoryType compile(
                    String name,
                    String code,
                    ScriptContext<FactoryType> context,
                    Map<String, String> params
                ) {
                    return (FactoryType) new LongFieldScript.Factory() {
                        @Override
                        public LongFieldScript.LeafFactory newFactory(
                            String fieldName,
                            Map<String, Object> params,
                            SearchLookup searchLookup,
                            OnScriptError onScriptError
                        ) {
                            return ctx -> new LongFieldScript(fieldName, params, searchLookup, onScriptError, ctx) {
                                @Override
                                public void execute() {
                                    throw new ElasticsearchException("test failure");
                                }
                            };
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

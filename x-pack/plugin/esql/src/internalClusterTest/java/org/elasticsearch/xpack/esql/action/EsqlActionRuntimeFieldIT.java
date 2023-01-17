/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.equalTo;

/**
 * Makes sure that the circuit breaker is "plugged in" to ESQL by configuring an
 * unreasonably small breaker and tripping it.
 */
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false) // ESQL is single node
public class EsqlActionRuntimeFieldIT extends ESIntegTestCase {
    private static final int SIZE = 5000;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(EsqlPlugin.class, TestRuntimeFieldPlugin.class);
    }

    public void testLong() throws InterruptedException, IOException {
        createIndexWithConstRuntimeField("long");
        EsqlQueryResponse response = EsqlActionIT.run("from test | stats sum(const)", Settings.EMPTY);
        assertThat(response.values(), equalTo(List.of(List.of((long) SIZE))));
    }

    public void testDouble() throws InterruptedException, IOException {
        createIndexWithConstRuntimeField("double");
        EsqlQueryResponse response = EsqlActionIT.run("from test | stats sum(const)", Settings.EMPTY);
        assertThat(response.values(), equalTo(List.of(List.of((double) SIZE))));
    }

    public void testKeyword() throws InterruptedException, IOException {
        createIndexWithConstRuntimeField("keyword");
        EsqlQueryResponse response = EsqlActionIT.run("from test | project const | limit 1", Settings.EMPTY);
        assertThat(response.values(), equalTo(List.of(List.of("const"))));
    }

    /**
     * Test grouping by runtime keyword which requires disabling the ordinals
     * optimization available to more keyword fields.
     */
    public void testKeywordBy() throws InterruptedException, IOException {
        createIndexWithConstRuntimeField("keyword");
        EsqlQueryResponse response = EsqlActionIT.run("from test | stats max(foo) by const", Settings.EMPTY);
        assertThat(response.values(), equalTo(List.of(List.of(SIZE - 1L, "const"))));
    }

    private void createIndexWithConstRuntimeField(String type) throws InterruptedException, IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("const");
            {
                mapping.field("type", type);
                mapping.startObject("script").field("source", "").field("lang", "dummy").endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        client().admin().indices().prepareCreate("test").setMapping(mapping.endObject()).get();

        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            indexRequests.add(client().prepareIndex("test").setId(Integer.toString(i)).setSource("foo", i));
        }
        indexRandom(true, indexRequests);
    }

    public static class TestRuntimeFieldPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override
                public String getType() {
                    return "dummy";
                }

                @Override
                @SuppressWarnings("unchecked")
                public <FactoryType> FactoryType compile(
                    String name,
                    String code,
                    ScriptContext<FactoryType> context,
                    Map<String, String> params
                ) {
                    if (context == LongFieldScript.CONTEXT) {
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
                                        emit(1);
                                    }
                                };
                            }
                        };
                    }
                    if (context == DoubleFieldScript.CONTEXT) {
                        return (FactoryType) new DoubleFieldScript.Factory() {
                            @Override
                            public DoubleFieldScript.LeafFactory newFactory(
                                String fieldName,
                                Map<String, Object> params,
                                SearchLookup searchLookup,
                                OnScriptError onScriptError
                            ) {
                                return ctx -> new DoubleFieldScript(fieldName, params, searchLookup, onScriptError, ctx) {
                                    @Override
                                    public void execute() {
                                        emit(1.0);
                                    }
                                };
                            }
                        };
                    }
                    if (context == StringFieldScript.CONTEXT) {
                        return (FactoryType) new StringFieldScript.Factory() {
                            @Override
                            public StringFieldScript.LeafFactory newFactory(
                                String fieldName,
                                Map<String, Object> params,
                                SearchLookup searchLookup,
                                OnScriptError onScriptError
                            ) {
                                return ctx -> new StringFieldScript(fieldName, params, searchLookup, onScriptError, ctx) {
                                    @Override
                                    public void execute() {
                                        emit("const");
                                    }
                                };
                            }
                        };
                    }
                    throw new IllegalArgumentException("unsupported context " + context);
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(LongFieldScript.CONTEXT);
                }
            };
        }
    }

}

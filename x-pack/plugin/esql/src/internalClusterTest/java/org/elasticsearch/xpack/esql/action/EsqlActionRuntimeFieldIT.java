/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.BooleanFieldScript;
import org.elasticsearch.script.DateFieldScript;
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
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
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class EsqlActionRuntimeFieldIT extends AbstractEsqlIntegTestCase {
    private final int SIZE = between(10, 100);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestRuntimeFieldPlugin.class);
    }

    public void testLong() throws InterruptedException, IOException {
        createIndexWithConstRuntimeField("long");
        EsqlQueryResponse response = run("from test | stats sum(const)");
        assertThat(response.values(), equalTo(List.of(List.of((long) SIZE))));
    }

    public void testDouble() throws InterruptedException, IOException {
        createIndexWithConstRuntimeField("double");
        EsqlQueryResponse response = run("from test | stats sum(const)");
        assertThat(response.values(), equalTo(List.of(List.of((double) SIZE))));
    }

    public void testKeyword() throws InterruptedException, IOException {
        createIndexWithConstRuntimeField("keyword");
        EsqlQueryResponse response = run("from test | keep const | limit 1");
        assertThat(response.values(), equalTo(List.of(List.of("const"))));
    }

    /**
     * Test grouping by runtime keyword which requires disabling the ordinals
     * optimization available to more keyword fields.
     */
    public void testKeywordBy() throws InterruptedException, IOException {
        createIndexWithConstRuntimeField("keyword");
        EsqlQueryResponse response = run("from test | stats max(foo) by const");
        assertThat(response.values(), equalTo(List.of(List.of(SIZE - 1L, "const"))));
    }

    public void testBoolean() throws InterruptedException, IOException {
        createIndexWithConstRuntimeField("boolean");
        EsqlQueryResponse response = run("from test | sort foo | limit 3");
        assertThat(response.values(), equalTo(List.of(List.of(true, 0L), List.of(true, 1L), List.of(true, 2L))));
    }

    public void testDate() throws InterruptedException, IOException {
        createIndexWithConstRuntimeField("date");
        EsqlQueryResponse response = run("""
            from test | eval d=date_format(const, "yyyy") | stats min (foo) by d""");
        assertThat(response.values(), equalTo(List.of(List.of(0L, "2023"))));
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

        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < SIZE; i++) {
            bulk.add(client().prepareIndex("test").setId(Integer.toString(i)).setSource("foo", i));
        }
        bulk.get();
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
                    if (context == BooleanFieldScript.CONTEXT) {
                        return (FactoryType) new BooleanFieldScript.Factory() {
                            @Override
                            public BooleanFieldScript.LeafFactory newFactory(
                                String fieldName,
                                Map<String, Object> params,
                                SearchLookup searchLookup,
                                OnScriptError onScriptError
                            ) {
                                return ctx -> new BooleanFieldScript(fieldName, params, searchLookup, onScriptError, ctx) {
                                    @Override
                                    public void execute() {
                                        emit(true);
                                    }
                                };
                            }
                        };
                    }
                    if (context == DateFieldScript.CONTEXT) {
                        return (FactoryType) new DateFieldScript.Factory() {
                            @Override
                            public DateFieldScript.LeafFactory newFactory(
                                String fieldName,
                                Map<String, Object> params,
                                SearchLookup searchLookup,
                                DateFormatter dateFormatter,
                                OnScriptError onScriptError
                            ) {
                                return ctx -> new DateFieldScript(fieldName, params, searchLookup, dateFormatter, onScriptError, ctx) {
                                    @Override
                                    public void execute() {
                                        emit(dateFormatter.parseMillis("2023-01-01T00:00:00Z"));
                                    }
                                };
                            }
                        };
                    }
                    throw new IllegalArgumentException("unsupported context " + context);
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(
                        LongFieldScript.CONTEXT,
                        DoubleFieldScript.CONTEXT,
                        StringFieldScript.CONTEXT,
                        BooleanFieldScript.CONTEXT,
                        DateFieldScript.CONTEXT
                    );
                }
            };
        }
    }

}

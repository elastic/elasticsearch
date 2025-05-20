/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.index.query.IntervalFilterScript;
import org.elasticsearch.index.similarity.ScriptedSimilarity.Doc;
import org.elasticsearch.index.similarity.ScriptedSimilarity.Field;
import org.elasticsearch.index.similarity.ScriptedSimilarity.Query;
import org.elasticsearch.index.similarity.ScriptedSimilarity.Term;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.emptyMap;

/**
 * A mocked script engine that can be used for testing purpose.
 *
 * This script engine allows to define a set of predefined scripts that basically a combination of a key and a
 * function:
 *
 * The key can be anything as long as it is a {@link String} and is used to resolve the scripts
 * at compilation time. For inline scripts, the key can be a description of the script. For stored and file scripts,
 * the source must match a key in the predefined set of scripts.
 *
 * The function is used to provide the result of the script execution and can return anything.
 */
public class MockScriptEngine implements ScriptEngine {

    /** A non-typed compiler for a single custom context */
    public interface ContextCompiler {
        Object compile(Function<Map<String, Object>, Object> script, Map<String, String> params);
    }

    public static final String NAME = "mockscript";

    private final String type;
    private final Map<String, MockDeterministicScript> scripts;
    private final Map<ScriptContext<?>, ContextCompiler> contexts;

    public MockScriptEngine(
        String type,
        Map<String, Function<Map<String, Object>, Object>> scripts,
        Map<ScriptContext<?>, ContextCompiler> contexts
    ) {
        this(type, scripts, Collections.emptyMap(), contexts);
    }

    public MockScriptEngine(
        String type,
        Map<String, Function<Map<String, Object>, Object>> deterministicScripts,
        Map<String, Function<Map<String, Object>, Object>> nonDeterministicScripts,
        Map<ScriptContext<?>, ContextCompiler> contexts
    ) {

        Map<String, MockDeterministicScript> scriptMap = Maps.newMapWithExpectedSize(
            deterministicScripts.size() + nonDeterministicScripts.size()
        );
        deterministicScripts.forEach((key, value) -> scriptMap.put(key, MockDeterministicScript.asDeterministic(value)));
        nonDeterministicScripts.forEach((key, value) -> scriptMap.put(key, MockDeterministicScript.asNonDeterministic(value)));

        this.type = type;
        this.scripts = Collections.unmodifiableMap(scriptMap);
        this.contexts = Collections.unmodifiableMap(contexts);
    }

    public MockScriptEngine() {
        this(NAME, Collections.emptyMap(), Collections.emptyMap());
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public <T> T compile(String name, String source, ScriptContext<T> context, Map<String, String> params) {
        // Scripts are always resolved using the script's source. For inline scripts, it's easy because they don't have names and the
        // source is always provided. For stored and file scripts, the source of the script must match the key of a predefined script.
        MockDeterministicScript script = scripts.get(source);
        if (script == null) {
            throw new IllegalArgumentException(
                "No pre defined script matching ["
                    + source
                    + "] for script with name ["
                    + name
                    + "], "
                    + "did you declare the mocked script?"
            );
        }
        MockCompiledScript mockCompiled = new MockCompiledScript(name, params, source, script);
        if (context.instanceClazz.equals(FieldScript.class)) {
            return context.factoryClazz.cast(new MockFieldScriptFactory(script));
        } else if (context.instanceClazz.equals(TermsSetQueryScript.class)) {
            TermsSetQueryScript.Factory factory = (parameters, lookup) -> (TermsSetQueryScript.LeafFactory) ctx -> new TermsSetQueryScript(
                parameters,
                lookup,
                ctx
            ) {
                @Override
                public Number execute() {
                    Map<String, Object> vars = new HashMap<>(parameters);
                    vars.put("params", parameters);
                    vars.put("doc", getDoc());
                    return (Number) script.apply(vars);
                }
            };
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(NumberSortScript.class)) {
            NumberSortScript.Factory factory = (parameters, lookup) -> new NumberSortScript.LeafFactory() {
                @Override
                public NumberSortScript newInstance(DocReader reader) {
                    return new NumberSortScript(parameters, lookup, reader) {
                        @Override
                        public double execute() {
                            Map<String, Object> vars = new HashMap<>(parameters);
                            vars.put("params", parameters);
                            vars.put("doc", getDoc());
                            try {
                                vars.put("_score", get_score());
                            } catch (Exception ignore) {
                                // nothing to do: if get_score throws we don't set the _score, likely the scorer is null,
                                // which is ok if _score was not requested e.g. top_hits.
                            }
                            return ((Number) script.apply(vars)).doubleValue();
                        }
                    };
                }

                @Override
                public boolean needs_score() {
                    return false;
                }
            };
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(StringSortScript.class)) {
            return context.factoryClazz.cast(new MockStringSortScriptFactory(script));
        } else if (context.instanceClazz.equals(BytesRefSortScript.class)) {
            return context.factoryClazz.cast(new MockBytesRefSortScriptFactory(script));
        } else if (context.instanceClazz.equals(IngestScript.class)) {
            IngestScript.Factory factory = (parameters, ctx) -> new IngestScript(parameters, ctx) {
                @Override
                public void execute() {
                    script.apply(ctx);
                }
            };
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(AggregationScript.class)) {
            return context.factoryClazz.cast(new MockAggregationScript(script));
        } else if (context.instanceClazz.equals(IngestConditionalScript.class)) {
            IngestConditionalScript.Factory factory = parameters -> new IngestConditionalScript(parameters) {
                @Override
                public boolean execute(Map<String, Object> ctx) {
                    return (boolean) script.apply(ctx);
                }
            };
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(UpdateScript.class)) {
            UpdateScript.Factory factory = (parameters, ctx) -> new UpdateScript(parameters, ctx) {
                @Override
                public void execute() {
                    final Map<String, Object> vars = new HashMap<>();
                    vars.put("ctx", ctx);
                    vars.put("params", parameters);
                    vars.putAll(parameters);
                    script.apply(vars);
                }
            };
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(ReindexScript.class)) {
            ReindexScript.Factory factory = (parameters, ctx) -> new ReindexScript(parameters, ctx) {
                @Override
                public void execute() {
                    final Map<String, Object> vars = new HashMap<>();
                    vars.put("ctx", ctx);
                    vars.put("params", parameters);
                    vars.putAll(parameters);
                    script.apply(vars);
                }
            };
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(UpdateByQueryScript.class)) {
            UpdateByQueryScript.Factory factory = (parameters, ctx) -> new UpdateByQueryScript(parameters, ctx) {
                @Override
                public void execute() {
                    final Map<String, Object> vars = new HashMap<>();
                    vars.put("ctx", ctx);
                    vars.put("params", parameters);
                    vars.putAll(parameters);
                    script.apply(vars);
                }
            };
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(BucketAggregationScript.class)) {
            BucketAggregationScript.Factory factory = parameters -> new BucketAggregationScript(parameters) {
                @Override
                public Double execute() {
                    Object ret = script.apply(getParams());
                    if (ret == null) {
                        return null;
                    } else {
                        return ((Number) ret).doubleValue();
                    }
                }
            };
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(BucketAggregationSelectorScript.class)) {
            BucketAggregationSelectorScript.Factory factory = parameters -> new BucketAggregationSelectorScript(parameters) {
                @Override
                public boolean execute() {
                    return (boolean) script.apply(getParams());
                }
            };
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(SignificantTermsHeuristicScoreScript.class)) {
            return context.factoryClazz.cast(new MockSignificantTermsHeuristicScoreScript(script));
        } else if (context.instanceClazz.equals(TemplateScript.class)) {
            TemplateScript.Factory factory = vars -> {
                Map<String, Object> varsWithParams = new HashMap<>();
                if (vars != null) {
                    varsWithParams.put("params", vars);
                }
                return new TemplateScript(vars) {
                    @Override
                    public String execute() {
                        return (String) script.apply(varsWithParams);
                    }
                };
            };
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(FilterScript.class)) {
            FilterScript.Factory factory = mockCompiled::createFilterScript;
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(SimilarityScript.class)) {
            SimilarityScript.Factory factory = mockCompiled::createSimilarityScript;
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(SimilarityWeightScript.class)) {
            SimilarityWeightScript.Factory factory = mockCompiled::createSimilarityWeightScript;
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(ScoreScript.class)) {
            ScoreScript.Factory factory = new MockScoreScript(script);
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(ScriptedMetricAggContexts.InitScript.class)) {
            ScriptedMetricAggContexts.InitScript.Factory factory = new MockMetricAggInitScriptFactory(script);
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(ScriptedMetricAggContexts.MapScript.class)) {
            ScriptedMetricAggContexts.MapScript.Factory factory = new MockMetricAggMapScriptFactory(script);
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(ScriptedMetricAggContexts.CombineScript.class)) {
            ScriptedMetricAggContexts.CombineScript.Factory factory = new MockMetricAggCombineScriptFactory(script);
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(ScriptedMetricAggContexts.ReduceScript.class)) {
            ScriptedMetricAggContexts.ReduceScript.Factory factory = new MockMetricAggReduceScriptFactory(script);
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(IntervalFilterScript.class)) {
            IntervalFilterScript.Factory factory = mockCompiled::createIntervalFilterScript;
            return context.factoryClazz.cast(factory);
        } else if (context.instanceClazz.equals(BooleanFieldScript.class)) {
            BooleanFieldScript.Factory booleanFieldScript = (f, p, s, onScriptError) -> ctx -> new BooleanFieldScript(
                f,
                p,
                s,
                OnScriptError.FAIL,
                ctx
            ) {
                @Override
                public void execute() {
                    emit(true);
                }
            };
            return context.factoryClazz.cast(booleanFieldScript);
        } else if (context.instanceClazz.equals(StringFieldScript.class)) {
            StringFieldScript.Factory stringFieldScript = (f, p, s, onScriptError) -> ctx -> new StringFieldScript(
                f,
                p,
                s,
                OnScriptError.FAIL,
                ctx
            ) {
                @Override
                public void execute() {
                    emit("test");
                }
            };
            return context.factoryClazz.cast(stringFieldScript);
        } else if (context.instanceClazz.equals(LongFieldScript.class)) {
            LongFieldScript.Factory longFieldScript = (f, p, s, onScriptError) -> ctx -> new LongFieldScript(
                f,
                p,
                s,
                OnScriptError.FAIL,
                ctx
            ) {
                @Override
                public void execute() {
                    emit(1L);
                }
            };
            return context.factoryClazz.cast(longFieldScript);
        } else if (context.instanceClazz.equals(DoubleFieldScript.class)) {
            DoubleFieldScript.Factory doubleFieldScript = (f, p, s, onScriptError) -> ctx -> new DoubleFieldScript(
                f,
                p,
                s,
                OnScriptError.FAIL,
                ctx
            ) {
                @Override
                public void execute() {
                    emit(1.2D);
                }
            };
            return context.factoryClazz.cast(doubleFieldScript);
        } else if (context.instanceClazz.equals(DateFieldScript.class)) {
            DateFieldScript.Factory dateFieldScript = (f, p, s, formatter, onScriptError) -> ctx -> new DateFieldScript(
                f,
                p,
                s,
                formatter,
                OnScriptError.FAIL,
                ctx
            ) {
                @Override
                public void execute() {
                    emit(123L);
                }
            };
            return context.factoryClazz.cast(dateFieldScript);
        } else if (context.instanceClazz.equals(IpFieldScript.class)) {
            IpFieldScript.Factory ipFieldScript = (f, p, s, onScriptError) -> ctx -> new IpFieldScript(f, p, s, OnScriptError.FAIL, ctx) {
                @Override
                public void execute() {
                    emit("127.0.0.1");
                }
            };
            return context.factoryClazz.cast(ipFieldScript);
        } else if (context.instanceClazz.equals(GeoPointFieldScript.class)) {
            GeoPointFieldScript.Factory geoPointFieldScript = (f, p, s, onScriptError) -> ctx -> new GeoPointFieldScript(
                f,
                p,
                s,
                OnScriptError.FAIL,
                ctx
            ) {
                @Override
                public void execute() {
                    emit(1.2D, 1.2D);
                }
            };
            return context.factoryClazz.cast(geoPointFieldScript);
        } else if (context.instanceClazz.equals(CompositeFieldScript.class)) {
            CompositeFieldScript.Factory objectFieldScript = (f, p, s, onScriptError) -> ctx -> new CompositeFieldScript(
                f,
                p,
                s,
                OnScriptError.FAIL,
                ctx
            ) {
                @Override
                public void execute() {
                    emit("field1", "value1");
                    emit("field2", "value2");
                }
            };
            return context.factoryClazz.cast(objectFieldScript);
        } else if (context.instanceClazz.equals(GeometryFieldScript.class)) {
            GeometryFieldScript.Factory geometryFieldScript = (f, p, s, onScriptError) -> ctx -> new GeometryFieldScript(
                f,
                p,
                s,
                OnScriptError.FAIL,
                ctx
            ) {
                @Override
                public void execute() {
                    emitFromObject("POINT(1.2 1.2)");
                }
            };
            return context.factoryClazz.cast(geometryFieldScript);
        } else if (context.instanceClazz.equals(DoubleValuesScript.class)) {
            DoubleValuesScript.Factory doubleValuesScript = () -> new MockDoubleValuesScript();
            return context.factoryClazz.cast(doubleValuesScript);
        }
        ContextCompiler compiler = contexts.get(context);
        if (compiler != null) {
            return context.factoryClazz.cast(compiler.compile(script, params));
        }
        throw new IllegalArgumentException("mock script engine does not know how to handle context [" + context.name + "]");
    }

    @Override
    public Set<ScriptContext<?>> getSupportedContexts() {
        return Set.of(
            FieldScript.CONTEXT,
            TermsSetQueryScript.CONTEXT,
            NumberSortScript.CONTEXT,
            StringSortScript.CONTEXT,
            IngestScript.CONTEXT,
            AggregationScript.CONTEXT,
            IngestConditionalScript.CONTEXT,
            UpdateScript.CONTEXT,
            BucketAggregationScript.CONTEXT,
            BucketAggregationSelectorScript.CONTEXT,
            SignificantTermsHeuristicScoreScript.CONTEXT,
            TemplateScript.CONTEXT,
            FilterScript.CONTEXT,
            SimilarityScript.CONTEXT,
            SimilarityWeightScript.CONTEXT,
            ScoreScript.CONTEXT,
            ScriptedMetricAggContexts.InitScript.CONTEXT,
            ScriptedMetricAggContexts.MapScript.CONTEXT,
            ScriptedMetricAggContexts.CombineScript.CONTEXT,
            ScriptedMetricAggContexts.ReduceScript.CONTEXT,
            IntervalFilterScript.CONTEXT
        );
    }

    private static Map<String, Object> createVars(Map<String, Object> params) {
        Map<String, Object> vars = new HashMap<>();
        vars.put("params", params);
        return vars;
    }

    public class MockCompiledScript {

        private final String name;
        private final String source;
        private final Map<String, String> options;
        private final Function<Map<String, Object>, Object> script;

        public MockCompiledScript(String name, Map<String, String> options, String source, Function<Map<String, Object>, Object> script) {
            this.name = name;
            this.source = source;
            this.options = options;
            this.script = script;
        }

        public String getName() {
            return name;
        }

        public FilterScript.LeafFactory createFilterScript(Map<String, Object> params, SearchLookup lookup) {
            return new MockFilterScript(lookup, params, script);
        }

        public SimilarityScript createSimilarityScript() {
            return new MockSimilarityScript(script != null ? script : ctx -> 42d);
        }

        public SimilarityWeightScript createSimilarityWeightScript() {
            return new MockSimilarityWeightScript(script != null ? script : ctx -> 42d);
        }

        public IntervalFilterScript createIntervalFilterScript() {
            return new IntervalFilterScript() {
                @Override
                public boolean execute(Interval interval) {
                    return false;
                }
            };
        }
    }

    public static class MockFilterScript implements FilterScript.LeafFactory {

        private final Function<Map<String, Object>, Object> script;
        private final Map<String, Object> vars;
        private final SearchLookup lookup;

        public MockFilterScript(SearchLookup lookup, Map<String, Object> vars, Function<Map<String, Object>, Object> script) {
            this.lookup = lookup;
            this.vars = vars;
            this.script = script;
        }

        public FilterScript newInstance(DocReader docReader) throws IOException {
            Map<String, Object> ctx = new HashMap<>(docReader.docAsMap());
            if (vars != null) {
                ctx.putAll(vars);
            }
            return new FilterScript(ctx, lookup, docReader) {
                @Override
                public boolean execute() {
                    return (boolean) script.apply(ctx);
                }

            };
        }
    }

    public class MockSimilarityScript extends SimilarityScript {

        private final Function<Map<String, Object>, Object> script;

        MockSimilarityScript(Function<Map<String, Object>, Object> script) {
            this.script = script;
        }

        @Override
        public double execute(double weight, Query query, Field field, Term term, Doc doc) {
            Map<String, Object> map = new HashMap<>();
            map.put("weight", weight);
            map.put("query", query);
            map.put("field", field);
            map.put("term", term);
            map.put("doc", doc);
            return ((Number) script.apply(map)).doubleValue();
        }
    }

    public class MockSimilarityWeightScript extends SimilarityWeightScript {

        private final Function<Map<String, Object>, Object> script;

        MockSimilarityWeightScript(Function<Map<String, Object>, Object> script) {
            this.script = script;
        }

        @Override
        public double execute(Query query, Field field, Term term) {
            Map<String, Object> map = new HashMap<>();
            map.put("query", query);
            map.put("field", field);
            map.put("term", term);
            return ((Number) script.apply(map)).doubleValue();
        }
    }

    public static class MockMetricAggInitScriptFactory implements ScriptedMetricAggContexts.InitScript.Factory {
        private final MockDeterministicScript script;

        MockMetricAggInitScriptFactory(MockDeterministicScript script) {
            this.script = script;
        }

        @Override
        public boolean isResultDeterministic() {
            return script.isResultDeterministic();
        }

        @Override
        public ScriptedMetricAggContexts.InitScript newInstance(Map<String, Object> params, Map<String, Object> state) {
            return new MockMetricAggInitScript(params, state, script);
        }
    }

    public static class MockMetricAggInitScript extends ScriptedMetricAggContexts.InitScript {
        private final Function<Map<String, Object>, Object> script;

        MockMetricAggInitScript(Map<String, Object> params, Map<String, Object> state, Function<Map<String, Object>, Object> script) {
            super(params, state);
            this.script = script;
        }

        public void execute() {
            Map<String, Object> map = new HashMap<>();

            if (getParams() != null) {
                map.putAll(getParams()); // TODO: remove this once scripts know to look for params under params key
                map.put("params", getParams());
            }

            map.put("state", getState());
            script.apply(map);
        }
    }

    public static class MockMetricAggMapScriptFactory implements ScriptedMetricAggContexts.MapScript.Factory {
        private final MockDeterministicScript script;

        MockMetricAggMapScriptFactory(MockDeterministicScript script) {
            this.script = script;
        }

        @Override
        public boolean isResultDeterministic() {
            return script.isResultDeterministic();
        }

        @Override
        public ScriptedMetricAggContexts.MapScript.LeafFactory newFactory(
            Map<String, Object> params,
            Map<String, Object> state,
            SearchLookup lookup
        ) {
            return new MockMetricAggMapScript(params, state, lookup, script);
        }
    }

    public static class MockMetricAggMapScript implements ScriptedMetricAggContexts.MapScript.LeafFactory {
        private final Map<String, Object> params;
        private final Map<String, Object> state;
        private final SearchLookup lookup;
        private final Function<Map<String, Object>, Object> script;

        MockMetricAggMapScript(
            Map<String, Object> params,
            Map<String, Object> state,
            SearchLookup lookup,
            Function<Map<String, Object>, Object> script
        ) {
            this.params = params;
            this.state = state;
            this.lookup = lookup;
            this.script = script;
        }

        @Override
        public ScriptedMetricAggContexts.MapScript newInstance(LeafReaderContext context) {
            return new ScriptedMetricAggContexts.MapScript(params, state, lookup, context) {
                @Override
                public void execute() {
                    Map<String, Object> map = new HashMap<>();

                    if (getParams() != null) {
                        map.putAll(getParams()); // TODO: remove this once scripts know to look for params under params key
                        map.put("params", getParams());
                    }

                    map.put("state", getState());
                    map.put("doc", getDoc());
                    map.put("_score", get_score());

                    script.apply(map);
                }
            };
        }
    }

    public static class MockMetricAggCombineScriptFactory implements ScriptedMetricAggContexts.CombineScript.Factory {
        private final MockDeterministicScript script;

        MockMetricAggCombineScriptFactory(MockDeterministicScript script) {
            this.script = script;
        }

        @Override
        public boolean isResultDeterministic() {
            return script.isResultDeterministic();
        }

        @Override
        public ScriptedMetricAggContexts.CombineScript newInstance(Map<String, Object> params, Map<String, Object> state) {
            return new MockMetricAggCombineScript(params, state, script);
        }
    }

    public static class MockMetricAggCombineScript extends ScriptedMetricAggContexts.CombineScript {
        private final Function<Map<String, Object>, Object> script;

        MockMetricAggCombineScript(Map<String, Object> params, Map<String, Object> state, Function<Map<String, Object>, Object> script) {
            super(params, state);
            this.script = script;
        }

        public Object execute() {
            Map<String, Object> map = new HashMap<>();

            if (getParams() != null) {
                map.putAll(getParams()); // TODO: remove this once scripts know to look for params under params key
                map.put("params", getParams());
            }

            map.put("state", getState());
            return script.apply(map);
        }
    }

    public static class MockMetricAggReduceScriptFactory implements ScriptedMetricAggContexts.ReduceScript.Factory {
        private final MockDeterministicScript script;

        MockMetricAggReduceScriptFactory(MockDeterministicScript script) {
            this.script = script;
        }

        @Override
        public boolean isResultDeterministic() {
            return script.isResultDeterministic();
        }

        @Override
        public ScriptedMetricAggContexts.ReduceScript newInstance(Map<String, Object> params, List<Object> states) {
            return new MockMetricAggReduceScript(params, states, script);
        }
    }

    public static class MockMetricAggReduceScript extends ScriptedMetricAggContexts.ReduceScript {
        private final Function<Map<String, Object>, Object> script;

        MockMetricAggReduceScript(Map<String, Object> params, List<Object> states, Function<Map<String, Object>, Object> script) {
            super(params, states);
            this.script = script;
        }

        public Object execute() {
            Map<String, Object> map = new HashMap<>();

            if (getParams() != null) {
                map.putAll(getParams()); // TODO: remove this once scripts know to look for params under params key
                map.put("params", getParams());
            }

            map.put("states", getStates());
            return script.apply(map);
        }
    }

    public static Script mockInlineScript(final String script) {
        return new Script(ScriptType.INLINE, "mock", script, emptyMap());
    }

    public class MockScoreScript implements ScoreScript.Factory {

        private final MockDeterministicScript script;

        public MockScoreScript(MockDeterministicScript script) {
            this.script = script;
        }

        @Override
        public ScoreScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
            return new ScoreScript.LeafFactory() {
                @Override
                public boolean needs_score() {
                    return true;
                }

                @Override
                public boolean needs_termStats() {
                    return false;
                }

                @Override
                public ScoreScript newInstance(DocReader docReader) throws IOException {
                    Scorable[] scorerHolder = new Scorable[1];
                    return new ScoreScript(params, null, docReader) {
                        @Override
                        public double execute(ExplanationHolder explanation) {
                            Map<String, Object> vars = new HashMap<>(getParams());
                            vars.put("doc", getDoc());
                            if (scorerHolder[0] != null) {
                                vars.put("_score", new ScoreAccessor(scorerHolder[0]));
                            }
                            return ((Number) script.apply(vars)).doubleValue();
                        }

                        @Override
                        public void setScorer(Scorable scorer) {
                            scorerHolder[0] = scorer;
                        }
                    };
                }
            };
        }

        @Override
        public boolean isResultDeterministic() {
            return script.isResultDeterministic();
        }
    }

    class MockAggregationScript implements AggregationScript.Factory {
        private final MockDeterministicScript script;

        MockAggregationScript(MockDeterministicScript script) {
            this.script = script;
        }

        @Override
        public boolean isResultDeterministic() {
            return script.isResultDeterministic();
        }

        @Override
        public AggregationScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
            return new AggregationScript.LeafFactory() {
                @Override
                public AggregationScript newInstance(final LeafReaderContext ctx) {
                    return new AggregationScript(params, lookup, ctx) {
                        @Override
                        public Object execute() {
                            Map<String, Object> vars = new HashMap<>(params);
                            vars.put("params", params);
                            vars.put("doc", getDoc());
                            vars.put("_score", get_score());
                            vars.put("_value", get_value());
                            return script.apply(vars);
                        }
                    };
                }

                @Override
                public boolean needs_score() {
                    return true;
                }
            };
        }
    }

    class MockSignificantTermsHeuristicScoreScript implements SignificantTermsHeuristicScoreScript.Factory {
        private final MockDeterministicScript script;

        MockSignificantTermsHeuristicScoreScript(MockDeterministicScript script) {
            this.script = script;
        }

        @Override
        public boolean isResultDeterministic() {
            return script.isResultDeterministic();
        }

        @Override
        public SignificantTermsHeuristicScoreScript newInstance() {
            return new SignificantTermsHeuristicScoreScript() {
                @Override
                public double execute(Map<String, Object> vars) {
                    return ((Number) script.apply(vars)).doubleValue();
                }
            };
        }
    }

    class MockFieldScriptFactory implements FieldScript.Factory {
        private final MockDeterministicScript script;

        MockFieldScriptFactory(MockDeterministicScript script) {
            this.script = script;
        }

        @Override
        public boolean isResultDeterministic() {
            return script.isResultDeterministic();
        }

        @Override
        public FieldScript.LeafFactory newFactory(Map<String, Object> parameters, SearchLookup lookup) {
            return ctx -> new FieldScript(parameters, lookup, ctx) {
                @Override
                public Object execute() {
                    Map<String, Object> vars = createVars(parameters);
                    vars.putAll(docAsMap());
                    return script.apply(vars);

                }
            };
        }
    }

    class MockStringSortScriptFactory implements StringSortScript.Factory {
        private final MockDeterministicScript script;

        MockStringSortScriptFactory(MockDeterministicScript script) {
            this.script = script;
        }

        @Override
        public boolean isResultDeterministic() {
            return script.isResultDeterministic();
        }

        @Override
        public StringSortScript.LeafFactory newFactory(Map<String, Object> parameters) {
            return docReader -> new StringSortScript(parameters, docReader) {
                @Override
                public String execute() {
                    Map<String, Object> vars = new HashMap<>(parameters);
                    vars.put("params", parameters);
                    vars.put("doc", getDoc());
                    try {
                        vars.put("_score", get_score());
                    } catch (Exception ignore) {
                        // nothing to do: if get_score throws we don't set the _score, likely the scorer is null,
                        // which is ok if _score was not requested e.g. top_hits.
                    }
                    return String.valueOf(script.apply(vars));
                }
            };
        }
    }

    class MockBytesRefSortScriptFactory implements BytesRefSortScript.Factory {
        private final MockDeterministicScript script;

        MockBytesRefSortScriptFactory(MockDeterministicScript script) {
            this.script = script;
        }

        @Override
        public boolean isResultDeterministic() {
            return script.isResultDeterministic();
        }

        @Override
        public BytesRefSortScript.LeafFactory newFactory(Map<String, Object> parameters) {
            return docReader -> new BytesRefSortScript(parameters, docReader) {
                @Override
                public BytesRefProducer execute() {
                    Map<String, Object> vars = new HashMap<>(parameters);
                    vars.put("params", parameters);
                    vars.put("doc", getDoc());
                    try {
                        vars.put("_score", get_score());
                    } catch (Exception ignore) {
                        // nothing to do: if get_score throws we don't set the _score, likely the scorer is null,
                        // which is ok if _score was not requested e.g. top_hits.
                    }
                    return (BytesRefProducer) script.apply(vars);
                }
            };
        }
    }

    class MockDoubleValuesScript extends DoubleValuesScript {
        @Override
        public double execute() {
            return 1.0;
        }

        @Override
        public double evaluate(DoubleValues[] functionValues) {
            return 1.0;
        }

        @Override
        public DoubleValuesSource getDoubleValuesSource(Function<String, DoubleValuesSource> sourceProvider) {
            return null;
        }

        @Override
        public SortField getSortField(Function<String, DoubleValuesSource> sourceProvider, boolean reverse) {
            return null;
        }

        @Override
        public Rescorer getRescorer(Function<String, DoubleValuesSource> sourceProvider) {
            return null;
        }

        @Override
        public String sourceText() {
            return null;
        }

        @Override
        public String[] variables() {
            return new String[0];
        }
    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.script.groovy;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.codehaus.groovy.ast.ClassCodeExpressionTransformer;
import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.ast.expr.ConstantExpression;
import org.codehaus.groovy.ast.expr.Expression;
import org.codehaus.groovy.classgen.GeneratorContext;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilePhase;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.control.SourceUnit;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;
import org.codehaus.groovy.control.customizers.ImportCustomizer;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides the infrastructure for Groovy as a scripting language for Elasticsearch
 */
public class GroovyScriptEngineService extends AbstractComponent implements ScriptEngineService {

    public static String GROOVY_SCRIPT_SANDBOX_ENABLED = "script.groovy.sandbox.enabled";

    private final AtomicLong counter = new AtomicLong();
    private final GroovyClassLoader loader;
    private final boolean sandboxed;

    @Inject
    public GroovyScriptEngineService(Settings settings) {
        super(settings);
        ImportCustomizer imports = new ImportCustomizer();
        imports.addStarImports("org.joda.time");
        imports.addStaticStars("java.lang.Math");
        CompilerConfiguration config = new CompilerConfiguration();
        config.addCompilationCustomizers(imports);
        this.sandboxed = settings.getAsBoolean(GROOVY_SCRIPT_SANDBOX_ENABLED, true);
        if (this.sandboxed) {
            config.addCompilationCustomizers(GroovySandboxExpressionChecker.getSecureASTCustomizer(settings));
        }
        // Add BigDecimal -> Double transformer
        config.addCompilationCustomizers(new GroovyBigDecimalTransformer(CompilePhase.CONVERSION));
        this.loader = new GroovyClassLoader(settings.getClassLoader(), config);
    }

    @Override
    public void close() {
        loader.clearCache();
        try {
            loader.close();
        } catch (IOException e) {
            logger.warn("Unable to close Groovy loader", e);
        }
    }

    @Override
    public String[] types() {
        return new String[]{"groovy"};
    }

    @Override
    public String[] extensions() {
        return new String[]{"groovy"};
    }

    @Override
    public boolean sandboxed() {
        return this.sandboxed;
    }

    @Override
    public Object compile(String script) {
        try {
            return loader.parseClass(script, generateScriptName());
        } catch (Throwable e) {
            if (logger.isTraceEnabled()) {
                logger.trace("exception compiling Groovy script:", e);
            }
            throw new GroovyScriptCompilationException(ExceptionsHelper.detailedMessage(e));
        }
    }

    /**
     * Return a script object with the given vars from the compiled script object
     */
    private Script createScript(Object compiledScript, Map<String, Object> vars) throws InstantiationException, IllegalAccessException {
        Class scriptClass = (Class) compiledScript;
        Script scriptObject = (Script) scriptClass.newInstance();
        Binding binding = new Binding();
        binding.getVariables().putAll(vars);
        scriptObject.setBinding(binding);
        return scriptObject;
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public ExecutableScript executable(Object compiledScript, Map<String, Object> vars) {
        try {
            Map<String, Object> allVars = new HashMap<>();
            if (vars != null) {
                allVars.putAll(vars);
            }
            return new GroovyScript(createScript(compiledScript, allVars), this.logger);
        } catch (Exception e) {
            throw new ScriptException("failed to build executable script", e);
        }
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public SearchScript search(Object compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        try {
            Map<String, Object> allVars = new HashMap<>();
            allVars.putAll(lookup.asMap());
            if (vars != null) {
                allVars.putAll(vars);
            }
            Script scriptObject = createScript(compiledScript, allVars);
            return new GroovyScript(scriptObject, lookup, this.logger);
        } catch (Exception e) {
            throw new ScriptException("failed to build search script", e);
        }
    }

    @Override
    public Object execute(Object compiledScript, Map<String, Object> vars) {
        try {
            Map<String, Object> allVars = new HashMap<>();
            if (vars != null) {
                allVars.putAll(vars);
            }
            Script scriptObject = createScript(compiledScript, allVars);
            return scriptObject.run();
        } catch (Exception e) {
            throw new ScriptException("failed to execute script", e);
        }
    }

    @Override
    public Object unwrap(Object value) {
        return value;
    }

    private String generateScriptName() {
        return "Script" + counter.incrementAndGet() + ".groovy";
    }

    public static final class GroovyScript implements ExecutableScript, SearchScript {

        private final Script script;
        private final SearchLookup lookup;
        private final Map<String, Object> variables;
        private final UpdateableFloat score;
        private final ESLogger logger;

        public GroovyScript(Script script, ESLogger logger) {
            this(script, null, logger);
        }

        public GroovyScript(Script script, SearchLookup lookup, ESLogger logger) {
            this.script = script;
            this.lookup = lookup;
            this.logger = logger;
            this.variables = script.getBinding().getVariables();
            this.score = new UpdateableFloat(0);
            // Add the _score variable, which will be updated per-document by
            // setting .value on the UpdateableFloat instance
            this.variables.put("_score", this.score);
        }

        @Override
        public void setScorer(Scorer scorer) {
            if (lookup != null) {
                lookup.setScorer(scorer);
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) {
            if (lookup != null) {
                lookup.setNextReader(context);
            }
        }

        @Override
        public void setNextDocId(int doc) {
            if (lookup != null) {
                lookup.setNextDocId(doc);
            }
        }

        @SuppressWarnings({"unchecked"})
        @Override
        public void setNextScore(float score) {
            this.score.value = score;
        }

        @SuppressWarnings({"unchecked"})
        @Override
        public void setNextVar(String name, Object value) {
            variables.put(name, value);
        }

        @Override
        public void setNextSource(Map<String, Object> source) {
            if (lookup != null) {
                lookup.source().setNextSource(source);
            }
        }

        @Override
        public Object run() {
            try {
                return script.run();
            } catch (Throwable e) {
                if (logger.isTraceEnabled()) {
                    logger.trace("exception running Groovy script", e);
                }
                throw new GroovyScriptExecutionException(ExceptionsHelper.detailedMessage(e));
            }
        }

        @Override
        public float runAsFloat() {
            return ((Number) run()).floatValue();
        }

        @Override
        public long runAsLong() {
            return ((Number) run()).longValue();
        }

        @Override
        public double runAsDouble() {
            return ((Number) run()).doubleValue();
        }

        @Override
        public Object unwrap(Object value) {
            return value;
        }

        /**
         * Float encapsulation that allows updating the value with public
         * member access. This is used to encapsulate the _score of a document
         * so that updating the _score for the next document incurs only the
         * overhead of setting a member variable
         */
        private final class UpdateableFloat extends Number {

            public float value;

            public UpdateableFloat(float value) {
                this.value = value;
            }

            @Override
            public int intValue() {
                return (int)value;
            }

            @Override
            public long longValue() {
                return (long)value;
            }

            @Override
            public float floatValue() {
                return value;
            }

            @Override
            public double doubleValue() {
                return value;
            }
        }
    }

    /**
     * A compilation customizer that is used to transform a number like 1.23,
     * which would normally be a BigDecimal, into a double value.
     */
    private class GroovyBigDecimalTransformer extends CompilationCustomizer {

        private GroovyBigDecimalTransformer(CompilePhase phase) {
            super(phase);
        }

        @Override
        public void call(final SourceUnit source, final GeneratorContext context, final ClassNode classNode) throws CompilationFailedException {
            new BigDecimalExpressionTransformer(source).visitClass(classNode);
        }
    }

    /**
     * Groovy expression transformer that converts BigDecimals to doubles
     */
    private class BigDecimalExpressionTransformer extends ClassCodeExpressionTransformer {

        private final SourceUnit source;

        private BigDecimalExpressionTransformer(SourceUnit source) {
            this.source = source;
        }

        @Override
        protected SourceUnit getSourceUnit() {
            return this.source;
        }

        @Override
        public Expression transform(Expression expr) {
            Expression newExpr = expr;
            if (expr instanceof ConstantExpression) {
                ConstantExpression constExpr = (ConstantExpression) expr;
                Object val = constExpr.getValue();
                if (val != null && val instanceof BigDecimal) {
                    newExpr = new ConstantExpression(((BigDecimal) val).doubleValue());
                }
            }
            return super.transform(newExpr);
        }
    }
}
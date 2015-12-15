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
import groovy.lang.GroovyCodeSource;
import groovy.lang.Script;
import org.apache.lucene.index.LeafReaderContext;
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
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.*;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides the infrastructure for Groovy as a scripting language for Elasticsearch
 */
public class GroovyScriptEngineService extends AbstractComponent implements ScriptEngineService {

    /**
     * The name of the scripting engine/language.
     */
    public static final String NAME = "groovy";
    /**
     * The name of the Groovy compiler setting to use associated with activating <code>invokedynamic</code> support.
     */
    public static final String GROOVY_INDY_SETTING_NAME = "indy";

    private final GroovyClassLoader loader;

    @Inject
    public GroovyScriptEngineService(Settings settings) {
        super(settings);

        ImportCustomizer imports = new ImportCustomizer();
        imports.addStarImports("org.joda.time");
        imports.addStaticStars("java.lang.Math");

        CompilerConfiguration config = new CompilerConfiguration();

        config.addCompilationCustomizers(imports);
        // Add BigDecimal -> Double transformer
        config.addCompilationCustomizers(new GroovyBigDecimalTransformer(CompilePhase.CONVERSION));

        // always enable invokeDynamic, not the crazy softreference-based stuff
        config.getOptimizationOptions().put(GROOVY_INDY_SETTING_NAME, true);

        // Groovy class loader to isolate Groovy-land code
        // classloader created here
        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        this.loader = AccessController.doPrivileged(new PrivilegedAction<GroovyClassLoader>() {
            @Override
            public GroovyClassLoader run() {
                // snapshot our context (which has permissions for classes), since the script has none
                final AccessControlContext engineContext = AccessController.getContext();
                return new GroovyClassLoader(new ClassLoader(getClass().getClassLoader()) {
                    @Override
                    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                        if (sm != null) {
                            try {
                                engineContext.checkPermission(new ClassPermission(name));
                            } catch (SecurityException e) {
                                throw new ClassNotFoundException(name, e);
                            }
                        }
                        return super.loadClass(name, resolve);
                    }
                }, config);
            }
        });
    }

    @Override
    public void close() {
        loader.clearCache();
        // close classloader here (why do we do this?)
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                try {
                    loader.close();
                } catch (IOException e) {
                    logger.warn("Unable to close Groovy loader", e);
                }
                return null;
            }
        });
    }

    @Override
    public void scriptRemoved(@Nullable CompiledScript script) {
        // script could be null, meaning the script has already been garbage collected
        if (script == null || NAME.equals(script.lang())) {
            // Clear the cache, this removes old script versions from the
            // cache to prevent running out of PermGen space
            loader.clearCache();
        }
    }

    @Override
    public String[] types() {
        return new String[]{NAME};
    }

    @Override
    public String[] extensions() {
        return new String[]{NAME};
    }

    @Override
    public boolean sandboxed() {
        return false;
    }

    @Override
    public Object compile(String script) {
        try {
            // we reuse classloader, so do a security check just in case.
            SecurityManager sm = System.getSecurityManager();
            if (sm != null) {
                sm.checkPermission(new SpecialPermission());
            }
            String fake = MessageDigests.toHexString(MessageDigests.sha1().digest(script.getBytes(StandardCharsets.UTF_8)));
            // same logic as GroovyClassLoader.parseClass() but with a different codesource string:
            return AccessController.doPrivileged(new PrivilegedAction<Object>() {
                public Class<?> run() {
                    GroovyCodeSource gcs = new GroovyCodeSource(script, fake, BootstrapInfo.UNTRUSTED_CODEBASE);
                    gcs.setCachable(false);
                    // TODO: we could be more complicated and paranoid, and move this to separate block, to
                    // sandbox the compilation process itself better.
                    return loader.parseClass(gcs);
                }
            });
        } catch (Throwable e) {
            if (logger.isTraceEnabled()) {
                logger.trace("exception compiling Groovy script:", e);
            }
            throw new ScriptException("failed to compile groovy script", e);
        }
    }

    /**
     * Return a script object with the given vars from the compiled script object
     */
    @SuppressWarnings("unchecked")
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
    public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> vars) {
        try {
            Map<String, Object> allVars = new HashMap<>();
            if (vars != null) {
                allVars.putAll(vars);
            }
            return new GroovyScript(compiledScript, createScript(compiledScript.compiled(), allVars), this.logger);
        } catch (Exception e) {
            throw new ScriptException("failed to build executable " + compiledScript, e);
        }
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public SearchScript search(final CompiledScript compiledScript, final SearchLookup lookup, @Nullable final Map<String, Object> vars) {
        return new SearchScript() {

            @Override
            public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
                final LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);
                Map<String, Object> allVars = new HashMap<>();
                allVars.putAll(leafLookup.asMap());
                if (vars != null) {
                    allVars.putAll(vars);
                }
                Script scriptObject;
                try {
                    scriptObject = createScript(compiledScript.compiled(), allVars);
                } catch (InstantiationException | IllegalAccessException e) {
                    throw new ScriptException("failed to build search " + compiledScript, e);
                }
                return new GroovyScript(compiledScript, scriptObject, leafLookup, logger);
            }

            @Override
            public boolean needsScores() {
                // TODO: can we reliably know if a groovy script makes use of _score
                return true;
            }
        };
    }

    public static final class GroovyScript implements ExecutableScript, LeafSearchScript {

        private final CompiledScript compiledScript;
        private final Script script;
        private final LeafSearchLookup lookup;
        private final Map<String, Object> variables;
        private final ESLogger logger;

        public GroovyScript(CompiledScript compiledScript, Script script, ESLogger logger) {
            this(compiledScript, script, null, logger);
        }

        @SuppressWarnings("unchecked")
        public GroovyScript(CompiledScript compiledScript, Script script, @Nullable LeafSearchLookup lookup, ESLogger logger) {
            this.compiledScript = compiledScript;
            this.script = script;
            this.lookup = lookup;
            this.logger = logger;
            this.variables = script.getBinding().getVariables();
        }

        @Override
        public void setScorer(Scorer scorer) {
            this.variables.put("_score", new ScoreAccessor(scorer));
        }

        @Override
        public void setDocument(int doc) {
            if (lookup != null) {
                lookup.setDocument(doc);
            }
        }

        @SuppressWarnings({"unchecked"})
        @Override
        public void setNextVar(String name, Object value) {
            variables.put(name, value);
        }

        @Override
        public void setSource(Map<String, Object> source) {
            if (lookup != null) {
                lookup.source().setSource(source);
            }
        }

        @Override
        public Object run() {
            try {
                // NOTE: we truncate the stack because IndyInterface has security issue (needs getClassLoader)
                // we don't do a security check just as a tradeoff, it cannot really escalate to anything.
                return AccessController.doPrivileged(new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        return script.run();
                    }
                });
            } catch (Throwable e) {
                if (logger.isTraceEnabled()) {
                    logger.trace("failed to run " + compiledScript, e);
                }
                throw new ScriptException("failed to run " + compiledScript, e);
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

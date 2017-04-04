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

package org.elasticsearch.painless;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.painless.Compiler.Loader;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of a ScriptEngine for the Painless language.
 */
public final class PainlessScriptEngineService extends AbstractComponent implements ScriptEngineService {

    /**
     * Standard name of the Painless language.
     */
    public static final String NAME = "painless";

    /**
     * Permissions context used during compilation.
     */
    private static final AccessControlContext COMPILATION_CONTEXT;

    /**
     * Setup the allowed permissions.
     */
    static {
        final Permissions none = new Permissions();
        none.setReadOnly();
        COMPILATION_CONTEXT = new AccessControlContext(new ProtectionDomain[] {
            new ProtectionDomain(null, none)
        });
    }

    /**
     * Default compiler settings to be used. Note that {@link CompilerSettings} is mutable but this instance shouldn't be mutated outside
     * of {@link PainlessScriptEngineService#PainlessScriptEngineService(Settings)}.
     */
    private final CompilerSettings defaultCompilerSettings = new CompilerSettings();

    /**
     * Constructor.
     * @param settings The settings to initialize the engine with.
     */
    public PainlessScriptEngineService(final Settings settings) {
        super(settings);
        defaultCompilerSettings.setRegexesEnabled(CompilerSettings.REGEX_ENABLED.get(settings));
    }

    /**
     * Get the type name(s) for the language.
     * @return Always contains only the single name of the language.
     */
    @Override
    public String getType() {
        return NAME;
    }

    /**
     * Get the extension(s) for the language.
     * @return Always contains only the single extension of the language.
     */
    @Override
    public String getExtension() {
        return NAME;
    }

    /**
     * When a script is anonymous (inline), we give it this name.
     */
    static final String INLINE_NAME = "<inline>";

    @Override
    public Object compile(String scriptName, final String scriptSource, final Map<String, String> params) {
        return compile(GenericElasticsearchScript.class, scriptName, scriptSource, params);
    }

    <T> T compile(Class<T> iface, String scriptName, final String scriptSource, final Map<String, String> params) {
        final CompilerSettings compilerSettings;

        if (params.isEmpty()) {
            // Use the default settings.
            compilerSettings = defaultCompilerSettings;
        } else {
            // Use custom settings specified by params.
            compilerSettings = new CompilerSettings();

            // Except regexes enabled - this is a node level setting and can't be changed in the request.
            compilerSettings.setRegexesEnabled(defaultCompilerSettings.areRegexesEnabled());

            Map<String, String> copy = new HashMap<>(params);

            String value = copy.remove(CompilerSettings.MAX_LOOP_COUNTER);
            if (value != null) {
                compilerSettings.setMaxLoopCounter(Integer.parseInt(value));
            }

            value = copy.remove(CompilerSettings.PICKY);
            if (value != null) {
                compilerSettings.setPicky(Boolean.parseBoolean(value));
            }

            value = copy.remove(CompilerSettings.INITIAL_CALL_SITE_DEPTH);
            if (value != null) {
                compilerSettings.setInitialCallSiteDepth(Integer.parseInt(value));
            }

            value = copy.remove(CompilerSettings.REGEX_ENABLED.getKey());
            if (value != null) {
                throw new IllegalArgumentException("[painless.regex.enabled] can only be set on node startup.");
            }

            if (!copy.isEmpty()) {
                throw new IllegalArgumentException("Unrecognized compile-time parameter(s): " + copy);
            }
        }

        // Check we ourselves are not being called by unprivileged code.
        SpecialPermission.check();

        // Create our loader (which loads compiled code with no permissions).
        final Loader loader = AccessController.doPrivileged(new PrivilegedAction<Loader>() {
            @Override
            public Loader run() {
                return new Loader(getClass().getClassLoader());
            }
        });

        try {
            // Drop all permissions to actually compile the code itself.
            return AccessController.doPrivileged(new PrivilegedAction<T>() {
                @Override
                public T run() {
                    String name = scriptName == null ? INLINE_NAME : scriptName;
                    return Compiler.compile(loader, iface, name, scriptSource, compilerSettings);
                }
            }, COMPILATION_CONTEXT);
        // Note that it is safe to catch any of the following errors since Painless is stateless.
        } catch (OutOfMemoryError | StackOverflowError | VerifyError | Exception e) {
            throw convertToScriptException(scriptName == null ? scriptSource : scriptName, scriptSource, e);
        }
    }

    /**
     * Retrieve an {@link ExecutableScript} for later use.
     * @param compiledScript A previously compiled script.
     * @param vars The variables to be used in the script.
     * @return An {@link ExecutableScript} with the currently specified variables.
     */
    @Override
    public ExecutableScript executable(final CompiledScript compiledScript, final Map<String, Object> vars) {
        return new ScriptImpl((GenericElasticsearchScript) compiledScript.compiled(), vars, null);
    }

    /**
     * Retrieve a {@link SearchScript} for later use.
     * @param compiledScript A previously compiled script.
     * @param lookup The object that ultimately allows access to search fields.
     * @param vars The variables to be used in the script.
     * @return An {@link SearchScript} with the currently specified variables.
     */
    @Override
    public SearchScript search(final CompiledScript compiledScript, final SearchLookup lookup, final Map<String, Object> vars) {
        return new SearchScript() {
            /**
             * Get the search script that will have access to search field values.
             * @param context The LeafReaderContext to be used.
             * @return A script that will have the search fields from the current context available for use.
             */
            @Override
            public LeafSearchScript getLeafSearchScript(final LeafReaderContext context) throws IOException {
                return new ScriptImpl((GenericElasticsearchScript) compiledScript.compiled(), vars, lookup.getLeafSearchLookup(context));
            }

            /**
             * Whether or not the score is needed.
             */
            @Override
            public boolean needsScores() {
                return ((GenericElasticsearchScript) compiledScript.compiled()).uses$_score();
            }
        };
    }

    /**
     * Action taken when the engine is closed.
     */
    @Override
    public void close() {
        // Nothing to do.
    }

    private ScriptException convertToScriptException(String scriptName, String scriptSource, Throwable t) {
        // create a script stack: this is just the script portion
        List<String> scriptStack = new ArrayList<>();
        for (StackTraceElement element : t.getStackTrace()) {
            if (WriterConstants.CLASS_NAME.equals(element.getClassName())) {
                // found the script portion
                int offset = element.getLineNumber();
                if (offset == -1) {
                    scriptStack.add("<<< unknown portion of script >>>");
                } else {
                    offset--; // offset is 1 based, line numbers must be!
                    int startOffset = getPreviousStatement(scriptSource, offset);
                    int endOffset = getNextStatement(scriptSource, offset);
                    StringBuilder snippet = new StringBuilder();
                    if (startOffset > 0) {
                        snippet.append("... ");
                    }
                    snippet.append(scriptSource.substring(startOffset, endOffset));
                    if (endOffset < scriptSource.length()) {
                        snippet.append(" ...");
                    }
                    scriptStack.add(snippet.toString());
                    StringBuilder pointer = new StringBuilder();
                    if (startOffset > 0) {
                        pointer.append("    ");
                    }
                    for (int i = startOffset; i < offset; i++) {
                        pointer.append(' ');
                    }
                    pointer.append("^---- HERE");
                    scriptStack.add(pointer.toString());
                }
                break;
            }
        }
        throw new ScriptException("compile error", t, scriptStack, scriptSource, PainlessScriptEngineService.NAME);
    }

    // very simple heuristic: +/- 25 chars. can be improved later.
    private int getPreviousStatement(String scriptSource, int offset) {
        return Math.max(0, offset - 25);
    }

    private int getNextStatement(String scriptSource, int offset) {
        return Math.min(scriptSource.length(), offset + 25);
    }

    @Override
    public boolean isInlineScriptEnabled() {
        return true;
    }
}

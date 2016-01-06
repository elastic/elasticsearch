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

package org.elasticsearch.plan.a;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.HashMap;
import java.util.Map;

public class PlanAScriptEngineService extends AbstractComponent implements ScriptEngineService {

    public static final String NAME = "plan-a";
    // default settings, used unless otherwise specified
    private static final CompilerSettings DEFAULT_COMPILER_SETTINGS = new CompilerSettings();

    public static final String NUMERIC_OVERFLOW = "numeric_overflow";

    // TODO: how should custom definitions be specified?
    private Definition definition = null;

    @Inject
    public PlanAScriptEngineService(Settings settings) {
        super(settings);
    }

    public void setDefinition(final Definition definition) {
        this.definition = new Definition(definition);
    }

    @Override
    public String[] types() {
        return new String[] { NAME };
    }

    @Override
    public String[] extensions() {
        return new String[] { NAME };
    }

    @Override
    public boolean sandboxed() {
        return true;
    }

    // context used during compilation
    private static final AccessControlContext COMPILATION_CONTEXT;
    static {
        Permissions none = new Permissions();
        none.setReadOnly();
        COMPILATION_CONTEXT = new AccessControlContext(new ProtectionDomain[] {
                new ProtectionDomain(null, none)
        });
    }

    @Override
    public Object compile(String script, Map<String, String> params) {
        final CompilerSettings compilerSettings;
        if (params.isEmpty()) {
            compilerSettings = DEFAULT_COMPILER_SETTINGS;
        } else {
            // custom settings
            compilerSettings = new CompilerSettings();
            Map<String,String> clone = new HashMap<>(params);
            String value = clone.remove(NUMERIC_OVERFLOW);
            if (value != null) {
                // TODO: can we get a real boolean parser in here?
                compilerSettings.setNumericOverflow(Boolean.parseBoolean(value));
            }
            if (!clone.isEmpty()) {
                throw new IllegalArgumentException("Unrecognized compile-time parameter(s): " + clone);
            }
        }
        // check we ourselves are not being called by unprivileged code
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        // create our loader (which loads compiled code with no permissions)
        Compiler.Loader loader = AccessController.doPrivileged(new PrivilegedAction<Compiler.Loader>() {
            @Override
            public Compiler.Loader run() {
                return new Compiler.Loader(getClass().getClassLoader());
            }
        });
        // drop all permissions to actually compile the code itself
        return AccessController.doPrivileged(new PrivilegedAction<Executable>() {
            @Override
            public Executable run() {
                return Compiler.compile(loader, "something", script, definition, compilerSettings);
            }
        }, COMPILATION_CONTEXT);
    }

    @Override
    public ExecutableScript executable(CompiledScript compiledScript, Map<String,Object> vars) {
        return new ScriptImpl((Executable) compiledScript.compiled(), vars, null);
    }

    @Override
    public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, Map<String,Object> vars) {
        return new SearchScript() {
            @Override
            public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
                return new ScriptImpl((Executable) compiledScript.compiled(), vars, lookup.getLeafSearchLookup(context));
            }

            @Override
            public boolean needsScores() {
                return true; // TODO: maybe even do these different and more like expressions.
            }
        };
    }

    @Override
    public void scriptRemoved(CompiledScript script) {
        // nothing to do
    }

    @Override
    public void close() throws IOException {
        // nothing to do
    }
}

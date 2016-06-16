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

package org.elasticsearch.script.python;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ClassPermission;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.ScoreAccessor;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.python.core.Py;
import org.python.core.PyCode;
import org.python.core.PyObject;
import org.python.core.PyStringMap;
import org.python.util.PythonInterpreter;

import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.Map;

/**
 *
 */
//TODO we can optimize the case for Map<String, Object> similar to PyStringMap
public class PythonScriptEngineService extends AbstractComponent implements ScriptEngineService {

    public static final String NAME = "python";
    public static final String EXTENSION = "py";

    private final PythonInterpreter interp;

    public PythonScriptEngineService(Settings settings) {
        super(settings);

        // classloader created here
        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        this.interp = AccessController.doPrivileged(new PrivilegedAction<PythonInterpreter> () {
            @Override
            public PythonInterpreter run() {
                // snapshot our context here for checks, as the script has no permissions
                final AccessControlContext engineContext = AccessController.getContext();
                PythonInterpreter interp = PythonInterpreter.threadLocalStateInterpreter(null);
                if (sm != null) {
                    interp.getSystemState().setClassLoader(new ClassLoader(getClass().getClassLoader()) {
                        @Override
                        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                            try {
                                engineContext.checkPermission(new ClassPermission(name));
                            } catch (SecurityException e) {
                                throw new ClassNotFoundException(name, e);
                            }
                            return super.loadClass(name, resolve);
                        }
                    });
                }
                return interp;
            }
        });
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public Object compile(String scriptName, String scriptSource, Map<String, String> params) {
        // classloader created here
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        return AccessController.doPrivileged(new PrivilegedAction<PyCode>() {
            @Override
            public PyCode run() {
                return interp.compile(scriptSource);
            }
        });
    }

    @Override
    public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> vars) {
        return new PythonExecutableScript((PyCode) compiledScript.compiled(), vars);
    }

    @Override
    public SearchScript search(final CompiledScript compiledScript, final SearchLookup lookup, @Nullable final Map<String, Object> vars) {
        return new SearchScript() {
            @Override
            public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {
                final LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);
                return new PythonSearchScript((PyCode) compiledScript.compiled(), vars, leafLookup);
            }
            @Override
            public boolean needsScores() {
                // TODO: can we reliably know if a python script makes use of _score
                return true;
            }
        };
    }

    @Override
    public void close() {
        interp.cleanup();
    }

    @Override
    public void scriptRemoved(@Nullable CompiledScript compiledScript) {
        // Nothing to do
    }

    public class PythonExecutableScript implements ExecutableScript {

        private final PyCode code;

        private final PyStringMap pyVars;

        public PythonExecutableScript(PyCode code, Map<String, Object> vars) {
            this.code = code;
            this.pyVars = new PyStringMap();
            if (vars != null) {
                for (Map.Entry<String, Object> entry : vars.entrySet()) {
                    pyVars.__setitem__(entry.getKey(), Py.java2py(entry.getValue()));
                }
            }
        }

        @Override
        public void setNextVar(String name, Object value) {
            pyVars.__setitem__(name, Py.java2py(value));
        }

        @Override
        public Object run() {
            interp.setLocals(pyVars);
            // eval the script with reduced privileges
            PyObject ret = evalRestricted(code);
            if (ret == null) {
                return null;
            }
            return ret.__tojava__(Object.class);
        }

        @Override
        public Object unwrap(Object value) {
            return unwrapValue(value);
        }
    }

    public class PythonSearchScript implements LeafSearchScript {

        private final PyCode code;

        private final PyStringMap pyVars;

        private final LeafSearchLookup lookup;

        public PythonSearchScript(PyCode code, Map<String, Object> vars, LeafSearchLookup lookup) {
            this.code = code;
            this.pyVars = new PyStringMap();
            for (Map.Entry<String, Object> entry : lookup.asMap().entrySet()) {
                pyVars.__setitem__(entry.getKey(), Py.java2py(entry.getValue()));
            }
            if (vars != null) {
                for (Map.Entry<String, Object> entry : vars.entrySet()) {
                    pyVars.__setitem__(entry.getKey(), Py.java2py(entry.getValue()));
                }
            }
            this.lookup = lookup;
        }

        @Override
        public void setScorer(Scorer scorer) {
            pyVars.__setitem__("_score", Py.java2py(new ScoreAccessor(scorer)));
        }

        @Override
        public void setDocument(int doc) {
            lookup.setDocument(doc);
        }

        @Override
        public void setSource(Map<String, Object> source) {
            lookup.source().setSource(source);
        }

        @Override
        public void setNextVar(String name, Object value) {
            pyVars.__setitem__(name, Py.java2py(value));
        }

        @Override
        public Object run() {
            interp.setLocals(pyVars);
            // eval the script with reduced privileges
            PyObject ret = evalRestricted(code);
            if (ret == null) {
                return null;
            }
            return ret.__tojava__(Object.class);
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
            return unwrapValue(value);
        }
    }

    // we don't have a way to specify codesource for generated jython classes,
    // so we just run them with a special context to reduce privileges
    private static final AccessControlContext PY_CONTEXT;
    static {
        Permissions none = new Permissions();
        none.setReadOnly();
        PY_CONTEXT = new AccessControlContext(new ProtectionDomain[] {
                new ProtectionDomain(null, none)
        });
    }

    /** Evaluates with reduced privileges */
    private final PyObject evalRestricted(final PyCode code) {
        // eval the script with reduced privileges
        return AccessController.doPrivileged(new PrivilegedAction<PyObject>() {
            @Override
            public PyObject run() {
                return interp.eval(code);
            }
        }, PY_CONTEXT);
    }

    public static Object unwrapValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof PyObject) {
            // seems like this is enough, inner PyDictionary will do the conversion for us for example, so expose it directly
            return ((PyObject) value).__tojava__(Object.class);
        }
        return value;
    }
}

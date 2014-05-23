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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.python.core.Py;
import org.python.core.PyCode;
import org.python.core.PyObject;
import org.python.core.PyStringMap;
import org.python.util.PythonInterpreter;

import java.util.Map;

/**
 *
 */
//TODO we can optimize the case for Map<String, Object> similar to PyStringMap
public class PythonScriptEngineService extends AbstractComponent implements ScriptEngineService {

    private final PythonInterpreter interp;

    @Inject
    public PythonScriptEngineService(Settings settings) {
        super(settings);

        this.interp = PythonInterpreter.threadLocalStateInterpreter(null);
    }

    @Override
    public String[] types() {
        return new String[]{"python", "py"};
    }

    @Override
    public String[] extensions() {
        return new String[]{"py"};
    }

    @Override
    public boolean sandboxed() {
        return false;
    }

    @Override
    public Object compile(String script) {
        return interp.compile(script);
    }

    @Override
    public ExecutableScript executable(Object compiledScript, Map<String, Object> vars) {
        return new PythonExecutableScript((PyCode) compiledScript, vars);
    }

    @Override
    public SearchScript search(Object compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        return new PythonSearchScript((PyCode) compiledScript, vars, lookup);
    }

    @Override
    public Object execute(Object compiledScript, Map<String, Object> vars) {
        PyObject pyVars = Py.java2py(vars);
        interp.setLocals(pyVars);
        PyObject ret = interp.eval((PyCode) compiledScript);
        if (ret == null) {
            return null;
        }
        return ret.__tojava__(Object.class);
    }

    @Override
    public Object unwrap(Object value) {
        return unwrapValue(value);
    }

    @Override
    public void close() {
        interp.cleanup();
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
            PyObject ret = interp.eval(code);
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

    public class PythonSearchScript implements SearchScript {

        private final PyCode code;

        private final PyStringMap pyVars;

        private final SearchLookup lookup;

        public PythonSearchScript(PyCode code, Map<String, Object> vars, SearchLookup lookup) {
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
            lookup.setScorer(scorer);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) {
            lookup.setNextReader(context);
        }

        @Override
        public void setNextDocId(int doc) {
            lookup.setNextDocId(doc);
        }

        @Override
        public void setNextSource(Map<String, Object> source) {
            lookup.source().setNextSource(source);
        }

        @Override
        public void setNextScore(float score) {
            pyVars.__setitem__("_score", Py.java2py(score));
        }

        @Override
        public void setNextVar(String name, Object value) {
            pyVars.__setitem__(name, Py.java2py(value));
        }

        @Override
        public Object run() {
            interp.setLocals(pyVars);
            PyObject ret = interp.eval(code);
            if (ret == null) {
                return null;
            }
            return ret.__tojava__(Object.class);
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
            return unwrapValue(value);
        }
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

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

package org.elasticsearch.script.expression;

import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.expressions.js.VariableContext;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleConstValueSource;
import org.apache.lucene.search.SortField;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.LegacyDateFieldMapper;
import org.elasticsearch.index.mapper.geo.BaseGeoPointFieldMapper;
import org.elasticsearch.script.ClassPermission;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Provides the infrastructure for Lucene expressions as a scripting language for Elasticsearch.  Only
 * {@link SearchScript}s are supported.
 */
public class ExpressionScriptEngineService extends AbstractComponent implements ScriptEngineService {

    public static final String NAME = "expression";

    public ExpressionScriptEngineService(Settings settings) {
        super(settings);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public String getExtension() {
        return NAME;
    }

    @Override
    public Object compile(String scriptName, String scriptSource, Map<String, String> params) {
        // classloader created here
        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        return AccessController.doPrivileged(new PrivilegedAction<Expression>() {
            @Override
            public Expression run() {
                try {
                    // snapshot our context here, we check on behalf of the expression
                    AccessControlContext engineContext = AccessController.getContext();
                    ClassLoader loader = getClass().getClassLoader();
                    if (sm != null) {
                        loader = new ClassLoader(loader) {
                            @Override
                            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                                try {
                                    engineContext.checkPermission(new ClassPermission(name));
                                } catch (SecurityException e) {
                                    throw new ClassNotFoundException(name, e);
                                }
                                return super.loadClass(name, resolve);
                            }
                        };
                    }
                    // NOTE: validation is delayed to allow runtime vars, and we don't have access to per index stuff here
                    return JavascriptCompiler.compile(scriptSource, JavascriptCompiler.DEFAULT_FUNCTIONS, loader);
                } catch (ParseException e) {
                    throw convertToScriptException("compile error", scriptSource, scriptSource, e);
                }
            }
        });
    }

    @Override
    public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        Expression expr = (Expression)compiledScript.compiled();
        MapperService mapper = lookup.doc().mapperService();
        // NOTE: if we need to do anything complicated with bindings in the future, we can just extend Bindings,
        // instead of complicating SimpleBindings (which should stay simple)
        SimpleBindings bindings = new SimpleBindings();
        ReplaceableConstValueSource specialValue = null;

        for (String variable : expr.variables) {
            try {
                if (variable.equals("_score")) {
                    bindings.add(new SortField("_score", SortField.Type.SCORE));
                } else if (variable.equals("_value")) {
                    specialValue = new ReplaceableConstValueSource();
                    bindings.add("_value", specialValue);
                    // noop: _value is special for aggregations, and is handled in ExpressionScriptBindings
                    // TODO: if some uses it in a scoring expression, they will get a nasty failure when evaluating...need a
                    // way to know this is for aggregations and so _value is ok to have...

                } else if (vars != null && vars.containsKey(variable)) {
                    // TODO: document and/or error if vars contains _score?
                    // NOTE: by checking for the variable in vars first, it allows masking document fields with a global constant,
                    // but if we were to reverse it, we could provide a way to supply dynamic defaults for documents missing the field?
                    Object value = vars.get(variable);
                    if (value instanceof Number) {
                        bindings.add(variable, new DoubleConstValueSource(((Number) value).doubleValue()));
                    } else {
                        throw new ParseException("Parameter [" + variable + "] must be a numeric type", 0);
                    }

                } else {
                    String fieldname = null;
                    String methodname = null;
                    String variablename = "value"; // .value is the default for doc['field'], its optional.
                    boolean dateAccessor = false; // true if the variable is of type doc['field'].date.xxx
                    VariableContext[] parts = VariableContext.parse(variable);
                    if (parts[0].text.equals("doc") == false) {
                        throw new ParseException("Unknown variable [" + parts[0].text + "]", 0);
                    }
                    if (parts.length < 2 || parts[1].type != VariableContext.Type.STR_INDEX) {
                        throw new ParseException("Variable 'doc' must be used with a specific field like: doc['myfield']", 3);
                    } else {
                        fieldname = parts[1].text;
                    }
                    if (parts.length == 3) {
                        if (parts[2].type == VariableContext.Type.METHOD) {
                            methodname = parts[2].text;
                        } else if (parts[2].type == VariableContext.Type.MEMBER) {
                            variablename = parts[2].text;
                        } else {
                            throw new IllegalArgumentException("Only member variables or member methods may be accessed on a field when not accessing the field directly");
                        }
                    }
                    if (parts.length > 3) {
                        // access to the .date "object" within the field
                        if (parts.length == 4 && ("date".equals(parts[2].text) || "getDate".equals(parts[2].text))) {
                            if (parts[3].type == VariableContext.Type.METHOD) {
                                methodname = parts[3].text;
                                dateAccessor = true;
                            } else if (parts[3].type == VariableContext.Type.MEMBER) {
                                variablename = parts[3].text;
                                dateAccessor = true;
                            }
                        }
                        if (!dateAccessor) {
                            throw new IllegalArgumentException("Variable [" + variable + "] does not follow an allowed format of either doc['field'] or doc['field'].method()");
                        }
                    }

                    MappedFieldType fieldType = mapper.fullName(fieldname);

                    if (fieldType == null) {
                        throw new ParseException("Field [" + fieldname + "] does not exist in mappings", 5);
                    }

                    IndexFieldData<?> fieldData = lookup.doc().fieldDataService().getForField(fieldType);

                    // delegate valuesource creation based on field's type
                    // there are three types of "fields" to expressions, and each one has a different "api" of variables and methods.

                    final ValueSource valueSource;
                    if (fieldType instanceof BaseGeoPointFieldMapper.GeoPointFieldType) {
                        // geo
                        if (methodname == null) {
                            valueSource = GeoField.getVariable(fieldData, fieldname, variablename);
                        } else {
                            valueSource = GeoField.getMethod(fieldData, fieldname, methodname);
                        }
                    } else if (fieldType instanceof LegacyDateFieldMapper.DateFieldType ||
                            fieldType instanceof DateFieldMapper.DateFieldType) {
                        if (dateAccessor) {
                            // date object
                            if (methodname == null) {
                                valueSource = DateObject.getVariable(fieldData, fieldname, variablename);
                            } else {
                                valueSource = DateObject.getMethod(fieldData, fieldname, methodname);
                            }
                        } else {
                            // date field itself
                            if (methodname == null) {
                                valueSource = DateField.getVariable(fieldData, fieldname, variablename);
                            } else {
                                valueSource = DateField.getMethod(fieldData, fieldname, methodname);
                            }
                        }
                    } else if (fieldData instanceof IndexNumericFieldData) {
                        // number
                        if (methodname == null) {
                            valueSource = NumericField.getVariable(fieldData, fieldname, variablename);
                        } else {
                            valueSource = NumericField.getMethod(fieldData, fieldname, methodname);
                        }
                    } else {
                        throw new ParseException("Field [" + fieldname + "] must be numeric, date, or geopoint", 5);
                    }

                    bindings.add(variable, valueSource);
                }
            } catch (Exception e) {
                // we defer "binding" of variables until here: give context for that variable
                throw convertToScriptException("link error", expr.sourceText, variable, e);
            }
        }

        final boolean needsScores = expr.getSortField(bindings, false).needsScores();
        return new ExpressionSearchScript(compiledScript, bindings, specialValue, needsScores);
    }

    /**
     * converts a ParseException at compile-time or link-time to a ScriptException
     */
    private ScriptException convertToScriptException(String message, String source, String portion, Throwable cause) {
        List<String> stack = new ArrayList<>();
        stack.add(portion);
        StringBuilder pointer = new StringBuilder();
        if (cause instanceof ParseException) {
            int offset = ((ParseException) cause).getErrorOffset();
            for (int i = 0; i < offset; i++) {
                pointer.append(' ');
            }
        }
        pointer.append("^---- HERE");
        stack.add(pointer.toString());
        throw new ScriptException(message, cause, stack, source, NAME);
    }

    @Override
    public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> vars) {
        return new ExpressionExecutableScript(compiledScript, vars);
    }

    @Override
    public void close() {}

    @Override
    public void scriptRemoved(CompiledScript script) {
        // Nothing to do
    }

    @Override
    public boolean isInlineScriptEnabled() {
        return true;
    }
}

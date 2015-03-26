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
import org.apache.lucene.queries.function.valuesource.DoubleConstValueSource;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.text.ParseException;
import java.util.Map;

/**
 * Provides the infrastructure for Lucene expressions as a scripting language for Elasticsearch.  Only
 * {@link SearchScript}s are supported.
 */
public class ExpressionScriptEngineService extends AbstractComponent implements ScriptEngineService {

    public static final String NAME = "expression";

    @Inject
    public ExpressionScriptEngineService(Settings settings) {
        super(settings);
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
        return true;
    }

    @Override
    public Object compile(String script) {
        try {
            // NOTE: validation is delayed to allow runtime vars, and we don't have access to per index stuff here
            return JavascriptCompiler.compile(script);
        } catch (ParseException e) {
            throw new ExpressionScriptCompilationException("Failed to parse expression: " + script, e);
        }
    }

    @Override
    public SearchScript search(Object compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        Expression expr = (Expression)compiledScript;
        MapperService mapper = lookup.doc().mapperService();
        // NOTE: if we need to do anything complicated with bindings in the future, we can just extend Bindings,
        // instead of complicating SimpleBindings (which should stay simple)
        SimpleBindings bindings = new SimpleBindings();
        ReplaceableConstValueSource specialValue = null;

        for (String variable : expr.variables) {
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
                    bindings.add(variable, new DoubleConstValueSource(((Number)value).doubleValue()));
                } else {
                    throw new ExpressionScriptCompilationException("Parameter [" + variable + "] must be a numeric type");
                }

            } else {
                VariableContext[] parts = VariableContext.parse(variable);
                if (parts[0].text.equals("doc") == false) {
                    throw new ExpressionScriptCompilationException("Unknown variable [" + parts[0].text + "] in expression");
                }
                if (parts.length < 2 || parts[1].type != VariableContext.Type.STR_INDEX) {
                    throw new ExpressionScriptCompilationException("Variable 'doc' in expression must be used with a specific field like: doc['myfield'].value");
                }
                if (parts.length < 3 || parts[2].type != VariableContext.Type.MEMBER || parts[2].text.equals("value") == false) {
                    throw new ExpressionScriptCompilationException("Invalid member for field data in expression.  Only '.value' is currently supported.");
                }
                String fieldname = parts[1].text;

                FieldMapper<?> field = mapper.smartNameFieldMapper(fieldname);
                if (field == null) {
                    throw new ExpressionScriptCompilationException("Field [" + fieldname + "] used in expression does not exist in mappings");
                }
                if (field.isNumeric() == false) {
                    // TODO: more context (which expression?)
                    throw new ExpressionScriptCompilationException("Field [" + fieldname + "] used in expression must be numeric");
                }
                IndexFieldData<?> fieldData = lookup.doc().fieldDataService().getForField((NumberFieldMapper)field);
                bindings.add(variable, new FieldDataValueSource(fieldData));
            }
        }

        return new ExpressionScript((Expression)compiledScript, bindings, specialValue);
    }

    @Override
    public ExecutableScript executable(Object compiledScript, @Nullable Map<String, Object> vars) {
        throw new UnsupportedOperationException("Cannot use expressions for updates");
    }

    @Override
    public Object execute(Object compiledScript, Map<String, Object> vars) {
        throw new UnsupportedOperationException("Cannot use expressions for updates");
    }

    @Override
    public Object unwrap(Object value) {
        return value;
    }

    @Override
    public void close() {}

    @Override
    public void scriptRemoved(CompiledScript script) {
        // Nothing to do
    }
}

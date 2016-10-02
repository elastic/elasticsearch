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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.expressions.Expression;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.GeneralScriptException;

/**
 * A bridge to evaluate an {@link Expression} against a map of variables in the context
 * of an {@link ExecutableScript}.
 */
public class ExpressionExecutableScript implements ExecutableScript {

    private final int NO_DOCUMENT = -1;

    public final CompiledScript compiledScript;
    public final Map<String, ReplaceableConstFunctionValues> functionValuesMap;
    public final ReplaceableConstFunctionValues[] functionValuesArray;

    public ExpressionExecutableScript(CompiledScript compiledScript, Map<String, Object> vars) {
        this.compiledScript = compiledScript;
        Expression expression = (Expression)this.compiledScript.compiled();
        int functionValuesLength = expression.variables.length;

        if (vars.size() != functionValuesLength) {
            throw new GeneralScriptException("Error using " + compiledScript + ". " +
                    "The number of variables in an executable expression script [" +
                    functionValuesLength + "] must match the number of variables in the variable map" +
                    " [" + vars.size() + "].");
        }

        functionValuesArray = new ReplaceableConstFunctionValues[functionValuesLength];
        functionValuesMap = new HashMap<>();

        for (int functionValuesIndex = 0; functionValuesIndex < functionValuesLength; ++functionValuesIndex) {
            String variableName = expression.variables[functionValuesIndex];
            functionValuesArray[functionValuesIndex] = new ReplaceableConstFunctionValues();
            functionValuesMap.put(variableName, functionValuesArray[functionValuesIndex]);
        }

        for (String varsName : vars.keySet()) {
            setNextVar(varsName, vars.get(varsName));
        }
    }

    @Override
    public void setNextVar(String name, Object value) {
        if (functionValuesMap.containsKey(name)) {
            if (value instanceof Number) {
                double doubleValue = ((Number)value).doubleValue();
                functionValuesMap.get(name).setValue(doubleValue);
            } else {
                throw new GeneralScriptException("Error using " + compiledScript + ". " +
                        "Executable expressions scripts can only process numbers." +
                        "  The variable [" + name + "] is not a number.");
            }
        } else {
            throw new GeneralScriptException("Error using " + compiledScript + ". " +
                    "The variable [" + name + "] does not exist in the executable expressions script.");
        }
    }

    @Override
    public Object run() {
        try {
            return ((Expression) compiledScript.compiled()).evaluate(NO_DOCUMENT, functionValuesArray);
        } catch (Exception exception) {
            throw new GeneralScriptException("Error evaluating " + compiledScript, exception);
        }
    }
}

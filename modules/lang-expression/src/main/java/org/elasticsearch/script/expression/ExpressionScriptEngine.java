/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.expressions.js.VariableContext;
import org.apache.lucene.search.DoubleValuesSource;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper.GeoPointFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.BucketAggregationScript;
import org.elasticsearch.script.BucketAggregationSelectorScript;
import org.elasticsearch.script.ClassPermission;
import org.elasticsearch.script.DoubleValuesScript;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.NumberSortScript;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.TermsSetQueryScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Provides the infrastructure for Lucene expressions as a scripting language for Elasticsearch.
 *
 * Only contexts returning numeric types or {@link Object} are supported.
 */
public class ExpressionScriptEngine implements ScriptEngine {

    public static final String NAME = "expression";

    private static Map<ScriptContext<?>, Function<Expression, Object>> contexts = Map.of(
        BucketAggregationScript.CONTEXT,
        ExpressionScriptEngine::newBucketAggregationScriptFactory,

        BucketAggregationSelectorScript.CONTEXT,
        (Expression expr) -> {
            BucketAggregationScript.Factory factory = newBucketAggregationScriptFactory(expr);
            BucketAggregationSelectorScript.Factory wrappedFactory = parameters -> new BucketAggregationSelectorScript(parameters) {
                @Override
                public boolean execute() {
                    return factory.newInstance(getParams()).execute().doubleValue() == 1.0;
                }
            };
            return wrappedFactory;
        },

        FilterScript.CONTEXT,
        (Expression expr) -> new FilterScript.Factory() {
            @Override
            public boolean isResultDeterministic() {
                return true;
            }

            @Override
            public FilterScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
                return newFilterScript(expr, lookup, params);
            }
        },

        ScoreScript.CONTEXT,
        (Expression expr) -> new ScoreScript.Factory() {
            @Override
            public ScoreScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
                return newScoreScript(expr, lookup, params);
            }

            @Override
            public boolean isResultDeterministic() {
                return true;
            }
        },

        TermsSetQueryScript.CONTEXT,
        (Expression expr) -> (TermsSetQueryScript.Factory) (p, lookup) -> newTermsSetQueryScript(expr, lookup, p),

        AggregationScript.CONTEXT,
        (Expression expr) -> new AggregationScript.Factory() {
            @Override
            public AggregationScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
                return newAggregationScript(expr, lookup, params);
            }

            @Override
            public boolean isResultDeterministic() {
                return true;
            }
        },

        NumberSortScript.CONTEXT,
        (Expression expr) -> new NumberSortScript.Factory() {
            @Override
            public NumberSortScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
                return newSortScript(expr, lookup, params);
            }

            @Override
            public boolean isResultDeterministic() {
                return true;
            }
        },

        FieldScript.CONTEXT,
        (Expression expr) -> new FieldScript.Factory() {
            @Override
            public FieldScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
                return newFieldScript(expr, lookup, params);
            }

            @Override
            public boolean isResultDeterministic() {
                return true;
            }
        },

        DoubleValuesScript.CONTEXT,
        (Expression expr) -> new ExpressionDoubleValuesScript(expr) {
            @Override
            public boolean isResultDeterministic() {
                return true;
            }
        }
    );

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public <T> T compile(String scriptName, String scriptSource, ScriptContext<T> context, Map<String, String> params) {
        // classloader created here
        final SecurityManager sm = System.getSecurityManager();
        SpecialPermission.check();
        Expression expr = AccessController.doPrivileged(new PrivilegedAction<Expression>() {
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
        if (contexts.containsKey(context) == false) {
            throw new IllegalArgumentException("expression engine does not know how to handle script context [" + context.name + "]");
        }
        return context.factoryClazz.cast(contexts.get(context).apply(expr));
    }

    @Override
    public Set<ScriptContext<?>> getSupportedContexts() {
        return contexts.keySet();
    }

    private static BucketAggregationScript.Factory newBucketAggregationScriptFactory(Expression expr) {
        return parameters -> {
            ReplaceableConstDoubleValues[] functionValuesArray = new ReplaceableConstDoubleValues[expr.variables.length];
            Map<String, ReplaceableConstDoubleValues> functionValuesMap = new HashMap<>();
            for (int i = 0; i < expr.variables.length; ++i) {
                functionValuesArray[i] = new ReplaceableConstDoubleValues();
                functionValuesMap.put(expr.variables[i], functionValuesArray[i]);
            }
            return new BucketAggregationScript(parameters) {
                @Override
                public Double execute() {
                    getParams().forEach((name, value) -> {
                        ReplaceableConstDoubleValues placeholder = functionValuesMap.get(name);
                        if (placeholder == null) {
                            throw new IllegalArgumentException(
                                "Error using "
                                    + expr
                                    + ". "
                                    + "The variable ["
                                    + name
                                    + "] does not exist in the executable expressions script."
                            );
                        } else if (value instanceof Number == false) {
                            throw new IllegalArgumentException(
                                "Error using "
                                    + expr
                                    + ". "
                                    + "Executable expressions scripts can only process numbers."
                                    + "  The variable ["
                                    + name
                                    + "] is not a number."
                            );
                        } else {
                            placeholder.setValue(((Number) value).doubleValue());
                        }
                    });
                    return expr.evaluate(functionValuesArray);
                }
            };
        };
    }

    private static NumberSortScript.LeafFactory newSortScript(Expression expr, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        // NOTE: if we need to do anything complicated with bindings in the future, we can just extend Bindings,
        // instead of complicating SimpleBindings (which should stay simple)
        SimpleBindings bindings = new SimpleBindings();
        boolean needsScores = false;
        for (String variable : expr.variables) {
            try {
                if (variable.equals("_score")) {
                    bindings.add("_score", DoubleValuesSource.SCORES);
                    needsScores = true;
                } else if (vars != null && vars.containsKey(variable)) {
                    bindFromParams(vars, bindings, variable);
                } else {
                    // delegate valuesource creation based on field's type
                    // there are three types of "fields" to expressions, and each one has a different "api" of variables and methods.
                    final DoubleValuesSource valueSource = getDocValueSource(variable, lookup);
                    needsScores |= valueSource.needsScores();
                    bindings.add(variable, valueSource);
                }
            } catch (Exception e) {
                // we defer "binding" of variables until here: give context for that variable
                throw convertToScriptException("link error", expr.sourceText, variable, e);
            }
        }
        return new ExpressionNumberSortScript(expr, bindings, needsScores);
    }

    private static TermsSetQueryScript.LeafFactory newTermsSetQueryScript(
        Expression expr,
        SearchLookup lookup,
        @Nullable Map<String, Object> vars
    ) {
        // NOTE: if we need to do anything complicated with bindings in the future, we can just extend Bindings,
        // instead of complicating SimpleBindings (which should stay simple)
        SimpleBindings bindings = new SimpleBindings();
        for (String variable : expr.variables) {
            try {
                if (vars != null && vars.containsKey(variable)) {
                    bindFromParams(vars, bindings, variable);
                } else {
                    // delegate valuesource creation based on field's type
                    // there are three types of "fields" to expressions, and each one has a different "api" of variables and methods.
                    bindings.add(variable, getDocValueSource(variable, lookup));
                }
            } catch (Exception e) {
                // we defer "binding" of variables until here: give context for that variable
                throw convertToScriptException("link error", expr.sourceText, variable, e);
            }
        }
        return new ExpressionTermSetQueryScript(expr, bindings);
    }

    private static AggregationScript.LeafFactory newAggregationScript(
        Expression expr,
        SearchLookup lookup,
        @Nullable Map<String, Object> vars
    ) {
        // NOTE: if we need to do anything complicated with bindings in the future, we can just extend Bindings,
        // instead of complicating SimpleBindings (which should stay simple)
        SimpleBindings bindings = new SimpleBindings();
        boolean needsScores = false;
        ReplaceableConstDoubleValueSource specialValue = null;
        for (String variable : expr.variables) {
            try {
                if (variable.equals("_score")) {
                    bindings.add("_score", DoubleValuesSource.SCORES);
                    needsScores = true;
                } else if (variable.equals("_value")) {
                    specialValue = new ReplaceableConstDoubleValueSource();
                    bindings.add("_value", specialValue);
                    // noop: _value is special for aggregations, and is handled in ExpressionScriptBindings
                    // TODO: if some uses it in a scoring expression, they will get a nasty failure when evaluating...need a
                    // way to know this is for aggregations and so _value is ok to have...

                } else if (vars != null && vars.containsKey(variable)) {
                    bindFromParams(vars, bindings, variable);
                } else {
                    // delegate valuesource creation based on field's type
                    // there are three types of "fields" to expressions, and each one has a different "api" of variables and methods.
                    final DoubleValuesSource valueSource = getDocValueSource(variable, lookup);
                    needsScores |= valueSource.needsScores();
                    bindings.add(variable, valueSource);
                }
            } catch (Exception e) {
                // we defer "binding" of variables until here: give context for that variable
                throw convertToScriptException("link error", expr.sourceText, variable, e);
            }
        }
        return new ExpressionAggregationScript(expr, bindings, needsScores, specialValue);
    }

    private static FieldScript.LeafFactory newFieldScript(Expression expr, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        SimpleBindings bindings = new SimpleBindings();
        for (String variable : expr.variables) {
            try {
                if (vars != null && vars.containsKey(variable)) {
                    bindFromParams(vars, bindings, variable);
                } else {
                    bindings.add(variable, getDocValueSource(variable, lookup));
                }
            } catch (Exception e) {
                throw convertToScriptException("link error", expr.sourceText, variable, e);
            }
        }
        return new ExpressionFieldScript(expr, bindings);
    }

    /**
     * This is a hack for filter scripts, which must return booleans instead of doubles as expression do.
     * See https://github.com/elastic/elasticsearch/issues/26429.
     */
    private static FilterScript.LeafFactory newFilterScript(Expression expr, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        ScoreScript.LeafFactory searchLeafFactory = newScoreScript(expr, lookup, vars);
        return docReader -> {
            ScoreScript script = searchLeafFactory.newInstance(docReader);
            return new FilterScript(vars, lookup, docReader) {
                @Override
                public boolean execute() {
                    return script.execute(null) != 0.0;
                }

                @Override
                public void setDocument(int docid) {
                    script.setDocument(docid);
                }
            };
        };
    }

    private static ScoreScript.LeafFactory newScoreScript(Expression expr, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        // NOTE: if we need to do anything complicated with bindings in the future, we can just extend Bindings,
        // instead of complicating SimpleBindings (which should stay simple)
        SimpleBindings bindings = new SimpleBindings();
        boolean needsScores = false;
        for (String variable : expr.variables) {
            try {
                if (variable.equals("_score")) {
                    bindings.add("_score", DoubleValuesSource.SCORES);
                    needsScores = true;
                } else if (variable.equals("_value")) {
                    bindings.add("_value", DoubleValuesSource.constant(0));
                    // noop: _value is special for aggregations, and is handled in ExpressionScriptBindings
                    // TODO: if some uses it in a scoring expression, they will get a nasty failure when evaluating...need a
                    // way to know this is for aggregations and so _value is ok to have...
                } else if (vars != null && vars.containsKey(variable)) {
                    bindFromParams(vars, bindings, variable);
                } else {
                    // delegate valuesource creation based on field's type
                    // there are three types of "fields" to expressions, and each one has a different "api" of variables and methods.
                    final DoubleValuesSource valueSource = getDocValueSource(variable, lookup);
                    needsScores |= valueSource.needsScores();
                    bindings.add(variable, valueSource);
                }
            } catch (Exception e) {
                // we defer "binding" of variables until here: give context for that variable
                throw convertToScriptException("link error", expr.sourceText, variable, e);
            }
        }
        return new ExpressionScoreScript(expr, bindings, needsScores);
    }

    /**
     * converts a ParseException at compile-time or link-time to a ScriptException
     */
    private static ScriptException convertToScriptException(String message, String source, String portion, Throwable cause) {
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

    private static DoubleValuesSource getDocValueSource(String variable, SearchLookup lookup) throws ParseException {
        VariableContext[] parts = VariableContext.parse(variable);
        if (parts[0].text.equals("doc") == false) {
            throw new ParseException("Unknown variable [" + parts[0].text + "]", 0);
        }
        if (parts.length < 2 || parts[1].type != VariableContext.Type.STR_INDEX) {
            throw new ParseException("Variable 'doc' must be used with a specific field like: doc['myfield']", 3);
        }

        // .value is the default for doc['field'], its optional.
        String variablename = "value";
        String methodname = null;
        if (parts.length == 3) {
            if (parts[2].type == VariableContext.Type.METHOD) {
                methodname = parts[2].text;
            } else if (parts[2].type == VariableContext.Type.MEMBER) {
                variablename = parts[2].text;
            } else {
                throw new IllegalArgumentException(
                    "Only member variables or member methods may be accessed on a field when not accessing the field directly"
                );
            }
        }
        // true if the variable is of type doc['field'].date.xxx
        boolean dateAccessor = false;
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
            if (dateAccessor == false) {
                throw new IllegalArgumentException(
                    "Variable [" + variable + "] does not follow an allowed format of either doc['field'] or doc['field'].method()"
                );
            }
        }

        String fieldname = parts[1].text;
        MappedFieldType fieldType = lookup.fieldType(fieldname);

        if (fieldType == null) {
            throw new ParseException("Field [" + fieldname + "] does not exist in mappings", 5);
        }

        IndexFieldData<?> fieldData = lookup.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
        final DoubleValuesSource valueSource;
        if (fieldType instanceof GeoPointFieldType) {
            // geo
            if (methodname == null) {
                valueSource = GeoField.getVariable(fieldData, fieldname, variablename);
            } else {
                valueSource = GeoField.getMethod(fieldData, fieldname, methodname);
            }
        } else if (fieldType instanceof DateFieldMapper.DateFieldType) {
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
        return valueSource;
    }

    // TODO: document and/or error if params contains _score?
    // NOTE: by checking for the variable in params first, it allows masking document fields with a global constant,
    // but if we were to reverse it, we could provide a way to supply dynamic defaults for documents missing the field?
    private static void bindFromParams(@Nullable final Map<String, Object> params, final SimpleBindings bindings, final String variable)
        throws ParseException {
        // NOTE: by checking for the variable in vars first, it allows masking document fields with a global constant,
        // but if we were to reverse it, we could provide a way to supply dynamic defaults for documents missing the field?
        Object value = params.get(variable);
        if (value instanceof Number) {
            bindings.add(variable, DoubleValuesSource.constant(((Number) value).doubleValue()));
        } else {
            throw new ParseException("Parameter [" + variable + "] must be a numeric type", 0);
        }
    }
}

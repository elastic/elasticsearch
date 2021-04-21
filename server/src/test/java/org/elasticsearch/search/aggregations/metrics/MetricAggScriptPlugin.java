/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.test.ESTestCase;

/**
 * Provides a number of dummy scripts for tests.
 *
 * Each script provided allows for an {@code inc} parameter which will
 * be added to each value read from a document.
 */
public class MetricAggScriptPlugin extends MockScriptPlugin {

    /** The name of the script engine type this plugin provides. */
    public static final String METRIC_SCRIPT_ENGINE = "metric_scripts";

    /** Script to take a field name in params and sum the values of the field. */
    public static final String SUM_FIELD_PARAMS_SCRIPT = "sum_field_params";

    /** Script to sum the values of a field named {@code values}. */
    public static final String SUM_VALUES_FIELD_SCRIPT = "sum_values_field";

    /** Script to return the value of a field named {@code value}. */
    public static final String VALUE_FIELD_SCRIPT = "value_field";

    /** Script to return the {@code _value} provided by aggs framework. */
    public static final String VALUE_SCRIPT = "_value";

    /** Script to return a random double */
    public static final String RANDOM_SCRIPT = "Math.random()";

    @Override
    public String pluginScriptLang() {
        return METRIC_SCRIPT_ENGINE;
    }

    @Override
    protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        Function<Map<String, Object>, Integer> getInc = vars -> {
            if (vars == null || vars.containsKey("inc") == false) {
                return 0;
            } else {
                return ((Number) vars.get("inc")).intValue();
            }
        };
        BiFunction<Map<String, Object>, String, Object> sum = (vars, fieldname) -> {
            int inc = getInc.apply(vars);
            LeafDocLookup docLookup = (LeafDocLookup) vars.get("doc");
            List<Long> values = new ArrayList<>();
            for (Object v : docLookup.get(fieldname)) {
                values.add(((Number) v).longValue() + inc);
            }
            return values;
        };
        scripts.put(SUM_FIELD_PARAMS_SCRIPT, vars -> {
            String fieldname = (String) vars.get("field");
            return sum.apply(vars, fieldname);
        });
        scripts.put(SUM_VALUES_FIELD_SCRIPT, vars -> sum.apply(vars, "values"));
        scripts.put(VALUE_FIELD_SCRIPT, vars -> sum.apply(vars, "value"));
        scripts.put(VALUE_SCRIPT, vars -> {
            int inc = getInc.apply(vars);
            return ((Number) vars.get("_value")).doubleValue() + inc;
        });
        return scripts;
    }

    @Override
    protected Map<String, Function<Map<String, Object>, Object>> nonDeterministicPluginScripts() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

        scripts.put("Math.random()", vars -> ESTestCase.randomDouble());

        return scripts;
    }
}

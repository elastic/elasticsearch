/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import static java.util.Collections.emptyMap;

/**
 * Static method aliases for constructors of known {@link ScoreFunctionBuilder}s.
 */
public class ScoreFunctionBuilders {

    public static ExponentialDecayFunctionBuilder exponentialDecayFunction(String fieldName, Object origin, Object scale) {
        return new ExponentialDecayFunctionBuilder(fieldName, origin, scale, null);
    }

    public static ExponentialDecayFunctionBuilder exponentialDecayFunction(String fieldName, Object origin, Object scale, Object offset) {
        return new ExponentialDecayFunctionBuilder(fieldName, origin, scale, offset);
    }

    public static ExponentialDecayFunctionBuilder exponentialDecayFunction(
        String fieldName,
        Object origin,
        Object scale,
        Object offset,
        double decay
    ) {
        return new ExponentialDecayFunctionBuilder(fieldName, origin, scale, offset, decay);
    }

    public static GaussDecayFunctionBuilder gaussDecayFunction(String fieldName, Object origin, Object scale) {
        return new GaussDecayFunctionBuilder(fieldName, origin, scale, null);
    }

    public static GaussDecayFunctionBuilder gaussDecayFunction(String fieldName, Object origin, Object scale, Object offset) {
        return new GaussDecayFunctionBuilder(fieldName, origin, scale, offset);
    }

    public static GaussDecayFunctionBuilder gaussDecayFunction(String fieldName, Object origin, Object scale, Object offset, double decay) {
        return new GaussDecayFunctionBuilder(fieldName, origin, scale, offset, decay);
    }

    public static LinearDecayFunctionBuilder linearDecayFunction(String fieldName, Object origin, Object scale) {
        return new LinearDecayFunctionBuilder(fieldName, origin, scale, null);
    }

    public static LinearDecayFunctionBuilder linearDecayFunction(String fieldName, Object origin, Object scale, Object offset) {
        return new LinearDecayFunctionBuilder(fieldName, origin, scale, offset);
    }

    public static LinearDecayFunctionBuilder linearDecayFunction(
        String fieldName,
        Object origin,
        Object scale,
        Object offset,
        double decay
    ) {
        return new LinearDecayFunctionBuilder(fieldName, origin, scale, offset, decay);
    }

    public static ScriptScoreFunctionBuilder scriptFunction(Script script) {
        return (new ScriptScoreFunctionBuilder(script));
    }

    public static ScriptScoreFunctionBuilder scriptFunction(String script) {
        return (new ScriptScoreFunctionBuilder(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, script, emptyMap())));
    }

    public static RandomScoreFunctionBuilder randomFunction() {
        return new RandomScoreFunctionBuilder();
    }

    public static WeightBuilder weightFactorFunction(float weight) {
        return new WeightBuilder().setWeight(weight);
    }

    public static FieldValueFactorFunctionBuilder fieldValueFactorFunction(String fieldName) {
        return new FieldValueFactorFunctionBuilder(fieldName);
    }
}

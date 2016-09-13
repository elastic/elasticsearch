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

package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.script.Script;

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

    public static ExponentialDecayFunctionBuilder exponentialDecayFunction(String fieldName, Object origin, Object scale, Object offset,
            double decay) {
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

    public static LinearDecayFunctionBuilder linearDecayFunction(String fieldName, Object origin, Object scale, Object offset,
            double decay) {
        return new LinearDecayFunctionBuilder(fieldName, origin, scale, offset, decay);
    }

    public static ScriptScoreFunctionBuilder scriptFunction(Script script) {
        return (new ScriptScoreFunctionBuilder(script));
    }

    public static ScriptScoreFunctionBuilder scriptFunction(String script) {
        return (new ScriptScoreFunctionBuilder(new Script(script)));
    }

    public static RandomScoreFunctionBuilder randomFunction(int seed) {
        return (new RandomScoreFunctionBuilder()).seed(seed);
    }

    public static RandomScoreFunctionBuilder randomFunction(long seed) {
        return (new RandomScoreFunctionBuilder()).seed(seed);
    }

    public static RandomScoreFunctionBuilder randomFunction(String seed) {
        return (new RandomScoreFunctionBuilder()).seed(seed);
    }
    
    public static WeightBuilder weightFactorFunction(float weight) {
        return (WeightBuilder)(new WeightBuilder().setWeight(weight));
    }

    public static FieldValueFactorFunctionBuilder fieldValueFactorFunction(String fieldName) {
        return new FieldValueFactorFunctionBuilder(fieldName);
    }
}

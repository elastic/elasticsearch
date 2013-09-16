/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.index.query.functionscore.exp.ExponentialDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.factor.FactorBuilder;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.lin.LinearDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.random.RandomScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionBuilder;

import java.util.Map;

public class ScoreFunctionBuilders {
   
    public static ExponentialDecayFunctionBuilder exponentialDecayFunction(String fieldName, Object origin, Object scale) {
        return new ExponentialDecayFunctionBuilder(fieldName, origin, scale);
    }
    
    public static ExponentialDecayFunctionBuilder exponentialDecayFunction(String fieldName, Object scale) {
        return new ExponentialDecayFunctionBuilder(fieldName, null, scale);
    }
    
    public static GaussDecayFunctionBuilder gaussDecayFunction(String fieldName, Object origin, Object scale) {
        return new GaussDecayFunctionBuilder(fieldName, origin, scale);
    }
    
    public static GaussDecayFunctionBuilder gaussDecayFunction(String fieldName, Object scale) {
        return new GaussDecayFunctionBuilder(fieldName, null, scale);
    }
    
    public static LinearDecayFunctionBuilder linearDecayFunction(String fieldName, Object origin, Object scale) {
        return new LinearDecayFunctionBuilder(fieldName, origin, scale);
    }
    
    public static LinearDecayFunctionBuilder linearDecayFunction(String fieldName, Object scale) {
        return new LinearDecayFunctionBuilder(fieldName, null, scale);
    }

    public static ScriptScoreFunctionBuilder scriptFunction(String script) {
        return (new ScriptScoreFunctionBuilder()).script(script);
    }

    public static ScriptScoreFunctionBuilder scriptFunction(String script, String lang) {
        return (new ScriptScoreFunctionBuilder()).script(script).lang(lang);
    }

    public static ScriptScoreFunctionBuilder scriptFunction(String script, String lang, Map<String, Object> params) {
        return (new ScriptScoreFunctionBuilder()).script(script).lang(lang).params(params);
    }

    public static ScriptScoreFunctionBuilder scriptFunction(String script, Map<String, Object> params) {
        return (new ScriptScoreFunctionBuilder()).script(script).params(params);
    }

    public static FactorBuilder factorFunction(float boost) {
        return (new FactorBuilder()).boostFactor(boost);
    }

    public static RandomScoreFunctionBuilder randomFunction(long seed) {
        return (new RandomScoreFunctionBuilder()).seed(seed);
    }
}

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

package org.elasticsearch.action.bench;

import org.elasticsearch.action.quality.PrecisionAtN;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This class represents a registraion point for all evaluators that can be used within the benchmark framework.
 * 
 * Each evaluator is registered under a unique id for (de-)serialisation.
 * */
@SuppressWarnings("rawtypes")
public class BenchEvaluators {
    private static final Map<Integer, Evaluator> mapping = new HashMap<Integer, Evaluator>();
    private static final Map<String, Integer> reverse = new HashMap<String, Integer>();

    static {
        mapping.put(0, new PrecisionAtN());
        
        for (Entry<Integer, Evaluator> entry : mapping.entrySet()) {
            reverse.put(entry.getValue().getClass().getName(), entry.getKey());
        }
    }
    
    public static int getIdForObject(Evaluator eval) {
        return reverse.get(eval.getClass().getName());
    }
    
    public static Evaluator getNewObjectForId(int id) {
        return mapping.get(id).getInstance();
    }
}

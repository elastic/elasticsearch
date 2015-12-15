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

package org.elasticsearch.plan.a;

import org.apache.lucene.search.Scorer;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.ScoreAccessor;
import org.elasticsearch.search.lookup.LeafSearchLookup;

import java.util.HashMap;
import java.util.Map;

final class ScriptImpl implements ExecutableScript, LeafSearchScript {
    final Executable executable;
    final Map<String,Object> variables;
    final LeafSearchLookup lookup;
    
    ScriptImpl(Executable executable, Map<String,Object> vars, LeafSearchLookup lookup) {
        this.executable = executable;
        this.lookup = lookup;
        this.variables = new HashMap<>();
        if (vars != null) {
            variables.putAll(vars);
        }
        if (lookup != null) {
            variables.putAll(lookup.asMap());
        }
    }
    
    @Override
    public void setNextVar(String name, Object value) {
        variables.put(name, value);
    }
    
    @Override
    public Object run() {
        return executable.execute(variables);
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
        return value;
    }

    @Override
    public void setScorer(Scorer scorer) {
        variables.put("_score", new ScoreAccessor(scorer));
    }

    @Override
    public void setDocument(int doc) {
        if (lookup != null) {
            lookup.setDocument(doc);
        }
    }

    @Override
    public void setSource(Map<String,Object> source) {
        if (lookup != null) {
            lookup.source().setSource(source);
        }
    }
}

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

package org.elasticsearch.painless;

import org.elasticsearch.painless.node.SFunction;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Program-wide globals (initializers, synthetic methods, etc)
 */
public class Globals {
    private final Map<String,SFunction> syntheticMethods = new HashMap<>();
    private final Map<String,Constant> constantInitializers = new HashMap<>();
    private final BitSet statements;
    
    /** Create a new Globals from the set of statement boundaries */
    public Globals(BitSet statements) {
        this.statements = statements;
    }
    
    /** Adds a new synthetic method to be written. It must be analyzed! */
    public void addSyntheticMethod(SFunction function) {
        if (!function.synthetic) {
            throw new IllegalStateException("method: " + function.name + " is not synthetic");
        }
        if (syntheticMethods.put(function.name, function) != null) {
            throw new IllegalStateException("synthetic method: " + function.name + " already exists");
        }
    }
    
    /** Adds a new constant initializer to be written */
    public void addConstantInitializer(Constant constant) {
        if (constantInitializers.put(constant.name, constant) != null) {
            throw new IllegalStateException("constant initializer: " + constant.name + " already exists");
        }
    }
    
    /** Returns the current synthetic methods */
    public Map<String,SFunction> getSyntheticMethods() {
        return syntheticMethods;
    }
    
    /** Returns the current initializers */
    public Map<String,Constant> getConstantInitializers() {
        return constantInitializers;
    }
    
    /** Returns the set of statement boundaries */
    public BitSet getStatements() {
        return statements;
    }
}

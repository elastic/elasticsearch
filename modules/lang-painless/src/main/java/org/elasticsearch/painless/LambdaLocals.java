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

import org.elasticsearch.painless.Definition.Type;

import java.util.List;
import java.util.Objects;

/** Extension of locals for lambdas */
// Note: this isn't functional yet, it throws UOE
// TODO: implement slot renumbering for captures.
class LambdaLocals extends Locals {
    private List<Variable> captures;

    LambdaLocals(Locals parent, List<Parameter> parameters, List<Variable> captures) {
        super(parent);
        for (Parameter parameter : parameters) {
            defineVariable(parameter.location, parameter.type, parameter.name, false);
        }
        this.captures = Objects.requireNonNull(captures);
    }
    
    @Override
    public Variable getVariable(Location location, String name) {
        Variable variable = lookupVariable(location, name);
        if (variable != null) {
            return variable;
        }
        if (getParent() != null) {
            variable = getParent().getVariable(location, name);
            if (variable != null) {
                assert captures != null; // unused right now
                // make it read-only, and record that it was used.
                throw new UnsupportedOperationException("lambda capture is not supported");
            }
        }
        throw location.createError(new IllegalArgumentException("Variable [" + name + "] is not defined."));
    }

    @Override
    public Type getReturnType() {
        return Definition.DEF_TYPE;
    }
}

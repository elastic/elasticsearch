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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;

import java.util.Objects;
import java.util.Set;

/**
 * The superclass for all nodes.
 */
public abstract class ANode {

    /**
     * The identifier of the script and character offset used for debugging and errors.
     */
    final Location location;

    /**
     * Standard constructor with location used for error tracking.
     */
    ANode(Location location) {
        this.location = Objects.requireNonNull(location);
    }

    /**
     * Adds all variable names referenced to the variable set.
     * <p>
     * This can be called at any time, e.g. to support lambda capture.
     * @param variables set of variables referenced (any scope)
     */
    abstract void extractVariables(Set<String> variables);

    /**
     * Checks for errors and collects data for the writing phase.
     */
    abstract void analyze(Locals locals);

    /**
     * Writes ASM based on the data collected during the analysis phase.
     */
    abstract void write(MethodWriter writer, Globals globals);

    RuntimeException createError(RuntimeException exception) {
        return location.createError(exception);
    }
}

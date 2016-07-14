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

import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;

/**
 * This is the base class for an expression may store a value in local memory.
 */
public abstract class AStoreable extends AExpression {

    boolean store = false;

    AStoreable(Location location) {
        super(location);
    }

    AStoreable(Location location, AExpression prefix) {
        super(location, prefix);
    }

    /**
     * Returns a value based on this storeable's size on the stack.  This is
     * used during the writing phase to dup stack values from this storeable as
     * necessary during certain store operations.
     */
    abstract int size();

    /**
     * If this node or a sub-node of this node uses dynamic calls then
     * actual will be set to this value.  Returns true if actual was updated.
     * This is used for an optimization during assignment to def type targets.
     */
    abstract boolean updateActual(Type actual);

    /**
     * Called before a storeable node is loaded or stored used to push load/store
     * constants onto the stack if necessary.
     */
    abstract void prestore(MethodWriter writer, Globals globals);

    /**
     * Called to load a storable used for compound assignments.
     */
    abstract void load(MethodWriter writer, Globals globals);

    /**
     * Called to store a storabable to local memory.
     */
    abstract void store(MethodWriter writer, Globals globals);
}

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

import java.util.Objects;

/**
 * The super class for an expression that can store a value in local memory.
 */
abstract class AStoreable extends AExpression {

    /**
     * Set to true when this node is an lhs-expression and will be storing
     * a value from an rhs-expression.
     */
    boolean write = false;

    /**
     * Standard constructor with location used for error tracking.
     */
    AStoreable(Location location) {
        super(location);

        prefix = null;
    }

    /**
     * This constructor is used by variable/method chains when postfixes are specified.
     */
    AStoreable(Location location, AExpression prefix) {
        super(location);

        this.prefix = Objects.requireNonNull(prefix);
    }

    /**
     * Returns a value based on the number of elements previously placed on the
     * stack to load/store a certain piece of a variable/method chain.  This is
     * used during the writing phase to dup stack values from this storeable as
     * necessary during certain store operations.
     * <p>
     * Examples:
     * {@link EVariable} returns 0 because it requires nothing extra to perform
     *                   a load/store
     * {@link PSubField} returns 1 because it requires the name of the field as
     *                   an index on the stack to perform a load/store
     * {@link PSubBrace} returns 2 because it requires both the variable slot and
     *                   an index into the array on the stack to perform a
     *                   load/store
     */
    abstract int accessElementCount();

    /**
     * Returns true if this node or a sub-node of this node can be optimized with
     * rhs actual type to avoid an unnecessary cast.
     */
    abstract boolean isDefOptimized();

    /**
     * If this node or a sub-node of this node uses dynamic calls then
     * actual will be set to this value. This is used for an optimization
     * during assignment to def type targets.
     */
    abstract void updateActual(Type actual);

    /**
     * Called before a storeable node is loaded or stored.  Used to load prefixes and
     * push load/store constants onto the stack if necessary.
     */
    abstract void setup(MethodWriter writer, Globals globals);

    /**
     * Called to load a storable used for compound assignments.
     */
    abstract void load(MethodWriter writer, Globals globals);

    /**
     * Called to store a storabable to local memory.
     */
    abstract void store(MethodWriter writer, Globals globals);
}

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

import org.elasticsearch.painless.Location;

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
     * Returns true if this node or a sub-node of this node can be optimized with
     * rhs actual type to avoid an unnecessary cast.
     */
    abstract boolean isDefOptimized();

    /**
     * If this node or a sub-node of this node uses dynamic calls then
     * actual will be set to this value. This is used for an optimization
     * during assignment to def type targets.
     */
    abstract void updateActual(Class<?> actual);
}

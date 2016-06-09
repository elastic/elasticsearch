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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.MethodWriter;

/**
 * The superclass for all L* (link) nodes.
 */
public abstract class ALink extends ANode {

    /**
     * Size is set to a value based on this link's size on the stack.  This is
     * used during the writing phase to dup stack values from this link as
     * necessary during certain store operations.
     */
    final int size;

    /**
     * Set to false only if the link is not going to be read from.
     */
    boolean load = true;

    /**
     * Set to true only if the link is going to be written to and
     * is the final link in a chain.
     */
    boolean store = false;

    /**
     * Set to true if this link represents a statik type to be accessed.
     */
    boolean statik = false;

    /**
     * Set by the parent chain to type of the previous link or null if
     * there was no previous link.
     */
    Type before = null;

    /**
     * Set by the link to be the type after the link has been loaded/stored.
     */
    Type after = null;

    /**
     * Set to true if this link could be a stand-alone statement.
     */
    boolean statement = false;

    /**
     * Used by {@link LString} to set the value of the String constant.  Also
     * used by shortcuts to represent a constant key.
     */
    String string = null;

    ALink(Location location, int size) {
        super(location);

        this.size = size;
    }

    /**
     * Checks for errors and collects data for the writing phase.
     * @return Possibly returns a different {@link ALink} node if a type is
     * def or a shortcut is used. Otherwise, returns itself.  This will be
     * updated into the {@link EChain} node's list of links.
     */
    abstract ALink analyze(Locals locals);

    /**
     * Write values before a load/store occurs such as an array index.
     */
    abstract void write(MethodWriter writer);

    /**
     * Write a load for the specific link type.
     */
    abstract void load(MethodWriter writer);

    /**
     * Write a store for the specific link type.
     */
    abstract void store(MethodWriter writer);

    /**
     * Used to copy link data from one to another during analysis in the case of replacement.
     */
    final ALink copy(ALink link) {
        load       = link.load;
        store      = link.store;
        statik     = link.statik;
        before     = link.before;
        after      = link.after;
        statement  = link.statement;
        string     = link.string;

        return this;
    }
}

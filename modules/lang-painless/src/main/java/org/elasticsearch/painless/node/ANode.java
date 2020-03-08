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
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.IRNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static java.lang.Math.max;
import static java.util.Collections.emptyList;

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
     * Writes ASM based on the data collected during the analysis phase.
     * @param classNode the root {@link ClassNode}
     */
    abstract IRNode write(ClassNode classNode);

    /**
     * Create an error with location information pointing to this node.
     */
    RuntimeException createError(RuntimeException exception) {
        return location.createError(exception);
    }

    /**
     * Subclasses should implement this with a method like {@link #singleLineToString(Object...)} or
     * {@link #multilineToString(Collection, Collection)}.
     */
    public abstract String toString();

    // Below are utilities for building the toString

    /**
     * Build {@link #toString()} for a node without inserting line breaks between the sub-nodes.
     */
    protected String singleLineToString(Object... subs) {
        return singleLineToString(Arrays.asList(subs));
    }

    /**
     * Build {@link #toString()} for a node without inserting line breaks between the sub-nodes.
     */
    protected String singleLineToString(Collection<? extends Object> subs) {
        return joinWithName(getClass().getSimpleName(), subs, emptyList());
    }

    /**
     * Build {@link #toString()} for a node that optionally ends in {@code (Args some arguments here)}. Usually function calls.
     */
    protected String singleLineToStringWithOptionalArgs(Collection<? extends ANode> arguments, Object... restOfSubs) {
        List<Object> subs = new ArrayList<>();
        Collections.addAll(subs, restOfSubs);
        if (false == arguments.isEmpty()) {
            subs.add(joinWithName("Args", arguments, emptyList()));
        }
        return singleLineToString(subs);
    }

    /**
     * Build {@link #toString()} for a node that should have new lines after some of its sub-nodes.
     */
    protected String multilineToString(Collection<? extends Object> sameLine, Collection<? extends Object> ownLine) {
        return joinWithName(getClass().getSimpleName(), sameLine, ownLine);
    }

    /**
     * Zip two (potentially uneven) lists together into for {@link #toString()}.
     */
    protected List<String> pairwiseToString(Collection<? extends Object> lefts, Collection<? extends Object> rights) {
        List<String> pairs = new ArrayList<>(max(lefts.size(), rights.size()));
        Iterator<? extends Object> left = lefts.iterator();
        Iterator<? extends Object> right = rights.iterator();
        while (left.hasNext() || right.hasNext()) {
            pairs.add(joinWithName("Pair",
                    Arrays.asList(left.hasNext() ? left.next() : "<uneven>", right.hasNext() ? right.next() : "<uneven>"),
                    emptyList()));
        }
        return pairs;
    }

    /**
     * Build a {@link #toString()} for some expressions. Usually best to use {@link #singleLineToString(Object...)} or
     * {@link #multilineToString(Collection, Collection)} instead because they include the name of the node by default.
     */
    protected String joinWithName(String name, Collection<? extends Object> sameLine,
            Collection<? extends Object> ownLine) {
        StringBuilder b = new StringBuilder();
        b.append('(').append(name);
        for (Object sub : sameLine) {
            b.append(' ').append(sub);
        }
        if (ownLine.size() == 1 && sameLine.isEmpty()) {
            b.append(' ').append(ownLine.iterator().next());
        } else {
            for (Object sub : ownLine) {
                b.append("\n  ").append(Objects.toString(sub).replace("\n", "\n  "));
            }
        }
        return b.append(')').toString();
    }
}

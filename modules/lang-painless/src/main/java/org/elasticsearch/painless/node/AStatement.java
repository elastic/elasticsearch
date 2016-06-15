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

import org.elasticsearch.painless.Locals;
import org.objectweb.asm.Label;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;

/**
 * The superclass for all S* (statement) nodes.
 */
public abstract class AStatement extends ANode {

    /**
     * Set to true when the final statement in an {@link SSource} is reached.
     * Used to determine whether or not an auto-return is necessary.
     */
    boolean lastSource = false;

    /**
     * Set to true when a loop begins.  Used by {@link SBlock} to help determine
     * when the final statement of a loop is reached.
     */
    boolean beginLoop = false;

    /**
     * Set to true when inside a loop.  Used by {@link SBreak} and {@link SContinue}
     * to determine if a break/continue statement is legal.
     */
    boolean inLoop = false;

    /**
     * Set to true when on the last statement of a loop.  Used by {@link SContinue}
     * to prevent extraneous continue statements.
     */
    boolean lastLoop = false;

    /**
     * Set to true if a statement would cause the method to exit.  Used to
     * determine whether or not an auto-return is necessary.
     */
    boolean methodEscape = false;

    /**
     * Set to true if a statement would cause a loop to exit.  Used to
     * prevent unreachable statements.
     */
    boolean loopEscape = false;

    /**
     * Set to true if all current paths escape from the current {@link SBlock}.
     * Used during the analysis phase to prevent unreachable statements and
     * the writing phase to prevent extraneous bytecode gotos from being written.
     */
    boolean allEscape = false;

    /**
     * Set to true if any continue statement occurs in a loop.  Used to prevent
     * unnecessary infinite loops.
     */
    boolean anyContinue = false;

    /**
     * Set to true if any break statement occurs in a loop.  Used to prevent
     * extraneous loops.
     */
    boolean anyBreak = false;

    /**
     * Set to the loop counter variable slot as a shortcut if loop statements
     * are being counted.
     */
    int loopCounterSlot = -1;

    /**
     * Set to the approximate number of statements in a loop block to prevent
     * infinite loops during runtime.
     */
    int statementCount = 0;

    /**
     * Set to the beginning of a loop so a continue statement knows where to
     * jump to.  Only used during the writing phase.
     */
    Label continu = null;

    /**
     * Set to the beginning of a loop so a break statement knows where to
     * jump to.  Only used during the writing phase.
     */
    Label brake = null;

    AStatement(Location location) {
        super(location);
    }

    /**
     * Checks for errors and collects data for the writing phase.
     */
    abstract void analyze(Locals locals);

    /**
     * Writes ASM based on the data collected during the analysis phase.
     */
    abstract void write(MethodWriter writer);
}

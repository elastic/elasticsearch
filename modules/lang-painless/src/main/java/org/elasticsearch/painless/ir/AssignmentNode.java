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

package org.elasticsearch.painless.ir;


import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.def;

import java.util.Objects;

public class AssignmentNode extends BinaryNode {

    protected final Location location;
    protected final boolean pre;
    protected final boolean post;
    protected final Operation operation;
    protected final boolean read;
    protected final boolean cat;
    protected final PainlessCast there;
    protected final PainlessCast back;

    public AssignmentNode(
            Location location,
            boolean pre,
            boolean post,
            Operation operation,
            boolean read,
            boolean cat,
            PainlessCast there,
            PainlessCast back
    ) {
        this.location = Objects.requireNonNull(location);
        this.pre = pre;
        this.post = post;
        this.operation = Objects.requireNonNull(operation);
        this.read = read;
        this.cat = cat;
        this.there = there;
        this.back = back;
    }

    @Override
    public void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        // For the case where the assignment represents a String concatenation
        // we must, depending on the Java version, write a StringBuilder or
        // track types going onto the stack.  This must be done before the
        // lhs is read because we need the StringBuilder to be placed on the
        // stack ahead of any potential concatenation arguments.
        int catElementStackSize = 0;

        if (cat) {
            catElementStackSize = methodWriter.writeNewStrings();
        }

        leftNode.setup(classWriter, methodWriter, globals); // call the setup method on the lhs to prepare for a load/store operation

        if (cat) {
            // Handle the case where we are doing a compound assignment
            // representing a String concatenation.

            methodWriter.writeDup(leftNode.accessElementCount(), catElementStackSize); // dup the top element and insert it
                                                                                       // before concat helper on stack
            leftNode.load(classWriter, methodWriter, globals);                         // read the current lhs's value
            methodWriter.writeAppendStrings(leftNode.getType());                       // append the lhs's value using the StringBuilder

            rightNode.write(classWriter, methodWriter, globals); // write the bytecode for the rhs

            if (rightNode instanceof BinaryMathNode == false || ((BinaryMathNode)rightNode).cat == false) { // check to see if the rhs
                                                                                                // has already done a concatenation
                methodWriter.writeAppendStrings(rightNode.getType());                           // append the rhs's value since
                                                                                                // it's hasn't already
            }

            methodWriter.writeToStrings(); // put the value for string concat onto the stack
            methodWriter.writeCast(back);  // if necessary, cast the String to the lhs actual type

            if (read) {
                // if this lhs is also read from dup the value onto the stack
                methodWriter.writeDup(MethodWriter.getType(leftNode.getType()).getSize(), leftNode.accessElementCount());
            }

            // store the lhs's value from the stack in its respective variable/field/array
            leftNode.store(classWriter, methodWriter, globals);
        } else if (operation != null) {
            // Handle the case where we are doing a compound assignment that
            // does not represent a String concatenation.

            methodWriter.writeDup(leftNode.accessElementCount(), 0); // if necessary, dup the previous lhs's value
                                                                // to be both loaded from and stored to
            leftNode.load(classWriter, methodWriter, globals); // load the current lhs's value

            if (read && post) {
                // dup the value if the lhs is also read from and is a post increment
                methodWriter.writeDup(MethodWriter.getType(leftNode.getType()).getSize(), leftNode.accessElementCount());
            }

            methodWriter.writeCast(there); // if necessary cast the current lhs's value
                                           // to the promotion type between the lhs and rhs types
            rightNode.write(classWriter, methodWriter, globals); // write the bytecode for the rhs

            // XXX: fix these types, but first we need def compound assignment tests.
            // its tricky here as there are possibly explicit casts, too.
            // write the operation instruction for compound assignment
            if (getType() == def.class) {
                methodWriter.writeDynamicBinaryInstruction(
                    location, getType(), def.class, def.class, operation, DefBootstrap.OPERATOR_COMPOUND_ASSIGNMENT);
            } else {
                methodWriter.writeBinaryInstruction(location, getType(), operation);
            }

            methodWriter.writeCast(back); // if necessary cast the promotion type value back to the lhs's type

            if (read && !post) {
                // dup the value if the lhs is also read from and is not a post increment
                methodWriter.writeDup(MethodWriter.getType(leftNode.getType()).getSize(), leftNode.accessElementCount());
            }

            leftNode.store(classWriter, methodWriter, globals); // store the lhs's value from the stack in its respective variable/field/array
        } else {
            // Handle the case for a simple write.

            rightNode.write(classWriter, methodWriter, globals); // write the bytecode for the rhs rhs

            if (read) {
                // dup the value if the lhs is also read from
                methodWriter.writeDup(MethodWriter.getType(leftNode.getType()).getSize(), leftNode.accessElementCount());
            }

            // store the lhs's value from the stack in its respective variable/field/array
            leftNode.store(classWriter, methodWriter, globals);
        }
    }
}

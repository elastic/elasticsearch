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
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScopeTable;

public class AssignmentNode extends BinaryNode {

    /* ---- begin node data ---- */

    private boolean post;
    private Operation operation;
    private boolean read;
    private boolean cat; // set to true for a compound assignment String concatenation
    private Class<?> compoundType;
    private PainlessCast there;
    private PainlessCast back;

    public void setPost(boolean post) {
        this.post = post;
    }

    public boolean getPost() {
        return post;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setRead(boolean read) {
        this.read = read;
    }

    public boolean getRead() {
        return read;
    }

    public void setCat(boolean cat) {
        this.cat = cat;
    }

    public boolean getCat() {
        return cat;
    }

    public void setCompoundType(Class<?> compoundType) {
        this.compoundType = compoundType;
    }

    public Class<?> getCompoundType() {
        return compoundType;
    }

    public String getCompoundCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(compoundType);
    }

    public void setThere(PainlessCast there) {
        this.there = there;
    }

    public PainlessCast getThere() {
        return there;
    }

    public void setBack(PainlessCast back) {
        this.back = back;
    }

    public PainlessCast getBack() {
        return back;
    }

    /* ---- end node data ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, ScopeTable scopeTable) {
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

        // call the setup method on the lhs to prepare for a load/store operation
        getLeftNode().setup(classWriter, methodWriter, scopeTable);

        if (cat) {
            // Handle the case where we are doing a compound assignment
            // representing a String concatenation.

            methodWriter.writeDup(getLeftNode().accessElementCount(), catElementStackSize); // dup the top element and insert it
                                                                                            // before concat helper on stack
            getLeftNode().load(classWriter, methodWriter, scopeTable);             // read the current lhs's value
            methodWriter.writeAppendStrings(getLeftNode().getExpressionType()); // append the lhs's value using the StringBuilder

            getRightNode().write(classWriter, methodWriter, scopeTable); // write the bytecode for the rhs

            // check to see if the rhs has already done a concatenation
            if (getRightNode() instanceof BinaryMathNode == false || ((BinaryMathNode)getRightNode()).getCat() == false) {
                // append the rhs's value since it's hasn't already
                methodWriter.writeAppendStrings(getRightNode().getExpressionType());
            }

            methodWriter.writeToStrings(); // put the value for string concat onto the stack
            methodWriter.writeCast(back);  // if necessary, cast the String to the lhs actual type

            if (read) {
                // if this lhs is also read from dup the value onto the stack
                methodWriter.writeDup(MethodWriter.getType(
                        getLeftNode().getExpressionType()).getSize(), getLeftNode().accessElementCount());
            }

            // store the lhs's value from the stack in its respective variable/field/array
            getLeftNode().store(classWriter, methodWriter, scopeTable);
        } else if (operation != null) {
            // Handle the case where we are doing a compound assignment that
            // does not represent a String concatenation.

            methodWriter.writeDup(getLeftNode().accessElementCount(), 0); // if necessary, dup the previous lhs's value
                                                                          // to be both loaded from and stored to

            getLeftNode().load(classWriter, methodWriter, scopeTable); // load the current lhs's value

            if (read && post) {
                // dup the value if the lhs is also read from and is a post increment
                methodWriter.writeDup(MethodWriter.getType(
                        getLeftNode().getExpressionType()).getSize(), getLeftNode().accessElementCount());
            }

            methodWriter.writeCast(there); // if necessary cast the current lhs's value
                                           // to the promotion type between the lhs and rhs types

            getRightNode().write(classWriter, methodWriter, scopeTable); // write the bytecode for the rhs

            // XXX: fix these types, but first we need def compound assignment tests.
            // its tricky here as there are possibly explicit casts, too.
            // write the operation instruction for compound assignment
            if (compoundType == def.class) {
                methodWriter.writeDynamicBinaryInstruction(
                        location, compoundType, def.class, def.class, operation, DefBootstrap.OPERATOR_COMPOUND_ASSIGNMENT);
            } else {
                methodWriter.writeBinaryInstruction(location, compoundType, operation);
            }

            methodWriter.writeCast(back); // if necessary cast the promotion type value back to the lhs's type

            if (read && !post) {
                // dup the value if the lhs is also read from and is not a post increment
                methodWriter.writeDup(MethodWriter.getType(
                        getLeftNode().getExpressionType()).getSize(), getLeftNode().accessElementCount());
            }

            // store the lhs's value from the stack in its respective variable/field/array
            getLeftNode().store(classWriter, methodWriter, scopeTable);
        } else {
            // Handle the case for a simple write.

            getRightNode().write(classWriter, methodWriter, scopeTable); // write the bytecode for the rhs rhs

            if (read) {
                // dup the value if the lhs is also read from
                methodWriter.writeDup(MethodWriter.getType(
                        getLeftNode().getExpressionType()).getSize(), getLeftNode().accessElementCount());
            }

            // store the lhs's value from the stack in its respective variable/field/array
            getLeftNode().store(classWriter, methodWriter, scopeTable);
        }
    }
}

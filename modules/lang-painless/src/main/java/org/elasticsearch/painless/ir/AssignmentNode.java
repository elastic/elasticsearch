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

public class AssignmentNode extends BinaryNode {

    /* ---- begin tree structure ---- */

    protected TypeNode compoundTypeNode;

    public AssignmentNode setCompoundTypeNode(TypeNode compoundTypeNode) {
        this.compoundTypeNode = compoundTypeNode;
        return this;
    }

    public TypeNode getCompoundTypeNode() {
        return compoundTypeNode;
    }

    public Class<?> getCompoundType() {
        return compoundTypeNode.getType();
    }

    public String getCompoundCanonicalTypeName() {
        return compoundTypeNode.getCanonicalTypeName();
    }
    
    @Override
    public AssignmentNode setLeftNode(ExpressionNode leftNode) {
        super.setLeftNode(leftNode);
        return this;
    }

    @Override
    public AssignmentNode setRightNode(ExpressionNode rightNode) {
        super.setRightNode(rightNode);
        return this;
    }

    @Override
    public AssignmentNode setTypeNode(TypeNode typeNode) {
        super.setTypeNode(typeNode);
        return this;
    }

    /* ---- end tree structure, begin node data ---- */

    protected boolean pre;
    protected boolean post;
    protected Operation operation;
    protected boolean read;
    protected boolean cat;
    protected PainlessCast there;
    protected PainlessCast back;

    public AssignmentNode setPre(boolean pre) {
        this.pre = pre;
        return this;
    }

    public boolean getPre() {
        return pre;
    }

    public AssignmentNode setPost(boolean post) {
        this.post = post;
        return this;
    }

    public boolean getPost() {
        return post;
    }

    public AssignmentNode setOperation(Operation operation) {
        this.operation = operation;
        return this;
    }

    public Operation getOperation() {
        return operation;
    }

    public AssignmentNode setRead(boolean read) {
        this.read = read;
        return this;
    }

    public boolean getRead() {
        return read;
    }

    public AssignmentNode setCat(boolean cat) {
        this.cat = cat;
        return this;
    }

    public boolean getCat() {
        return cat;
    }

    public AssignmentNode setThere(PainlessCast there) {
        this.there = there;
        return this;
    }

    public PainlessCast getThere() {
        return there;
    }

    public AssignmentNode setBack(PainlessCast back) {
        this.back = back;
        return this;
    }

    public PainlessCast getBack() {
        return back;
    }

    @Override
    public AssignmentNode setLocation(Location location) {
        super.setLocation(location);
        return this;
    }

    /* ---- end node data ---- */

    public AssignmentNode() {
        // do nothing
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
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
            if (getCompoundType() == def.class) {
                methodWriter.writeDynamicBinaryInstruction(
                    location, getCompoundType(), def.class, def.class, operation, DefBootstrap.OPERATOR_COMPOUND_ASSIGNMENT);
            } else {
                methodWriter.writeBinaryInstruction(location, getCompoundType(), operation);
            }

            methodWriter.writeCast(back); // if necessary cast the promotion type value back to the lhs's type

            if (read && !post) {
                // dup the value if the lhs is also read from and is not a post increment
                methodWriter.writeDup(MethodWriter.getType(leftNode.getType()).getSize(), leftNode.accessElementCount());
            }

            // store the lhs's value from the stack in its respective variable/field/array
            leftNode.store(classWriter, methodWriter, globals);
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

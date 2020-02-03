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
import org.elasticsearch.painless.WriterConstants;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScopeTable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BinaryMathNode extends BinaryNode {

    /* ---- begin node data ---- */

    private Operation operation;
    private Class<?> binaryType;
    private Class<?> shiftType;
    private boolean cat; // set to true for a String concatenation
    private boolean originallyExplicit; // record whether there was originally an explicit cast

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setBinaryType(Class<?> binaryType) {
        this.binaryType = binaryType;
    }

    public Class<?> getBinaryType() {
        return binaryType;
    }

    public String getBinaryCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(binaryType);
    }

    public void setShiftType(Class<?> shiftType) {
        this.shiftType = shiftType;
    }

    public Class<?> getShiftType() {
        return shiftType;
    }

    public String getShiftCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(shiftType);
    }

    public void setCat(boolean cat) {
        this.cat = cat;
    }

    public boolean getCat() {
        return cat;
    }

    public void setOriginallExplicit(boolean originallyExplicit) {
        this.originallyExplicit = originallyExplicit;
    }

    public boolean getOriginallyExplicit() {
        return originallyExplicit;
    }

    @Override
    public void setLocation(Location location) {
        super.setLocation(location);
    }

    /* ---- end node data ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        methodWriter.writeDebugInfo(location);

        if (getBinaryType() == String.class && operation == Operation.ADD) {
            if (cat == false) {
                methodWriter.writeNewStrings();
            }

            getLeftNode().write(classWriter, methodWriter, globals, scopeTable);

            if (getLeftNode() instanceof BinaryMathNode == false || ((BinaryMathNode)getLeftNode()).getCat() == false) {
                methodWriter.writeAppendStrings(getLeftNode().getExpressionType());
            }

            getRightNode().write(classWriter, methodWriter, globals, scopeTable);

            if (getRightNode() instanceof BinaryMathNode == false || ((BinaryMathNode)getRightNode()).getCat() == false) {
                methodWriter.writeAppendStrings(getRightNode().getExpressionType());
            }

            if (cat == false) {
                methodWriter.writeToStrings();
            }
        } else if (operation == Operation.FIND || operation == Operation.MATCH) {
            getRightNode().write(classWriter, methodWriter, globals, scopeTable);
            getLeftNode().write(classWriter, methodWriter, globals, scopeTable);
            methodWriter.invokeVirtual(org.objectweb.asm.Type.getType(Pattern.class), WriterConstants.PATTERN_MATCHER);

            if (operation == Operation.FIND) {
                methodWriter.invokeVirtual(org.objectweb.asm.Type.getType(Matcher.class), WriterConstants.MATCHER_FIND);
            } else if (operation == Operation.MATCH) {
                methodWriter.invokeVirtual(org.objectweb.asm.Type.getType(Matcher.class), WriterConstants.MATCHER_MATCHES);
            } else {
                throw new IllegalStateException("unexpected binary math operation [" + operation + "] " +
                        "for type [" + getExpressionCanonicalTypeName() + "]");
            }
        } else {
            getLeftNode().write(classWriter, methodWriter, globals, scopeTable);
            getRightNode().write(classWriter, methodWriter, globals, scopeTable);

            if (binaryType == def.class || (shiftType != null && shiftType == def.class)) {
                // def calls adopt the wanted return value. if there was a narrowing cast,
                // we need to flag that so that its done at runtime.
                int flags = 0;
                if (originallyExplicit) {
                    flags |= DefBootstrap.OPERATOR_EXPLICIT_CAST;
                }
                methodWriter.writeDynamicBinaryInstruction(location,
                        getExpressionType(), getLeftNode().getExpressionType(), getRightNode().getExpressionType(), operation, flags);
            } else {
                methodWriter.writeBinaryInstruction(location, getExpressionType(), operation);
            }
        }
    }
}

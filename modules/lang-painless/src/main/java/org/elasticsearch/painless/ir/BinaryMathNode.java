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
import org.elasticsearch.painless.lookup.def;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BinaryMathNode extends ShiftNode {

    protected final Location location;
    protected final Operation operation;
    protected final boolean cat;
    protected final boolean originallyExplicit; // record whether there was originally an explicit cast

    public BinaryMathNode(Location location, Operation operation, boolean cat, boolean originallyExplicit) {
        this.location = Objects.requireNonNull(location);
        this.operation = Objects.requireNonNull(operation);
        this.cat = cat;
        this.originallyExplicit = originallyExplicit;
    }

    @Override
    public void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        if (getType() == String.class && operation == Operation.ADD) {
            if (!cat) {
                methodWriter.writeNewStrings();
            }

            leftNode.write(classWriter, methodWriter, globals);

            if ((leftNode instanceof BinaryMathNode) == false || ((BinaryMathNode)leftNode).cat == false) {
                methodWriter.writeAppendStrings(leftNode.getType());
            }

            rightNode.write(classWriter, methodWriter, globals);

            if ((rightNode instanceof BinaryMathNode) == false || ((BinaryMathNode)rightNode).cat == false) {
                methodWriter.writeAppendStrings(rightNode.getType());
            }

            if (!cat) {
                methodWriter.writeToStrings();
            }
        } else if (operation == Operation.FIND || operation == Operation.MATCH) {
            rightNode.write(classWriter, methodWriter, globals);
            leftNode.write(classWriter, methodWriter, globals);
            methodWriter.invokeVirtual(org.objectweb.asm.Type.getType(Pattern.class), WriterConstants.PATTERN_MATCHER);

            if (operation == Operation.FIND) {
                methodWriter.invokeVirtual(org.objectweb.asm.Type.getType(Matcher.class), WriterConstants.MATCHER_FIND);
            } else if (operation == Operation.MATCH) {
                methodWriter.invokeVirtual(org.objectweb.asm.Type.getType(Matcher.class), WriterConstants.MATCHER_MATCHES);
            } else {
                throw new IllegalStateException("unexpected math operation [" + operation + "] " +
                        "for type [" + getCanonicalTypeName() + "]");
            }
        } else {
            leftNode.write(classWriter, methodWriter, globals);
            rightNode.write(classWriter, methodWriter, globals);

            if (getType() == def.class || (getShiftTypeNode() != null && getShiftType() == def.class)) {
                // def calls adopt the wanted return value. if there was a narrowing cast,
                // we need to flag that so that its done at runtime.
                int flags = 0;
                if (originallyExplicit) {
                    flags |= DefBootstrap.OPERATOR_EXPLICIT_CAST;
                }
                methodWriter.writeDynamicBinaryInstruction(location, getType(), leftNode.getType(), rightNode.getType(), operation, flags);
            } else {
                methodWriter.writeBinaryInstruction(location, getType(), operation);
            }
        }
    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.loggerusage;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.IntInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TypeInsnNode;
import org.objectweb.asm.tree.analysis.Analyzer;
import org.objectweb.asm.tree.analysis.AnalyzerException;
import org.objectweb.asm.tree.analysis.BasicInterpreter;
import org.objectweb.asm.tree.analysis.BasicValue;
import org.objectweb.asm.tree.analysis.Frame;

import java.util.Arrays;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.test.loggerusage.ESLoggerUsageChecker.LOGGER_CLASS;
import static org.elasticsearch.test.loggerusage.ESLoggerUsageChecker.LOGGER_METHODS;
import static org.elasticsearch.test.loggerusage.ESLoggerUsageChecker.IGNORE_CHECKS_ANNOTATION;

public class MethodChecker extends MethodVisitor {

    public static final Type THROWABLE_CLASS = Type.getType(Throwable.class);
    public static final Type STRING_CLASS = Type.getType(String.class);
    public static final Type OBJECT_CLASS = Type.getType(Object.class);
    public static final Type OBJECT_ARRAY_CLASS = Type.getType(Object[].class);
    public static final Type SUPPLIER_ARRAY_CLASS = Type.getType(Supplier[].class);
    public static final Type MARKER_CLASS = Type.getType(Marker.class);
    public static final Type PARAMETERIZED_MESSAGE_CLASS = Type.getType(ParameterizedMessage.class);
    // types which are subject to checking when used in logger. <code>TestMessage<code> is also declared here to make
    // sure this functionality works
    public static final Set<Type> CUSTOM_MESSAGE_TYPE = Set.of(
        Type.getObjectType("org/elasticsearch/common/logging/ESLogMessage"));

    private final String className;
    private final Consumer<WrongLoggerUsage> wrongUsageCallback;

    private boolean ignoreChecks;
    private int lineNumber;
    private MethodNode methodNode;
    private AbstractInsnNode[] instructions;
    private Frame<BasicValue>[] logMessageFrames;
    private Frame<BasicValue>[] arraySizeFrames;

    public MethodChecker(String className, int access, String name, String desc,
                         Consumer<WrongLoggerUsage> wrongUsageCallback) {
        super(Opcodes.ASM5, new MethodNode(access, name, desc, null, null));
        this.className = className;
        this.wrongUsageCallback = wrongUsageCallback;
    }

    @Override
    public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
        if (IGNORE_CHECKS_ANNOTATION.equals(Type.getType(desc).getClassName())) {
            ignoreChecks = true;
        }
        return super.visitAnnotation(desc, visible);
    }

    @Override
    public void visitEnd() {
        if (!ignoreChecks) {
            methodNode = (MethodNode) mv;
            findBadLoggerUsages();
        }
        super.visitEnd();
    }

    public void findBadLoggerUsages() {
        analyzeMethod();
        walkInstructions();
    }

    private void analyzeMethod() {
        Analyzer<BasicValue> stringPlaceHolderAnalyzer = new Analyzer<>(new PlaceHolderStringInterpreter());
        Analyzer<BasicValue> arraySizeAnalyzer = new Analyzer<>(new ArraySizeInterpreter());
        try {
            stringPlaceHolderAnalyzer.analyze(className, methodNode);
            arraySizeAnalyzer.analyze(className, methodNode);
        } catch (AnalyzerException e) {
            throw new IllegalStateException("Internal error: failed in analysis step", e);
        }
        logMessageFrames = stringPlaceHolderAnalyzer.getFrames();
        arraySizeFrames = arraySizeAnalyzer.getFrames();
        instructions = methodNode.instructions.toArray();
    }

    private void walkInstructions() {
        for (int i = 0; i < instructions.length; i++) {
            AbstractInsnNode insn = instructions[i];
            if (insn instanceof LineNumberNode) {
                lineNumber = ((LineNumberNode) insn).line;
            }
            if (!(insn instanceof MethodInsnNode)) {
                continue;
            }
            MethodInsnNode methodInsn = (MethodInsnNode) insn;
            if (insn.getOpcode() == Opcodes.INVOKEINTERFACE) {
                if (Type.getObjectType(methodInsn.owner).getClassName().equals(LOGGER_CLASS) && LOGGER_METHODS.contains(methodInsn.name)) {
                    int argOffset = Type.getArgumentTypes(methodInsn.desc)[0].equals(MARKER_CLASS) ? 1 : 0;
                    verifyMethodUsage(i, methodInsn, argOffset);
                }
            } else if (insn.getOpcode() == Opcodes.INVOKEVIRTUAL) {
                // using strings because this test do not depend on server
                if (methodInsn.owner.equals("org/elasticsearch/common/logging/DeprecationLogger") &&
                    methodInsn.name.equals("deprecate")) {
                    int argOffset = 1; // skip key
                    verifyMethodUsage(i, methodInsn, argOffset);
                }
            } else if (insn.getOpcode() == Opcodes.INVOKESPECIAL) {
                verifyConstructorUsage(i, methodInsn);
            }
        }
    }

    private void verifyMethodUsage(int insnIndex, MethodInsnNode methodInsn, int argOffset) {
        Type[] argumentTypes = Type.getArgumentTypes(methodInsn.desc);
        int effectiveArgs = argumentTypes.length - argOffset;

        if (effectiveArgs == 2 &&
            argumentTypes[argOffset].equals(STRING_CLASS) &&
            (argumentTypes[argOffset + 1].equals(OBJECT_ARRAY_CLASS) ||
                argumentTypes[argOffset + 1].equals(SUPPLIER_ARRAY_CLASS))) {
            // VARARGS METHOD: debug(Marker?, String, (Object...|Supplier...))
            checkArrayArgs(logMessageFrames[insnIndex], arraySizeFrames[insnIndex], methodInsn, argOffset, argOffset + 1);
        } else if (effectiveArgs >= 2 &&
            argumentTypes[argOffset].equals(STRING_CLASS) &&
            argumentTypes[argOffset + 1].equals(OBJECT_CLASS)) {
            // MULTI-PARAM METHOD: debug(Marker?, String, Object p0, ...)
            checkFixedArityArgs(logMessageFrames[insnIndex], methodInsn, argOffset, effectiveArgs - 1);
        } else if (effectiveArgs == 1 ||
            (effectiveArgs == 2 && argumentTypes[argOffset + 1].equals(THROWABLE_CLASS))) {
            // all the rest: debug(Marker?, (Message|MessageSupplier|CharSequence|Object|String|Supplier), Throwable?)
            checkFixedArityArgs(logMessageFrames[insnIndex], methodInsn, argOffset, 0);
        } else {
            throw new IllegalStateException("Method invoked on " + LOGGER_CLASS +
                " that is not supported by logger usage checker");
        }
    }

    private void verifyConstructorUsage(int insnIndex, MethodInsnNode methodInsn) {
        Type objectType = Type.getObjectType(methodInsn.owner);
        Type[] argumentTypes = Type.getArgumentTypes(methodInsn.desc);

        if (CUSTOM_MESSAGE_TYPE.contains(objectType)) {
            if (argumentTypes.length == 2 &&
                argumentTypes[0].equals(STRING_CLASS) &&
                argumentTypes[1].equals(OBJECT_ARRAY_CLASS)) {
                checkArrayArgs(logMessageFrames[insnIndex], arraySizeFrames[insnIndex], methodInsn, 0, 1);
            }
        } else if (objectType.equals(PARAMETERIZED_MESSAGE_CLASS)) {
            if (argumentTypes.length == 2 &&
                argumentTypes[0].equals(STRING_CLASS) &&
                argumentTypes[1].equals(OBJECT_ARRAY_CLASS)) {
                checkArrayArgs(logMessageFrames[insnIndex], arraySizeFrames[insnIndex], methodInsn, 0, 1);
            } else if (argumentTypes.length == 2 &&
                argumentTypes[0].equals(STRING_CLASS) &&
                argumentTypes[1].equals(OBJECT_CLASS)) {
                checkFixedArityArgs(logMessageFrames[insnIndex], methodInsn, 0, 1);
            } else if (argumentTypes.length == 3 &&
                argumentTypes[0].equals(STRING_CLASS) &&
                argumentTypes[1].equals(OBJECT_CLASS) &&
                argumentTypes[2].equals(OBJECT_CLASS)) {
                checkFixedArityArgs(logMessageFrames[insnIndex], methodInsn, 0, 2);
            } else if (argumentTypes.length == 3 &&
                argumentTypes[0].equals(STRING_CLASS) &&
                argumentTypes[1].equals(OBJECT_ARRAY_CLASS) &&
                argumentTypes[2].equals(THROWABLE_CLASS)) {
                checkArrayArgs(logMessageFrames[insnIndex], arraySizeFrames[insnIndex], methodInsn, 0, 1);
            } else {
                throw new IllegalStateException("Constructor invoked on " + objectType +
                    " that is not supported by logger usage checker" +
                    new WrongLoggerUsage(className, methodNode.name, methodInsn.name, lineNumber,
                        "Constructor: " + Arrays.toString(argumentTypes)));
            }
        }
    }

    private void checkFixedArityArgs(Frame<BasicValue> logMessageFrame, MethodInsnNode methodInsn,
                                     int messageIndex, int argsSize) {
        PlaceHolderStringBasicValue logMessageLength = checkLogMessageConsistency(logMessageFrame, methodInsn,
            messageIndex, argsSize);
        if (logMessageLength == null) {
            return;
        }
        if (logMessageLength.minValue != argsSize) {
            wrongUsageCallback.accept(new WrongLoggerUsage(className, methodNode.name, methodInsn.name, lineNumber,
                "Expected " + logMessageLength.minValue + " arguments but got " + argsSize));
        }
    }

    private void checkArrayArgs(Frame<BasicValue> logMessageFrame, Frame<BasicValue> arraySizeFrame,
                                MethodInsnNode methodInsn, int messageIndex, int arrayIndex) {
        BasicValue arraySizeObject = getStackValue(arraySizeFrame, methodInsn, arrayIndex);
        if (!(arraySizeObject instanceof ArraySizeBasicValue)) {
            wrongUsageCallback.accept(new WrongLoggerUsage(className, methodNode.name, methodInsn.name, lineNumber,
                "Could not determine size of array"));
            return;
        }
        ArraySizeBasicValue arraySize = (ArraySizeBasicValue) arraySizeObject;
        if (arraySize.minValue != arraySize.maxValue) {
            wrongUsageCallback.accept(new WrongLoggerUsage(className, methodNode.name, methodInsn.name, lineNumber,
                "Multiple parameter arrays with conflicting sizes"));
            return;
        }
        PlaceHolderStringBasicValue logMessageLength = checkLogMessageConsistency(logMessageFrame, methodInsn,
            messageIndex, arraySize.minValue);
        if (logMessageLength == null) {
            return;
        }
        int chainedParams = getChainedParams(methodInsn);
        int args = arraySize.minValue + chainedParams;
        if (logMessageLength.minValue != args) {
            wrongUsageCallback.accept(new WrongLoggerUsage(className, methodNode.name, methodInsn.name, lineNumber,
                "Expected " + logMessageLength.minValue + " arguments but got " + arraySize.minValue));
        }
    }

    private PlaceHolderStringBasicValue checkLogMessageConsistency(Frame<BasicValue> logMessageFrame,
                                                                   MethodInsnNode methodInsn, int messageIndex,
                                                                   int argsSize) {
        BasicValue logMessageLengthObject = getStackValue(logMessageFrame, methodInsn, messageIndex);
        if (!(logMessageLengthObject instanceof PlaceHolderStringBasicValue)) {
            if (argsSize > 0) {
                wrongUsageCallback.accept(new WrongLoggerUsage(className, methodNode.name, methodInsn.name, lineNumber,
                    "First argument must be a string constant so that we can statically ensure proper place holder usage"));
            } else {
                // don't check logger usage for logger.warn(someObject)
            }
            return null;
        }
        PlaceHolderStringBasicValue logMessageLength = (PlaceHolderStringBasicValue) logMessageLengthObject;
        if (logMessageLength.minValue != logMessageLength.maxValue) {
            wrongUsageCallback.accept(new WrongLoggerUsage(className, methodNode.name, methodInsn.name, lineNumber,
                "Multiple log messages with conflicting number of place holders"));
            return null;
        }
        return logMessageLength;
    }

    private BasicValue getStackValue(Frame<BasicValue> frame, MethodInsnNode methodInsn, int argIndex) {
        int argsSize = Type.getArgumentTypes(methodInsn.desc).length;
        int stackIndex = frame.getStackSize() - argsSize + argIndex;
        return stackIndex >= 0 ? frame.getStack(stackIndex) : null;
    }

    // counts how many times argAndField was called on the method chain
    private int getChainedParams(AbstractInsnNode startNode) {
        int count = 0;
        AbstractInsnNode current = startNode;
        while (current.getNext() != null) {
            current = current.getNext();
            if (current instanceof MethodInsnNode) {
                MethodInsnNode method = (MethodInsnNode) current;
                if (method.name.equals("argAndField")) {
                    count++;
                }
            }
        }
        return count;
    }

    private static class IntMinMaxTrackingBasicValue extends BasicValue {
        protected final int minValue;
        protected final int maxValue;

        IntMinMaxTrackingBasicValue(Type type, int value) {
            super(type);
            this.minValue = value;
            this.maxValue = value;
        }

        IntMinMaxTrackingBasicValue(Type type, int minValue, int maxValue) {
            super(type);
            this.minValue = minValue;
            this.maxValue = maxValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            IntMinMaxTrackingBasicValue that = (IntMinMaxTrackingBasicValue) o;
            return minValue == that.minValue && maxValue == that.maxValue;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + minValue;
            result = 31 * result + maxValue;
            return result;
        }

        @Override
        public String toString() {
            return "IntMinMaxTrackingBasicValue{" +
                "minValue=" + minValue +
                ", maxValue=" + maxValue +
                '}';
        }
    }

    private static class PlaceHolderStringBasicValue extends IntMinMaxTrackingBasicValue {
        static final Type STRING_OBJECT_TYPE = Type.getObjectType("java/lang/String");

        PlaceHolderStringBasicValue(int placeHolders) {
            super(STRING_OBJECT_TYPE, placeHolders);
        }

        PlaceHolderStringBasicValue(int minPlaceHolders, int maxPlaceHolders) {
            super(STRING_OBJECT_TYPE, minPlaceHolders, maxPlaceHolders);
        }
    }

    private static class ArraySizeBasicValue extends IntMinMaxTrackingBasicValue {
        ArraySizeBasicValue(Type type, int minArraySize, int maxArraySize) {
            super(type, minArraySize, maxArraySize);
        }
    }

    private static class IntegerConstantBasicValue extends IntMinMaxTrackingBasicValue {
        IntegerConstantBasicValue(Type type, int constant) {
            super(type, constant);
        }

        IntegerConstantBasicValue(Type type, int minConstant, int maxConstant) {
            super(type, minConstant, maxConstant);
        }
    }

    private static class PlaceHolderStringInterpreter extends BasicInterpreter {

        PlaceHolderStringInterpreter() {
            super(Opcodes.ASM7);
        }

        @Override
        public BasicValue newOperation(AbstractInsnNode insnNode) throws AnalyzerException {
            if (insnNode.getOpcode() == Opcodes.LDC) {
                Object constant = ((LdcInsnNode) insnNode).cst;
                if (constant instanceof String) {
                    return new PlaceHolderStringBasicValue(countNumberOfPlaceHolders((String) constant));
                }
            }
            return super.newOperation(insnNode);
        }

        @Override
        public BasicValue merge(BasicValue value1, BasicValue value2) {
            if (value1 instanceof PlaceHolderStringBasicValue && value2 instanceof PlaceHolderStringBasicValue &&
                !value1.equals(value2)) {
                PlaceHolderStringBasicValue c1 = (PlaceHolderStringBasicValue) value1;
                PlaceHolderStringBasicValue c2 = (PlaceHolderStringBasicValue) value2;
                return new PlaceHolderStringBasicValue(Math.min(c1.minValue, c2.minValue),
                    Math.max(c1.maxValue, c2.maxValue));
            }
            return super.merge(value1, value2);
        }

        private int countNumberOfPlaceHolders(String message) {
            int count = 0;
            for (int i = 1; i < message.length(); i++) {
                if (message.charAt(i - 1) == '{' && message.charAt(i) == '}') {
                    count++;
                    i += 1;
                }
            }
            return count;
        }
    }

    private static class ArraySizeInterpreter extends BasicInterpreter {

        ArraySizeInterpreter() {
            super(Opcodes.ASM7);
        }

        @Override
        public BasicValue newOperation(AbstractInsnNode insnNode) throws AnalyzerException {
            switch (insnNode.getOpcode()) {
                case ICONST_0:
                    return new IntegerConstantBasicValue(Type.INT_TYPE, 0);
                case ICONST_1:
                    return new IntegerConstantBasicValue(Type.INT_TYPE, 1);
                case ICONST_2:
                    return new IntegerConstantBasicValue(Type.INT_TYPE, 2);
                case ICONST_3:
                    return new IntegerConstantBasicValue(Type.INT_TYPE, 3);
                case ICONST_4:
                    return new IntegerConstantBasicValue(Type.INT_TYPE, 4);
                case ICONST_5:
                    return new IntegerConstantBasicValue(Type.INT_TYPE, 5);
                case BIPUSH:
                case SIPUSH:
                    return new IntegerConstantBasicValue(Type.INT_TYPE, ((IntInsnNode) insnNode).operand);
                case Opcodes.LDC:
                    Object constant = ((LdcInsnNode) insnNode).cst;
                    if (constant instanceof Integer) {
                        return new IntegerConstantBasicValue(Type.INT_TYPE, (Integer) constant);
                    } else {
                        return super.newOperation(insnNode);
                    }
                default:
                    return super.newOperation(insnNode);
            }
        }

        @Override
        public BasicValue merge(BasicValue value1, BasicValue value2) {
            if (value1 instanceof IntegerConstantBasicValue && value2 instanceof IntegerConstantBasicValue) {
                IntegerConstantBasicValue c1 = (IntegerConstantBasicValue) value1;
                IntegerConstantBasicValue c2 = (IntegerConstantBasicValue) value2;
                return new IntegerConstantBasicValue(Type.INT_TYPE, Math.min(c1.minValue, c2.minValue),
                    Math.max(c1.maxValue, c2.maxValue));
            } else if (value1 instanceof ArraySizeBasicValue && value2 instanceof ArraySizeBasicValue) {
                ArraySizeBasicValue c1 = (ArraySizeBasicValue) value1;
                ArraySizeBasicValue c2 = (ArraySizeBasicValue) value2;
                return new ArraySizeBasicValue(Type.INT_TYPE, Math.min(c1.minValue, c2.minValue),
                    Math.max(c1.maxValue, c2.maxValue));
            }
            return super.merge(value1, value2);
        }

        @Override
        public BasicValue unaryOperation(AbstractInsnNode insnNode, BasicValue value) throws AnalyzerException {
            if (insnNode.getOpcode() == Opcodes.ANEWARRAY && value instanceof IntegerConstantBasicValue) {
                IntegerConstantBasicValue constantBasicValue = (IntegerConstantBasicValue) value;
                String desc = ((TypeInsnNode) insnNode).desc;
                return new ArraySizeBasicValue(Type.getType("[" + Type.getObjectType(desc)),
                    constantBasicValue.minValue, constantBasicValue.maxValue);
            }
            return super.unaryOperation(insnNode, value);
        }

        @Override
        public BasicValue ternaryOperation(AbstractInsnNode insnNode, BasicValue value1, BasicValue value2,
                                           BasicValue value3)
            throws AnalyzerException {
            if (insnNode.getOpcode() == Opcodes.AASTORE && value1 instanceof ArraySizeBasicValue) {
                return value1;
            }
            return super.ternaryOperation(insnNode, value1, value2, value3);
        }
    }
}

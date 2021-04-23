/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.loggerusage;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class ESLoggerUsageChecker {
    public static final Type LOGGER_CLASS = Type.getType(Logger.class);
    public static final Type THROWABLE_CLASS = Type.getType(Throwable.class);
    public static final Type STRING_CLASS = Type.getType(String.class);
    public static final Type STRING_ARRAY_CLASS = Type.getType(String[].class);

    public static final Type OBJECT_CLASS = Type.getType(Object.class);
    public static final Type OBJECT_ARRAY_CLASS = Type.getType(Object[].class);
    public static final Type SUPPLIER_ARRAY_CLASS = Type.getType(Supplier[].class);
    public static final Type MARKER_CLASS = Type.getType(Marker.class);
    public static final List<String> LOGGER_METHODS = Arrays.asList("trace", "debug", "info", "warn", "error", "fatal");
    public static final String IGNORE_CHECKS_ANNOTATION = "org.elasticsearch.common.SuppressLoggerChecks";
    // types which are subject to checking when used in logger. <code>TestMessage<code> is also declared here to
    // make sure this functionality works
    public static final Set<Type> CUSTOM_MESSAGE_TYPE = Set.of(
        Type.getObjectType("org/elasticsearch/common/logging/ESLogMessage"));

    public static final Type PARAMETERIZED_MESSAGE_CLASS = Type.getType(ParameterizedMessage.class);

    @SuppressForbidden(reason = "command line tool")
    public static void main(String... args) throws Exception {
        System.out.println("checking for wrong usages of ESLogger...");
        boolean[] wrongUsageFound = new boolean[1];
        checkLoggerUsage(wrongLoggerUsage -> {
            System.err.println(wrongLoggerUsage.getErrorLines());
            wrongUsageFound[0] = true;
        }, args);
        if (wrongUsageFound[0]) {
            throw new Exception("Wrong logger usages found");
        } else {
            System.out.println("No wrong usages found");
        }
    }

    private static void checkLoggerUsage(Consumer<WrongLoggerUsage> wrongUsageCallback, String... classDirectories)
        throws IOException {
        for (String classDirectory : classDirectories) {
            Path root = Paths.get(classDirectory);
            if (Files.isDirectory(root) == false) {
                throw new IllegalArgumentException(root + " should be an existing directory");
            }
            Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (Files.isRegularFile(file) && file.getFileName().toString().endsWith(".class")) {
                        try (InputStream in = Files.newInputStream(file)) {
                            ESLoggerUsageChecker.check(wrongUsageCallback, in);
                        }
                    }
                    return super.visitFile(file, attrs);
                }
            });
        }
    }

    public static void check(Consumer<WrongLoggerUsage> wrongUsageCallback, InputStream inputStream) throws IOException {
        check(wrongUsageCallback, inputStream, s -> true);
    }

    // used by tests
    static void check(Consumer<WrongLoggerUsage> wrongUsageCallback, InputStream inputStream, Predicate<String> methodsToCheck)
        throws IOException {
        ClassReader cr = new ClassReader(inputStream);
        cr.accept(new ClassChecker(wrongUsageCallback, methodsToCheck), 0);
    }

    public static class WrongLoggerUsage {
        private final String className;
        private final String methodName;
        private final String logMethodName;
        private final int line;
        private final String errorMessage;

        public WrongLoggerUsage(String className, String methodName, String logMethodName, int line, String errorMessage) {
            this.className = className;
            this.methodName = methodName;
            this.logMethodName = logMethodName;
            this.line = line;
            this.errorMessage = errorMessage;
        }

        @Override
        public String toString() {
            return "WrongLoggerUsage{" +
                "className='" + className + '\'' +
                ", methodName='" + methodName + '\'' +
                ", logMethodName='" + logMethodName + '\'' +
                ", line=" + line +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
        }

        /**
         * Returns an error message that has the form of stack traces emitted by {@link Throwable#printStackTrace}
         */
        public String getErrorLines() {
            String fullClassName = Type.getObjectType(className).getClassName();
            String simpleClassName = fullClassName.substring(fullClassName.lastIndexOf(".") + 1, fullClassName.length());
            int innerClassIndex = simpleClassName.indexOf("$");
            if (innerClassIndex > 0) {
                simpleClassName = simpleClassName.substring(0, innerClassIndex);
            }
            simpleClassName = simpleClassName + ".java";
            StringBuilder sb = new StringBuilder();
            sb.append("Bad usage of ");
            sb.append(LOGGER_CLASS.getClassName()).append("#").append(logMethodName);
            sb.append(": ");
            sb.append(errorMessage);
            sb.append("\n\tat ");
            sb.append(fullClassName);
            sb.append(".");
            sb.append(methodName);
            sb.append("(");
            sb.append(simpleClassName);
            sb.append(":");
            sb.append(line);
            sb.append(")");
            return sb.toString();
        }
    }

    private static class ClassChecker extends ClassVisitor {
        private String className;
        private boolean ignoreChecks;
        private final Consumer<WrongLoggerUsage> wrongUsageCallback;
        private final Predicate<String> methodsToCheck;

        ClassChecker(Consumer<WrongLoggerUsage> wrongUsageCallback, Predicate<String> methodsToCheck) {
            super(Opcodes.ASM7);
            this.wrongUsageCallback = wrongUsageCallback;
            this.methodsToCheck = methodsToCheck;
        }

        @Override
        public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
            this.className = name;
        }

        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
            if (IGNORE_CHECKS_ANNOTATION.equals(Type.getType(desc).getClassName())) {
                ignoreChecks = true;
            }
            return super.visitAnnotation(desc, visible);
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            if (ignoreChecks == false && methodsToCheck.test(name)) {
                return new MethodChecker(this.className, access, name, desc, wrongUsageCallback);
            } else {
                return super.visitMethod(access, name, desc, signature, exceptions);
            }
        }
    }

    private static class MethodChecker extends MethodVisitor {
        private final String className;
        private final Consumer<WrongLoggerUsage> wrongUsageCallback;
        private boolean ignoreChecks;

        MethodChecker(String className, int access, String name, String desc, Consumer<WrongLoggerUsage> wrongUsageCallback) {
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
            if (ignoreChecks == false) {
                findBadLoggerUsages((MethodNode) mv);
            }
            super.visitEnd();
        }

        public void findBadLoggerUsages(MethodNode methodNode) {
            Analyzer<BasicValue> stringPlaceHolderAnalyzer = new Analyzer<>(new PlaceHolderStringInterpreter());
            Analyzer<BasicValue> arraySizeAnalyzer = new Analyzer<>(new ArraySizeInterpreter());
            try {
                stringPlaceHolderAnalyzer.analyze(className, methodNode);
                arraySizeAnalyzer.analyze(className, methodNode);
            } catch (AnalyzerException e) {
                throw new RuntimeException("Internal error: failed in analysis step", e);
            }
            Frame<BasicValue>[] logMessageFrames = stringPlaceHolderAnalyzer.getFrames();
            Frame<BasicValue>[] arraySizeFrames = arraySizeAnalyzer.getFrames();
            AbstractInsnNode[] insns = methodNode.instructions.toArray();
            int lineNumber = -1;
            for (int i = 0; i < insns.length; i++) {
                AbstractInsnNode insn = insns[i];
                if (insn instanceof LineNumberNode) {
                    LineNumberNode lineNumberNode = (LineNumberNode) insn;
                    lineNumber = lineNumberNode.line;
                }
                if (insn.getOpcode() == Opcodes.INVOKEINTERFACE) {
                    MethodInsnNode methodInsn = (MethodInsnNode) insn;
                    if (Type.getObjectType(methodInsn.owner).equals(LOGGER_CLASS)) {
                        if (LOGGER_METHODS.contains(methodInsn.name) == false) {
                            continue;
                        }

                        Type[] argumentTypes = Type.getArgumentTypes(methodInsn.desc);
                        int markerOffset = 0;
                        if (argumentTypes[0].equals(MARKER_CLASS)) {
                            markerOffset = 1;
                        }

                        int lengthWithoutMarker = argumentTypes.length - markerOffset;

                        verifyLoggerUsage(methodNode, logMessageFrames, arraySizeFrames, lineNumber, i,
                            methodInsn, argumentTypes, markerOffset, lengthWithoutMarker);
                    }
                } else if (insn.getOpcode() == Opcodes.INVOKESPECIAL) { // constructor invocation
                    MethodInsnNode methodInsn = (MethodInsnNode) insn;
                    Type objectType = Type.getObjectType(methodInsn.owner);

                    if (CUSTOM_MESSAGE_TYPE.contains(objectType)) {
                        Type[] argumentTypes = Type.getArgumentTypes(methodInsn.desc);
                        if (argumentTypes.length == 2 &&
                            argumentTypes[0].equals(STRING_CLASS) &&
                            argumentTypes[1].equals(OBJECT_ARRAY_CLASS)) {
                            checkArrayArgs(methodNode, logMessageFrames[i], arraySizeFrames[i], lineNumber, methodInsn, 0, 1);
                        }
                    }else if (objectType.equals(PARAMETERIZED_MESSAGE_CLASS)) {
                        Type[] argumentTypes = Type.getArgumentTypes(methodInsn.desc);
                        if (argumentTypes.length == 2 &&
                            argumentTypes[0].equals(STRING_CLASS) &&
                            argumentTypes[1].equals(OBJECT_ARRAY_CLASS)) {
                            checkArrayArgs(methodNode, logMessageFrames[i], arraySizeFrames[i], lineNumber, methodInsn, 0, 1);
                        } else if (argumentTypes.length == 2 &&
                            argumentTypes[0].equals(STRING_CLASS) &&
                            argumentTypes[1].equals(OBJECT_CLASS)) {
                            checkFixedArityArgs(methodNode, logMessageFrames[i], lineNumber, methodInsn, 0, 1);
                        } else if (argumentTypes.length == 3 &&
                            argumentTypes[0].equals(STRING_CLASS) &&
                            argumentTypes[1].equals(OBJECT_CLASS) &&
                            argumentTypes[2].equals(OBJECT_CLASS)) {
                            checkFixedArityArgs(methodNode, logMessageFrames[i], lineNumber, methodInsn, 0, 2);
                        } else if (argumentTypes.length == 3 &&
                            argumentTypes[0].equals(STRING_CLASS) &&
                            argumentTypes[1].equals(OBJECT_ARRAY_CLASS) &&
                            argumentTypes[2].equals(THROWABLE_CLASS)) {
                            checkArrayArgs(methodNode, logMessageFrames[i], arraySizeFrames[i], lineNumber, methodInsn, 0, 1);
                        } else if (argumentTypes.length == 3 &&
                            argumentTypes[0].equals(STRING_CLASS) &&
                            argumentTypes[1].equals(STRING_ARRAY_CLASS) &&
                            argumentTypes[2].equals(THROWABLE_CLASS)) {
                            checkArrayArgs(methodNode, logMessageFrames[i], arraySizeFrames[i], lineNumber, methodInsn, 0, 1);
                        } else {
                            throw new IllegalStateException("Constructor invoked on " + objectType +
                                " that is not supported by logger usage checker"+
                                new WrongLoggerUsage(className, methodNode.name, methodInsn.name, lineNumber,
                                "Constructor: "+ Arrays.toString(argumentTypes)));
                        }
                    }
                } else if (insn.getOpcode() == Opcodes.INVOKEVIRTUAL) {
                    //using strings because this test do not depend on server

                    MethodInsnNode methodInsn = (MethodInsnNode) insn;
                    if (methodInsn.owner.equals("org/elasticsearch/common/logging/DeprecationLogger")) {
                        if (methodInsn.name.equals("deprecate")) {
                            Type[] argumentTypes = Type.getArgumentTypes(methodInsn.desc);
                            int markerOffset = 1; // skip key

                            int lengthWithoutMarker = argumentTypes.length - markerOffset;

                            verifyLoggerUsage(methodNode, logMessageFrames, arraySizeFrames, lineNumber, i,
                                methodInsn, argumentTypes, markerOffset, lengthWithoutMarker);
                        }
                    }
                }
            }
        }

        private void verifyLoggerUsage(MethodNode methodNode, Frame<BasicValue>[] logMessageFrames, Frame<BasicValue>[] arraySizeFrames,
                                       int lineNumber, int i, MethodInsnNode methodInsn, Type[] argumentTypes,
                                       int markerOffset, int lengthWithoutMarker) {
            if (lengthWithoutMarker == 2 &&
                argumentTypes[markerOffset + 0].equals(STRING_CLASS) &&
                (argumentTypes[markerOffset + 1].equals(OBJECT_ARRAY_CLASS) ||
                    argumentTypes[markerOffset + 1].equals(SUPPLIER_ARRAY_CLASS))) {
                // VARARGS METHOD: debug(Marker?, String, (Object...|Supplier...))
                checkArrayArgs(methodNode, logMessageFrames[i], arraySizeFrames[i], lineNumber, methodInsn, markerOffset + 0,
                    markerOffset + 1);
            } else if (lengthWithoutMarker >= 2 &&
                argumentTypes[markerOffset + 0].equals(STRING_CLASS) &&
                argumentTypes[markerOffset + 1].equals(OBJECT_CLASS)) {
                // MULTI-PARAM METHOD: debug(Marker?, String, Object p0, ...)
                checkFixedArityArgs(methodNode, logMessageFrames[i], lineNumber, methodInsn, markerOffset + 0,
                    lengthWithoutMarker - 1);
            } else if ((lengthWithoutMarker == 1 || lengthWithoutMarker == 2) &&
                lengthWithoutMarker == 2 ? argumentTypes[markerOffset + 1].equals(THROWABLE_CLASS) : true) {
                // all the rest: debug(Marker?, (Message|MessageSupplier|CharSequence|Object|String|Supplier), Throwable?)
                checkFixedArityArgs(methodNode, logMessageFrames[i], lineNumber, methodInsn, markerOffset + 0, 0);
            } else {
                throw new IllegalStateException("Method invoked on " + LOGGER_CLASS.getClassName() +
                    " that is not supported by logger usage checker");
            }
        }

        private void checkFixedArityArgs(MethodNode methodNode, Frame<BasicValue> logMessageFrame, int lineNumber,
                                         MethodInsnNode methodInsn, int messageIndex, int positionalArgsLength) {
            PlaceHolderStringBasicValue logMessageLength = checkLogMessageConsistency(methodNode, logMessageFrame, lineNumber, methodInsn,
                messageIndex, positionalArgsLength);
            if (logMessageLength == null) {
                return;
            }
            if (logMessageLength.minValue != positionalArgsLength) {
                wrongUsageCallback.accept(new WrongLoggerUsage(className, methodNode.name, methodInsn.name, lineNumber,
                    "Expected " + logMessageLength.minValue + " arguments but got " + positionalArgsLength));
                return;
            }
        }

        private void checkArrayArgs(MethodNode methodNode, Frame<BasicValue> logMessageFrame, Frame<BasicValue> arraySizeFrame,
                                    int lineNumber, MethodInsnNode methodInsn, int messageIndex, int arrayIndex) {
            BasicValue arraySizeObject = getStackValue(arraySizeFrame, methodInsn, arrayIndex);
            if (arraySizeObject instanceof ArraySizeBasicValue == false) {
                wrongUsageCallback.accept(new WrongLoggerUsage(className, methodNode.name, methodInsn.name, lineNumber,
                    "Could not determine size of array"));
                return;
            }
            ArraySizeBasicValue arraySize = (ArraySizeBasicValue) arraySizeObject;
            PlaceHolderStringBasicValue logMessageLength = checkLogMessageConsistency(methodNode, logMessageFrame, lineNumber, methodInsn,
                messageIndex, arraySize.minValue);
            if (logMessageLength == null) {
                return;
            }
            if (arraySize.minValue != arraySize.maxValue) {
                wrongUsageCallback.accept(new WrongLoggerUsage(className, methodNode.name, methodInsn.name, lineNumber,
                    "Multiple parameter arrays with conflicting sizes"));
                return;
            }
            assert logMessageLength.minValue == logMessageLength.maxValue && arraySize.minValue == arraySize.maxValue;
            int chainedParams = getChainedParams(methodInsn);
            int args = arraySize.minValue + chainedParams;
            if (logMessageLength.minValue != args) {
                wrongUsageCallback.accept(new WrongLoggerUsage(className, methodNode.name, methodInsn.name, lineNumber,
                    "Expected " + logMessageLength.minValue + " arguments but got " + arraySize.minValue));
                return;
            }
        }

        //counts how many times argAndField  was called on the method chain
        private int getChainedParams(AbstractInsnNode startNode) {
            int c = 0;
            AbstractInsnNode current = startNode;
            while(current.getNext() != null){
                current = current.getNext();
                if(current instanceof MethodInsnNode){
                    MethodInsnNode method = (MethodInsnNode)current;
                    if(method.name.equals("argAndField")){
                        c++;
                    }
                }
            }
            return c;
        }

        private PlaceHolderStringBasicValue checkLogMessageConsistency(MethodNode methodNode, Frame<BasicValue> logMessageFrame,
                                                                       int lineNumber, MethodInsnNode methodInsn, int messageIndex,
                                                                       int argsSize) {
            BasicValue logMessageLengthObject = getStackValue(logMessageFrame, methodInsn, messageIndex);
            if (logMessageLengthObject instanceof PlaceHolderStringBasicValue == false) {
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
    }

    private static int calculateNumberOfPlaceHolders(String message) {
        int count = 0;
        for (int i = 1; i < message.length(); i++) {
            if (message.charAt(i - 1) == '{' && message.charAt(i) == '}') {
                count++;
                i += 1;
            }
        }
        return count;
    }

    private static BasicValue getStackValue(Frame<BasicValue> f, MethodInsnNode methodInsn, int index) {
        int relIndex = Type.getArgumentTypes(methodInsn.desc).length - 1 - index;
        int top = f.getStackSize() - 1;
        return relIndex <= top ? f.getStack(top - relIndex) : null;
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
            if (super.equals(o) == false) return false;

            IntMinMaxTrackingBasicValue that = (IntMinMaxTrackingBasicValue) o;

            if (minValue != that.minValue) return false;
            return maxValue == that.maxValue;

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

    private static final class PlaceHolderStringBasicValue extends IntMinMaxTrackingBasicValue {
        public static final Type STRING_OBJECT_TYPE = Type.getObjectType("java/lang/String");

        PlaceHolderStringBasicValue(int placeHolders) {
            super(STRING_OBJECT_TYPE, placeHolders);
        }

        PlaceHolderStringBasicValue(int minPlaceHolders, int maxPlaceHolders) {
            super(STRING_OBJECT_TYPE, minPlaceHolders, maxPlaceHolders);
        }
    }

    private static final class ArraySizeBasicValue extends IntMinMaxTrackingBasicValue {
        ArraySizeBasicValue(Type type, int minArraySize, int maxArraySize) {
            super(type, minArraySize, maxArraySize);
        }
    }

    private static final class IntegerConstantBasicValue extends IntMinMaxTrackingBasicValue {
        IntegerConstantBasicValue(Type type, int constant) {
            super(type, constant);
        }

        IntegerConstantBasicValue(Type type, int minConstant, int maxConstant) {
            super(type, minConstant, maxConstant);
        }
    }

    private static final class PlaceHolderStringInterpreter extends BasicInterpreter {

        PlaceHolderStringInterpreter() {
            super(Opcodes.ASM7);
        }

        @Override
        public BasicValue newOperation(AbstractInsnNode insnNode) throws AnalyzerException {
            if (insnNode.getOpcode() == Opcodes.LDC) {
                Object constant = ((LdcInsnNode) insnNode).cst;
                if (constant instanceof String) {
                    return new PlaceHolderStringBasicValue(calculateNumberOfPlaceHolders((String) constant));
                }
            }
            return super.newOperation(insnNode);
        }

        @Override
        public BasicValue merge(BasicValue value1, BasicValue value2) {
            if (value1 instanceof PlaceHolderStringBasicValue && value2 instanceof PlaceHolderStringBasicValue
                && value1.equals(value2) == false) {
                PlaceHolderStringBasicValue c1 = (PlaceHolderStringBasicValue) value1;
                PlaceHolderStringBasicValue c2 = (PlaceHolderStringBasicValue) value2;
                return new PlaceHolderStringBasicValue(Math.min(c1.minValue, c2.minValue), Math.max(c1.maxValue, c2.maxValue));
            }
            return super.merge(value1, value2);
        }
    }

    private static final class ArraySizeInterpreter extends BasicInterpreter {

        ArraySizeInterpreter() {
            super(Opcodes.ASM7);
        }

        @Override
        public BasicValue newOperation(AbstractInsnNode insnNode) throws AnalyzerException {
            switch (insnNode.getOpcode()) {
                case ICONST_0: return new IntegerConstantBasicValue(Type.INT_TYPE, 0);
                case ICONST_1: return new IntegerConstantBasicValue(Type.INT_TYPE, 1);
                case ICONST_2: return new IntegerConstantBasicValue(Type.INT_TYPE, 2);
                case ICONST_3: return new IntegerConstantBasicValue(Type.INT_TYPE, 3);
                case ICONST_4: return new IntegerConstantBasicValue(Type.INT_TYPE, 4);
                case ICONST_5: return new IntegerConstantBasicValue(Type.INT_TYPE, 5);
                case BIPUSH:
                case SIPUSH: return new IntegerConstantBasicValue(Type.INT_TYPE, ((IntInsnNode)insnNode).operand);
                case Opcodes.LDC: {
                    Object constant = ((LdcInsnNode)insnNode).cst;
                    if (constant instanceof Integer) {
                        return new IntegerConstantBasicValue(Type.INT_TYPE, (Integer)constant);
                    } else {
                        return super.newOperation(insnNode);
                    }
                }
                default: return super.newOperation(insnNode);
            }
        }

        @Override
        public BasicValue merge(BasicValue value1, BasicValue value2) {
            if (value1 instanceof IntegerConstantBasicValue && value2 instanceof IntegerConstantBasicValue) {
                IntegerConstantBasicValue c1 = (IntegerConstantBasicValue) value1;
                IntegerConstantBasicValue c2 = (IntegerConstantBasicValue) value2;
                return new IntegerConstantBasicValue(Type.INT_TYPE, Math.min(c1.minValue, c2.minValue), Math.max(c1.maxValue, c2.maxValue));
            } else if (value1 instanceof ArraySizeBasicValue && value2 instanceof ArraySizeBasicValue) {
                ArraySizeBasicValue c1 = (ArraySizeBasicValue) value1;
                ArraySizeBasicValue c2 = (ArraySizeBasicValue) value2;
                return new ArraySizeBasicValue(Type.INT_TYPE, Math.min(c1.minValue, c2.minValue), Math.max(c1.maxValue, c2.maxValue));
            }
            return super.merge(value1, value2);
        }

        @Override
        public BasicValue unaryOperation(AbstractInsnNode insnNode, BasicValue value) throws AnalyzerException {
            if (insnNode.getOpcode() == Opcodes.ANEWARRAY && value instanceof IntegerConstantBasicValue) {
                IntegerConstantBasicValue constantBasicValue = (IntegerConstantBasicValue) value;
                String desc = ((TypeInsnNode) insnNode).desc;
                return new ArraySizeBasicValue(Type.getType("[" + Type.getObjectType(desc)), constantBasicValue.minValue,
                    constantBasicValue.maxValue);
            }
            return super.unaryOperation(insnNode, value);
        }

        @Override
        public BasicValue ternaryOperation(AbstractInsnNode insnNode, BasicValue value1, BasicValue value2, BasicValue value3)
            throws AnalyzerException {
            if (insnNode.getOpcode() == Opcodes.AASTORE && value1 instanceof ArraySizeBasicValue) {
                return value1;
            }
            return super.ternaryOperation(insnNode, value1, value2, value3);
        }
    }
}

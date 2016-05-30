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

package org.elasticsearch.test.loggerusage;

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
import java.util.function.Consumer;
import java.util.function.Predicate;

public class ESLoggerUsageChecker {
    public static final String LOGGER_CLASS = "org.elasticsearch.common.logging.ESLogger";
    public static final String THROWABLE_CLASS = "java.lang.Throwable";
    public static final List<String> LOGGER_METHODS = Arrays.asList("trace", "debug", "info", "warn", "error");
    public static final String IGNORE_CHECKS_ANNOTATION = "org.elasticsearch.common.SuppressLoggerChecks";

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
                    if (Files.isRegularFile(file) && file.endsWith(".class")) {
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
            sb.append(LOGGER_CLASS).append("#").append(logMethodName);
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

        public ClassChecker(Consumer<WrongLoggerUsage> wrongUsageCallback, Predicate<String> methodsToCheck) {
            super(Opcodes.ASM5);
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

        public MethodChecker(String className, int access, String name, String desc, Consumer<WrongLoggerUsage> wrongUsageCallback) {
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
            Frame<BasicValue>[] stringFrames = stringPlaceHolderAnalyzer.getFrames();
            Frame<BasicValue>[] arraySizeFrames = arraySizeAnalyzer.getFrames();
            AbstractInsnNode[] insns = methodNode.instructions.toArray();
            int lineNumber = -1;
            for (int i = 0; i < insns.length; i++) {
                AbstractInsnNode insn = insns[i];
                if (insn instanceof LineNumberNode) {
                    LineNumberNode lineNumberNode = (LineNumberNode) insn;
                    lineNumber = lineNumberNode.line;
                }
                if (insn.getOpcode() == Opcodes.INVOKEVIRTUAL) {
                    MethodInsnNode methodInsn = (MethodInsnNode) insn;
                    if (Type.getObjectType(methodInsn.owner).getClassName().equals(LOGGER_CLASS) == false) {
                        continue;
                    }
                    if (LOGGER_METHODS.contains(methodInsn.name) == false) {
                        continue;
                    }
                    Type[] argumentTypes = Type.getArgumentTypes(methodInsn.desc);
                    BasicValue logMessageLengthObject = getStackValue(stringFrames[i], argumentTypes.length - 1); // first argument
                    if (logMessageLengthObject instanceof PlaceHolderStringBasicValue == false) {
                        wrongUsageCallback.accept(new WrongLoggerUsage(className, methodNode.name, methodInsn.name, lineNumber,
                            "First argument must be a string constant so that we can statically ensure proper place holder usage"));
                        continue;
                    }
                    PlaceHolderStringBasicValue logMessageLength = (PlaceHolderStringBasicValue) logMessageLengthObject;
                    if (logMessageLength.minValue != logMessageLength.maxValue) {
                        wrongUsageCallback.accept(new WrongLoggerUsage(className, methodNode.name, methodInsn.name, lineNumber,
                            "Multiple log messages with conflicting number of place holders"));
                        continue;
                    }
                    BasicValue varArgsSizeObject = getStackValue(arraySizeFrames[i], 0); // last argument
                    if (varArgsSizeObject instanceof ArraySizeBasicValue == false) {
                        wrongUsageCallback.accept(new WrongLoggerUsage(className, methodNode.name, methodInsn.name, lineNumber,
                            "Could not determine size of varargs array"));
                        continue;
                    }
                    ArraySizeBasicValue varArgsSize = (ArraySizeBasicValue) varArgsSizeObject;
                    if (varArgsSize.minValue != varArgsSize.maxValue) {
                        wrongUsageCallback.accept(new WrongLoggerUsage(className, methodNode.name, methodInsn.name, lineNumber,
                            "Multiple parameter arrays with conflicting sizes"));
                        continue;
                    }
                    assert logMessageLength.minValue == logMessageLength.maxValue && varArgsSize.minValue == varArgsSize.maxValue;
                    if (logMessageLength.minValue != varArgsSize.minValue) {
                        wrongUsageCallback.accept(new WrongLoggerUsage(className, methodNode.name, methodInsn.name, lineNumber,
                            "Expected " + logMessageLength.minValue + " arguments but got " + varArgsSize.minValue));
                        continue;
                    }
                }
            }
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

    private static BasicValue getStackValue(Frame<BasicValue> f, int index) {
        int top = f.getStackSize() - 1;
        return index <= top ? f.getStack(top - index) : null;
    }

    private static class IntMinMaxTrackingBasicValue extends BasicValue {
        protected final int minValue;
        protected final int maxValue;

        public IntMinMaxTrackingBasicValue(Type type, int value) {
            super(type);
            this.minValue = value;
            this.maxValue = value;
        }

        public IntMinMaxTrackingBasicValue(Type type, int minValue, int maxValue) {
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

        public PlaceHolderStringBasicValue(int placeHolders) {
            super(STRING_OBJECT_TYPE, placeHolders);
        }

        public PlaceHolderStringBasicValue(int minPlaceHolders, int maxPlaceHolders) {
            super(STRING_OBJECT_TYPE, minPlaceHolders, maxPlaceHolders);
        }
    }

    private static final class ArraySizeBasicValue extends IntMinMaxTrackingBasicValue {
        public ArraySizeBasicValue(Type type, int minArraySize, int maxArraySize) {
            super(type, minArraySize, maxArraySize);
        }
    }

    private static final class IntegerConstantBasicValue extends IntMinMaxTrackingBasicValue {
        public IntegerConstantBasicValue(Type type, int constant) {
            super(type, constant);
        }

        public IntegerConstantBasicValue(Type type, int minConstant, int maxConstant) {
            super(type, minConstant, maxConstant);
        }
    }

    private static final class PlaceHolderStringInterpreter extends BasicInterpreter {
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

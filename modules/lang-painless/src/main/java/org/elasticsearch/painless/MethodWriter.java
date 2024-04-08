/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Deque;
import java.util.List;

import static org.elasticsearch.painless.WriterConstants.CHAR_TO_STRING;
import static org.elasticsearch.painless.WriterConstants.DEF_BOOTSTRAP_HANDLE;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_B_BOOLEAN;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_B_BYTE_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_B_BYTE_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_B_CHARACTER_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_B_CHARACTER_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_B_DOUBLE_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_B_DOUBLE_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_B_FLOAT_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_B_FLOAT_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_B_INTEGER_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_B_INTEGER_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_B_LONG_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_B_LONG_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_B_SHORT_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_B_SHORT_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_P_BOOLEAN;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_P_BYTE_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_P_BYTE_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_P_CHAR_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_P_CHAR_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_P_DOUBLE_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_P_DOUBLE_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_P_FLOAT_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_P_FLOAT_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_P_INT_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_P_INT_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_P_LONG_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_P_LONG_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_P_SHORT_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_P_SHORT_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_STRING_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_STRING_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_UTIL_TYPE;
import static org.elasticsearch.painless.WriterConstants.LAMBDA_BOOTSTRAP_HANDLE;
import static org.elasticsearch.painless.WriterConstants.MAX_STRING_CONCAT_ARGS;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.STRING_CONCAT_BOOTSTRAP_HANDLE;
import static org.elasticsearch.painless.WriterConstants.STRING_TO_CHAR;
import static org.elasticsearch.painless.WriterConstants.STRING_TYPE;
import static org.elasticsearch.painless.WriterConstants.UTILITY_TYPE;

/**
 * Extension of {@link GeneratorAdapter} with some utility methods.
 * <p>
 * Set of methods used during the writing phase of compilation
 * shared by the nodes of the Painless tree.
 */
public final class MethodWriter extends GeneratorAdapter {
    private final BitSet statements;
    private final CompilerSettings settings;

    private final Deque<List<Type>> stringConcatArgs = new ArrayDeque<>();

    public MethodWriter(int access, Method method, ClassVisitor cw, BitSet statements, CompilerSettings settings) {
        super(
            Opcodes.ASM5,
            cw.visitMethod(access, method.getName(), method.getDescriptor(), null, null),
            access,
            method.getName(),
            method.getDescriptor()
        );

        this.statements = statements;
        this.settings = settings;
    }

    /**
     * Marks a new statement boundary.
     * <p>
     * This is invoked for each statement boundary (leaf {@code S*} nodes).
     */
    public void writeStatementOffset(Location location) {
        int offset = location.getOffset();
        // ensure we don't have duplicate stuff going in here. can catch bugs
        // (e.g. nodes get assigned wrong offsets by antlr walker)
        // TODO: introduce a way to ignore internal statements so this assert is not triggered
        // TODO: https://github.com/elastic/elasticsearch/issues/51836
        // assert statements.get(offset) == false;
        statements.set(offset);
    }

    /**
     * Encodes the offset into the line number table as {@code offset + 1}.
     * <p>
     * This is invoked before instructions that can hit exceptions.
     */
    public void writeDebugInfo(Location location) {
        // TODO: maybe track these in bitsets too? this is trickier...
        Label label = new Label();
        visitLabel(label);
        visitLineNumber(location.getOffset() + 1, label);
    }

    public void writeLoopCounter(int slot, Location location) {
        assert slot != -1;
        writeDebugInfo(location);
        final Label end = new Label();

        iinc(slot, -1);
        visitVarInsn(Opcodes.ILOAD, slot);
        push(0);
        ifICmp(GeneratorAdapter.GT, end);
        throwException(PAINLESS_ERROR_TYPE, "The maximum number of statements that can be executed in a loop has been reached.");
        mark(end);
    }

    public void writeCast(PainlessCast cast) {
        if (cast == null) {
            return;
        }
        if (cast.originalType == char.class && cast.targetType == String.class) {
            invokeStatic(UTILITY_TYPE, CHAR_TO_STRING);
        } else if (cast.originalType == String.class && cast.targetType == char.class) {
            invokeStatic(UTILITY_TYPE, STRING_TO_CHAR);
        } else if (cast.unboxOriginalType != null && cast.boxTargetType != null) {
            unbox(getType(cast.unboxOriginalType));
            writeCast(cast.unboxOriginalType, cast.boxTargetType);
            box(getType(cast.boxTargetType));
        } else if (cast.unboxOriginalType != null) {
            unbox(getType(cast.unboxOriginalType));
            writeCast(cast.originalType, cast.targetType);
        } else if (cast.unboxTargetType != null) {
            writeCast(cast.originalType, cast.targetType);
            unbox(getType(cast.unboxTargetType));
        } else if (cast.boxOriginalType != null) {
            box(getType(cast.boxOriginalType));
            writeCast(cast.originalType, cast.targetType);
        } else if (cast.boxTargetType != null) {
            writeCast(cast.originalType, cast.targetType);
            box(getType(cast.boxTargetType));
        } else if (cast.originalType == def.class) {
            if (cast.explicitCast) {
                if (cast.targetType == boolean.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_P_BOOLEAN);
                else if (cast.targetType == byte.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_P_BYTE_EXPLICIT);
                else if (cast.targetType == short.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_P_SHORT_EXPLICIT);
                else if (cast.targetType == char.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_P_CHAR_EXPLICIT);
                else if (cast.targetType == int.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_P_INT_EXPLICIT);
                else if (cast.targetType == long.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_P_LONG_EXPLICIT);
                else if (cast.targetType == float.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_P_FLOAT_EXPLICIT);
                else if (cast.targetType == double.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_P_DOUBLE_EXPLICIT);
                else if (cast.targetType == Boolean.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_BOOLEAN);
                else if (cast.targetType == Byte.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_BYTE_EXPLICIT);
                else if (cast.targetType == Short.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_SHORT_EXPLICIT);
                else if (cast.targetType == Character.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_CHARACTER_EXPLICIT);
                else if (cast.targetType == Integer.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_INTEGER_EXPLICIT);
                else if (cast.targetType == Long.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_LONG_EXPLICIT);
                else if (cast.targetType == Float.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_FLOAT_EXPLICIT);
                else if (cast.targetType == Double.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_DOUBLE_EXPLICIT);
                else if (cast.targetType == String.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_STRING_EXPLICIT);
                else {
                    writeCast(cast.originalType, cast.targetType);
                }
            } else {
                if (cast.targetType == boolean.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_P_BOOLEAN);
                else if (cast.targetType == byte.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_P_BYTE_IMPLICIT);
                else if (cast.targetType == short.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_P_SHORT_IMPLICIT);
                else if (cast.targetType == char.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_P_CHAR_IMPLICIT);
                else if (cast.targetType == int.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_P_INT_IMPLICIT);
                else if (cast.targetType == long.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_P_LONG_IMPLICIT);
                else if (cast.targetType == float.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_P_FLOAT_IMPLICIT);
                else if (cast.targetType == double.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_P_DOUBLE_IMPLICIT);
                else if (cast.targetType == Boolean.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_BOOLEAN);
                else if (cast.targetType == Byte.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_BYTE_IMPLICIT);
                else if (cast.targetType == Short.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_SHORT_IMPLICIT);
                else if (cast.targetType == Character.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_CHARACTER_IMPLICIT);
                else if (cast.targetType == Integer.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_INTEGER_IMPLICIT);
                else if (cast.targetType == Long.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_LONG_IMPLICIT);
                else if (cast.targetType == Float.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_FLOAT_IMPLICIT);
                else if (cast.targetType == Double.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_DOUBLE_IMPLICIT);
                else if (cast.targetType == String.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_STRING_IMPLICIT);
                else {
                    writeCast(cast.originalType, cast.targetType);
                }
            }
        } else {
            writeCast(cast.originalType, cast.targetType);
        }
    }

    private void writeCast(Class<?> from, Class<?> to) {
        if (from.equals(to)) {
            return;
        }

        if (from != boolean.class && from.isPrimitive() && to != boolean.class && to.isPrimitive()) {
            cast(getType(from), getType(to));
        } else {
            if (to.isAssignableFrom(from) == false) {
                checkCast(getType(to));
            }
        }
    }

    /**
     * Proxy the box method to use valueOf instead to ensure that the modern boxing methods are used.
     */
    @Override
    public void box(Type type) {
        valueOf(type);
    }

    public static Type getType(Class<?> clazz) {
        if (clazz.isArray()) {
            Class<?> component = clazz.getComponentType();
            int dimensions = 1;

            while (component.isArray()) {
                component = component.getComponentType();
                ++dimensions;
            }

            if (component == def.class) {
                char[] braces = new char[dimensions];
                Arrays.fill(braces, '[');

                return Type.getType(new String(braces) + Type.getType(Object.class).getDescriptor());
            }
        } else if (clazz == def.class) {
            return Type.getType(Object.class);
        }

        return Type.getType(clazz);
    }

    /** Starts a new string concat.
     * @return the size of arguments pushed to stack (the object that does string concats, e.g. a StringBuilder)
     */
    public List<Type> writeNewStrings() {
        List<Type> list = new ArrayList<>();
        stringConcatArgs.push(list);
        return list;
    }

    public void writeAppendStrings(Class<?> clazz) {
        List<Type> currentConcat = stringConcatArgs.peek();
        currentConcat.add(getType(clazz));
        // prevent too many concat args.
        // If there are too many, do the actual concat:
        if (currentConcat.size() >= MAX_STRING_CONCAT_ARGS) {
            writeToStrings();
            currentConcat = writeNewStrings();
            // add the return value type as new first param for next concat:
            currentConcat.add(STRING_TYPE);
        }
    }

    public void writeToStrings() {
        final String desc = Type.getMethodDescriptor(STRING_TYPE, stringConcatArgs.pop().toArray(Type[]::new));
        invokeDynamic("concat", desc, STRING_CONCAT_BOOTSTRAP_HANDLE);
    }

    /** Writes a dynamic binary instruction: returnType, lhs, and rhs can be different */
    public void writeDynamicBinaryInstruction(
        Location location,
        Class<?> returnType,
        Class<?> lhs,
        Class<?> rhs,
        Operation operation,
        int flags
    ) {
        Type methodType = Type.getMethodType(getType(returnType), getType(lhs), getType(rhs));

        switch (operation) {
            case MUL -> invokeDefCall("mul", methodType, DefBootstrap.BINARY_OPERATOR, flags);
            case DIV -> invokeDefCall("div", methodType, DefBootstrap.BINARY_OPERATOR, flags);
            case REM -> invokeDefCall("rem", methodType, DefBootstrap.BINARY_OPERATOR, flags);
            case ADD -> {
                // if either side is primitive, then the + operator should always throw NPE on null,
                // so we don't need a special NPE guard.
                // otherwise, we need to allow nulls for possible string concatenation.
                boolean hasPrimitiveArg = lhs.isPrimitive() || rhs.isPrimitive();
                if (hasPrimitiveArg == false) {
                    flags |= DefBootstrap.OPERATOR_ALLOWS_NULL;
                }
                invokeDefCall("add", methodType, DefBootstrap.BINARY_OPERATOR, flags);
            }
            case SUB -> invokeDefCall("sub", methodType, DefBootstrap.BINARY_OPERATOR, flags);
            case LSH -> invokeDefCall("lsh", methodType, DefBootstrap.SHIFT_OPERATOR, flags);
            case USH -> invokeDefCall("ush", methodType, DefBootstrap.SHIFT_OPERATOR, flags);
            case RSH -> invokeDefCall("rsh", methodType, DefBootstrap.SHIFT_OPERATOR, flags);
            case BWAND -> invokeDefCall("and", methodType, DefBootstrap.BINARY_OPERATOR, flags);
            case XOR -> invokeDefCall("xor", methodType, DefBootstrap.BINARY_OPERATOR, flags);
            case BWOR -> invokeDefCall("or", methodType, DefBootstrap.BINARY_OPERATOR, flags);
            default -> throw location.createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    /** Writes a static binary instruction */
    public void writeBinaryInstruction(Location location, Class<?> clazz, Operation operation) {
        if ((clazz == float.class || clazz == double.class)
            && (operation == Operation.LSH
                || operation == Operation.USH
                || operation == Operation.RSH
                || operation == Operation.BWAND
                || operation == Operation.XOR
                || operation == Operation.BWOR)) {
            throw location.createError(new IllegalStateException("Illegal tree structure."));
        }

        switch (operation) {
            case MUL -> math(GeneratorAdapter.MUL, getType(clazz));
            case DIV -> math(GeneratorAdapter.DIV, getType(clazz));
            case REM -> math(GeneratorAdapter.REM, getType(clazz));
            case ADD -> math(GeneratorAdapter.ADD, getType(clazz));
            case SUB -> math(GeneratorAdapter.SUB, getType(clazz));
            case LSH -> math(GeneratorAdapter.SHL, getType(clazz));
            case USH -> math(GeneratorAdapter.USHR, getType(clazz));
            case RSH -> math(GeneratorAdapter.SHR, getType(clazz));
            case BWAND -> math(GeneratorAdapter.AND, getType(clazz));
            case XOR -> math(GeneratorAdapter.XOR, getType(clazz));
            case BWOR -> math(GeneratorAdapter.OR, getType(clazz));
            default -> throw location.createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    public void writeDup(final int size, final int xsize) {
        if (size == 1) {
            if (xsize == 2) {
                dupX2();
            } else if (xsize == 1) {
                dupX1();
            } else {
                dup();
            }
        } else if (size == 2) {
            if (xsize == 2) {
                dup2X2();
            } else if (xsize == 1) {
                dup2X1();
            } else {
                dup2();
            }
        }
    }

    public void writePop(final int size) {
        if (size == 1) {
            pop();
        } else if (size == 2) {
            pop2();
        }
    }

    @Override
    public void endMethod() {
        if (stringConcatArgs != null && stringConcatArgs.isEmpty() == false) {
            throw new IllegalStateException("String concat bytecode not completed.");
        }
        super.endMethod();
    }

    @Override
    public void visitEnd() {
        throw new AssertionError("Should never call this method on MethodWriter, use endMethod() instead");
    }

    /**
     * Writes a dynamic call for a def method.
     * @param name method name
     * @param methodType callsite signature
     * @param flavor type of call
     * @param params flavor-specific parameters
     */
    public void invokeDefCall(String name, Type methodType, int flavor, Object... params) {
        Object[] args = new Object[params.length + 2];
        args[0] = settings.getInitialCallSiteDepth();
        args[1] = flavor;
        System.arraycopy(params, 0, args, 2, params.length);
        invokeDynamic(name, methodType.getDescriptor(), DEF_BOOTSTRAP_HANDLE, args);
    }

    public void invokeMethodCall(PainlessMethod painlessMethod) {
        Type type = Type.getType(painlessMethod.javaMethod().getDeclaringClass());
        Method method = Method.getMethod(painlessMethod.javaMethod());

        if (Modifier.isStatic(painlessMethod.javaMethod().getModifiers())) {
            // invokeStatic assumes that the owner class is not an interface, so this is a
            // special case for interfaces where the interface method boolean needs to be set to
            // true to reference the appropriate class constant when calling a static interface
            // method since java 8 did not check, but java 9 and 10 do
            if (painlessMethod.javaMethod().getDeclaringClass().isInterface()) {
                visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    type.getInternalName(),
                    painlessMethod.javaMethod().getName(),
                    method.getDescriptor(),
                    true
                );
            } else {
                invokeStatic(type, method);
            }
        } else if (painlessMethod.javaMethod().getDeclaringClass().isInterface()) {
            invokeInterface(type, method);
        } else {
            invokeVirtual(type, method);
        }
    }

    public void invokeLambdaCall(FunctionRef functionRef) {
        Object[] args = new Object[7 + functionRef.delegateInjections.length];
        args[0] = Type.getMethodType(functionRef.interfaceMethodType.toMethodDescriptorString());
        args[1] = functionRef.delegateClassName;
        args[2] = functionRef.delegateInvokeType;
        args[3] = functionRef.delegateMethodName;
        args[4] = Type.getMethodType(functionRef.delegateMethodType.toMethodDescriptorString());
        args[5] = functionRef.isDelegateInterface ? 1 : 0;
        args[6] = functionRef.isDelegateAugmented ? 1 : 0;
        System.arraycopy(functionRef.delegateInjections, 0, args, 7, functionRef.delegateInjections.length);

        invokeDynamic(functionRef.interfaceMethodName, functionRef.getFactoryMethodDescriptor(), LAMBDA_BOOTSTRAP_HANDLE, args);
    }
}

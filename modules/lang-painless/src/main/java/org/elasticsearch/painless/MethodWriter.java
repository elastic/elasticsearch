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

package org.elasticsearch.painless;

import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Definition.Type;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.List;

import static org.elasticsearch.painless.WriterConstants.CHAR_TO_STRING;
import static org.elasticsearch.painless.WriterConstants.DEF_BOOTSTRAP_HANDLE;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_BOOLEAN;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_BYTE_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_BYTE_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_CHAR_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_CHAR_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_DOUBLE_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_DOUBLE_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_FLOAT_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_FLOAT_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_INT_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_INT_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_LONG_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_LONG_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_SHORT_EXPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_TO_SHORT_IMPLICIT;
import static org.elasticsearch.painless.WriterConstants.DEF_UTIL_TYPE;
import static org.elasticsearch.painless.WriterConstants.INDY_STRING_CONCAT_BOOTSTRAP_HANDLE;
import static org.elasticsearch.painless.WriterConstants.MAX_INDY_STRING_CONCAT_ARGS;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_APPEND_BOOLEAN;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_APPEND_CHAR;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_APPEND_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_APPEND_FLOAT;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_APPEND_INT;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_APPEND_LONG;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_APPEND_OBJECT;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_APPEND_STRING;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_CONSTRUCTOR;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_TOSTRING;
import static org.elasticsearch.painless.WriterConstants.STRINGBUILDER_TYPE;
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

    private final Deque<List<org.objectweb.asm.Type>> stringConcatArgs =
        (INDY_STRING_CONCAT_BOOTSTRAP_HANDLE == null) ?  null : new ArrayDeque<>();

    public MethodWriter(int access, Method method, ClassVisitor cw, BitSet statements, CompilerSettings settings) {
        super(Opcodes.ASM5, cw.visitMethod(access, method.getName(), method.getDescriptor(), null, null),
                access, method.getName(), method.getDescriptor());

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
        assert statements.get(offset) == false;
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

    public void writeLoopCounter(int slot, int count, Location location) {
        assert slot != -1;
        writeDebugInfo(location);
        final Label end = new Label();

        iinc(slot, -count);
        visitVarInsn(Opcodes.ILOAD, slot);
        push(0);
        ifICmp(GeneratorAdapter.GT, end);
        throwException(PAINLESS_ERROR_TYPE, "The maximum number of statements that can be executed in a loop has been reached.");
        mark(end);
    }

    public void writeCast(final Cast cast) {
        if (cast != null) {
            if (cast.from.clazz == char.class && cast.to.clazz == String.class) {
                invokeStatic(UTILITY_TYPE, CHAR_TO_STRING);
            } else if (cast.from.clazz == String.class && cast.to.clazz == char.class) {
                invokeStatic(UTILITY_TYPE, STRING_TO_CHAR);
            } else if (cast.unboxFrom != null) {
                unbox(cast.unboxFrom.type);
                writeCast(cast.from, cast.to);
            } else if (cast.unboxTo != null) {
                if (cast.from.dynamic) {
                    if (cast.explicit) {
                        if      (cast.to.clazz == Boolean.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_BOOLEAN);
                        else if (cast.to.clazz == Byte.class)    invokeStatic(DEF_UTIL_TYPE, DEF_TO_BYTE_EXPLICIT);
                        else if (cast.to.clazz == Short.class)   invokeStatic(DEF_UTIL_TYPE, DEF_TO_SHORT_EXPLICIT);
                        else if (cast.to.clazz == Character.class)    invokeStatic(DEF_UTIL_TYPE, DEF_TO_CHAR_EXPLICIT);
                        else if (cast.to.clazz == Integer.class)     invokeStatic(DEF_UTIL_TYPE, DEF_TO_INT_EXPLICIT);
                        else if (cast.to.clazz == Long.class)    invokeStatic(DEF_UTIL_TYPE, DEF_TO_LONG_EXPLICIT);
                        else if (cast.to.clazz == Float.class)   invokeStatic(DEF_UTIL_TYPE, DEF_TO_FLOAT_EXPLICIT);
                        else if (cast.to.clazz == Double.class)  invokeStatic(DEF_UTIL_TYPE, DEF_TO_DOUBLE_EXPLICIT);
                        else {
                            throw new IllegalStateException("Illegal tree structure.");
                        }
                    } else {
                        if      (cast.to.clazz == Boolean.class) invokeStatic(DEF_UTIL_TYPE, DEF_TO_BOOLEAN);
                        else if (cast.to.clazz == Byte.class)    invokeStatic(DEF_UTIL_TYPE, DEF_TO_BYTE_IMPLICIT);
                        else if (cast.to.clazz == Short.class)   invokeStatic(DEF_UTIL_TYPE, DEF_TO_SHORT_IMPLICIT);
                        else if (cast.to.clazz == Character.class)    invokeStatic(DEF_UTIL_TYPE, DEF_TO_CHAR_IMPLICIT);
                        else if (cast.to.clazz == Integer.class)     invokeStatic(DEF_UTIL_TYPE, DEF_TO_INT_IMPLICIT);
                        else if (cast.to.clazz == Long.class)    invokeStatic(DEF_UTIL_TYPE, DEF_TO_LONG_IMPLICIT);
                        else if (cast.to.clazz == Float.class)   invokeStatic(DEF_UTIL_TYPE, DEF_TO_FLOAT_IMPLICIT);
                        else if (cast.to.clazz == Double.class)  invokeStatic(DEF_UTIL_TYPE, DEF_TO_DOUBLE_IMPLICIT);
                        else {
                            throw new IllegalStateException("Illegal tree structure.");
                        }
                    }
                } else {
                    writeCast(cast.from, cast.to);
                    unbox(cast.unboxTo.type);
                }
            } else if (cast.boxFrom != null) {
                box(cast.boxFrom.type);
                writeCast(cast.from, cast.to);
            } else if (cast.boxTo != null) {
                writeCast(cast.from, cast.to);
                box(cast.boxTo.type);
            } else {
                writeCast(cast.from, cast.to);
            }
        }
    }

    private void writeCast(final Type from, final Type to) {
        if (from.equals(to)) {
            return;
        }

        if (from.clazz != boolean.class && from.clazz.isPrimitive() && to.clazz != boolean.class && to.clazz.isPrimitive()) {
            cast(from.type, to.type);
        } else {
            if (!to.clazz.isAssignableFrom(from.clazz)) {
                checkCast(to.type);
            }
        }
    }

    /**
     * Proxy the box method to use valueOf instead to ensure that the modern boxing methods are used.
     */
    @Override
    public void box(org.objectweb.asm.Type type) {
        valueOf(type);
    }

    public void writeBranch(final Label tru, final Label fals) {
        if (tru != null) {
            visitJumpInsn(Opcodes.IFNE, tru);
        } else if (fals != null) {
            visitJumpInsn(Opcodes.IFEQ, fals);
        }
    }

    /** Starts a new string concat.
     * @return the size of arguments pushed to stack (the object that does string concats, e.g. a StringBuilder)
     */
    public int writeNewStrings() {
        if (INDY_STRING_CONCAT_BOOTSTRAP_HANDLE != null) {
            // Java 9+: we just push our argument collector onto deque
            stringConcatArgs.push(new ArrayList<>());
            return 0; // nothing added to stack
        } else {
            // Java 8: create a StringBuilder in bytecode
            newInstance(STRINGBUILDER_TYPE);
            dup();
            invokeConstructor(STRINGBUILDER_TYPE, STRINGBUILDER_CONSTRUCTOR);
            return 1; // StringBuilder on stack
        }
    }

    public void writeAppendStrings(final Type type) {
        if (INDY_STRING_CONCAT_BOOTSTRAP_HANDLE != null) {
            // Java 9+: record type information
            stringConcatArgs.peek().add(type.type);
            // prevent too many concat args.
            // If there are too many, do the actual concat:
            if (stringConcatArgs.peek().size() >= MAX_INDY_STRING_CONCAT_ARGS) {
                writeToStrings();
                writeNewStrings();
                // add the return value type as new first param for next concat:
                stringConcatArgs.peek().add(STRING_TYPE);
            }
        } else {
            // Java 8: push a StringBuilder append
            if (type.clazz == boolean.class)      invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_BOOLEAN);
            else if (type.clazz == char.class)    invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_CHAR);
            else if (type.clazz == byte.class  ||
                     type.clazz == short.class ||
                     type.clazz == int.class)     invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_INT);
            else if (type.clazz == long.class)    invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_LONG);
            else if (type.clazz == float.class)   invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_FLOAT);
            else if (type.clazz == double.class)  invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_DOUBLE);
            else if (type.clazz == String.class)  invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_STRING);
            else                                  invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_OBJECT);
        }
    }

    public void writeToStrings() {
        if (INDY_STRING_CONCAT_BOOTSTRAP_HANDLE != null) {
            // Java 9+: use type information and push invokeDynamic
            final String desc = org.objectweb.asm.Type.getMethodDescriptor(STRING_TYPE,
                    stringConcatArgs.pop().stream().toArray(org.objectweb.asm.Type[]::new));
            invokeDynamic("concat", desc, INDY_STRING_CONCAT_BOOTSTRAP_HANDLE);
        } else {
            // Java 8: call toString() on StringBuilder
            invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_TOSTRING);
        }
    }

    /** Writes a dynamic binary instruction: returnType, lhs, and rhs can be different */
    public void writeDynamicBinaryInstruction(Location location, Type returnType, Type lhs, Type rhs,
                                              Operation operation, int flags) {
        org.objectweb.asm.Type methodType = org.objectweb.asm.Type.getMethodType(returnType.type, lhs.type, rhs.type);

        switch (operation) {
            case MUL:
                invokeDefCall("mul", methodType, DefBootstrap.BINARY_OPERATOR, flags);
                break;
            case DIV:
                invokeDefCall("div", methodType, DefBootstrap.BINARY_OPERATOR, flags);
                break;
            case REM:
                invokeDefCall("rem", methodType, DefBootstrap.BINARY_OPERATOR, flags);
                break;
            case ADD:
                // if either side is primitive, then the + operator should always throw NPE on null,
                // so we don't need a special NPE guard.
                // otherwise, we need to allow nulls for possible string concatenation.
                boolean hasPrimitiveArg = lhs.clazz.isPrimitive() || rhs.clazz.isPrimitive();
                if (!hasPrimitiveArg) {
                    flags |= DefBootstrap.OPERATOR_ALLOWS_NULL;
                }
                invokeDefCall("add", methodType, DefBootstrap.BINARY_OPERATOR, flags);
                break;
            case SUB:
                invokeDefCall("sub", methodType, DefBootstrap.BINARY_OPERATOR, flags);
                break;
            case LSH:
                invokeDefCall("lsh", methodType, DefBootstrap.SHIFT_OPERATOR, flags);
                break;
            case USH:
                invokeDefCall("ush", methodType, DefBootstrap.SHIFT_OPERATOR, flags);
                break;
            case RSH:
                invokeDefCall("rsh", methodType, DefBootstrap.SHIFT_OPERATOR, flags);
                break;
            case BWAND:
                invokeDefCall("and", methodType, DefBootstrap.BINARY_OPERATOR, flags);
                break;
            case XOR:
                invokeDefCall("xor", methodType, DefBootstrap.BINARY_OPERATOR, flags);
                break;
            case BWOR:
                invokeDefCall("or", methodType, DefBootstrap.BINARY_OPERATOR, flags);
                break;
            default:
                throw location.createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    /** Writes a static binary instruction */
    public void writeBinaryInstruction(Location location, Type type, Operation operation) {
        if ((type.clazz == float.class || type.clazz == double.class) &&
                (operation == Operation.LSH || operation == Operation.USH ||
                operation == Operation.RSH || operation == Operation.BWAND ||
                operation == Operation.XOR || operation == Operation.BWOR)) {
            throw location.createError(new IllegalStateException("Illegal tree structure."));
        }

        switch (operation) {
            case MUL:   math(GeneratorAdapter.MUL,  type.type); break;
            case DIV:   math(GeneratorAdapter.DIV,  type.type); break;
            case REM:   math(GeneratorAdapter.REM,  type.type); break;
            case ADD:   math(GeneratorAdapter.ADD,  type.type); break;
            case SUB:   math(GeneratorAdapter.SUB,  type.type); break;
            case LSH:   math(GeneratorAdapter.SHL,  type.type); break;
            case USH:   math(GeneratorAdapter.USHR, type.type); break;
            case RSH:   math(GeneratorAdapter.SHR,  type.type); break;
            case BWAND: math(GeneratorAdapter.AND,  type.type); break;
            case XOR:   math(GeneratorAdapter.XOR,  type.type); break;
            case BWOR:  math(GeneratorAdapter.OR,   type.type); break;
            default:
                throw location.createError(new IllegalStateException("Illegal tree structure."));
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
        if (stringConcatArgs != null && !stringConcatArgs.isEmpty()) {
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
    public void invokeDefCall(String name, org.objectweb.asm.Type methodType, int flavor, Object... params) {
        Object[] args = new Object[params.length + 2];
        args[0] = settings.getInitialCallSiteDepth();
        args[1] = flavor;
        System.arraycopy(params, 0, args, 2, params.length);
        invokeDynamic(name, methodType.getDescriptor(), DEF_BOOTSTRAP_HANDLE, args);
    }
}

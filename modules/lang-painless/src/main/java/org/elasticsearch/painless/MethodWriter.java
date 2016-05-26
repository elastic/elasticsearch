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
import org.elasticsearch.painless.Definition.Sort;
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
import static org.elasticsearch.painless.WriterConstants.DEF_ADD_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_AND_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_DIV_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_LSH_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_MUL_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_OR_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_REM_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_RSH_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_SUB_CALL;
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
import static org.elasticsearch.painless.WriterConstants.DEF_USH_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_UTIL_TYPE;
import static org.elasticsearch.painless.WriterConstants.DEF_XOR_CALL;
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

    private final Deque<List<org.objectweb.asm.Type>> stringConcatArgs = (INDY_STRING_CONCAT_BOOTSTRAP_HANDLE == null) ?
            null : new ArrayDeque<>();

    MethodWriter(int access, Method method, org.objectweb.asm.Type[] exceptions, ClassVisitor cv, BitSet statements) {
        super(Opcodes.ASM5, cv.visitMethod(access, method.getName(), method.getDescriptor(), null, getInternalNames(exceptions)),
                access, method.getName(), method.getDescriptor());
        this.statements = statements;
    }

    private static String[] getInternalNames(final org.objectweb.asm.Type[] types) {
        if (types == null) {
            return null;
        }
        String[] names = new String[types.length];
        for (int i = 0; i < names.length; ++i) {
            names[i] = types[i].getInternalName();
        }
        return names;
    }

    /** 
     * Marks a new statement boundary. 
     * <p>
     * This is invoked for each statement boundary (leaf {@code S*} nodes).
     */
    public void writeStatementOffset(int offset) {
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
    public void writeDebugInfo(int offset) {
        // TODO: maybe track these in bitsets too? this is trickier...
        Label label = new Label();
        visitLabel(label);
        visitLineNumber(offset + 1, label);
    }

    public void writeLoopCounter(int slot, int count, int offset) {
        if (slot > -1) {
            writeDebugInfo(offset);
            final Label end = new Label();

            iinc(slot, -count);
            visitVarInsn(Opcodes.ILOAD, slot);
            push(0);
            ifICmp(GeneratorAdapter.GT, end);
            throwException(PAINLESS_ERROR_TYPE, "The maximum number of statements that can be executed in a loop has been reached.");
            mark(end);
        }
    }

    public void writeCast(final Cast cast) {
        if (cast != null) {
            final Type from = cast.from;
            final Type to = cast.to;

            if (from.sort == Sort.CHAR && to.sort == Sort.STRING) {
                invokeStatic(UTILITY_TYPE, CHAR_TO_STRING);
            } else if (from.sort == Sort.STRING && to.sort == Sort.CHAR) {
                invokeStatic(UTILITY_TYPE, STRING_TO_CHAR);
            } else if (cast.unboxFrom) {
                if (from.sort == Sort.DEF) {
                    if (cast.explicit) {
                        if      (to.sort == Sort.BOOL)   invokeStatic(DEF_UTIL_TYPE, DEF_TO_BOOLEAN);
                        else if (to.sort == Sort.BYTE)   invokeStatic(DEF_UTIL_TYPE, DEF_TO_BYTE_EXPLICIT);
                        else if (to.sort == Sort.SHORT)  invokeStatic(DEF_UTIL_TYPE, DEF_TO_SHORT_EXPLICIT);
                        else if (to.sort == Sort.CHAR)   invokeStatic(DEF_UTIL_TYPE, DEF_TO_CHAR_EXPLICIT);
                        else if (to.sort == Sort.INT)    invokeStatic(DEF_UTIL_TYPE, DEF_TO_INT_EXPLICIT);
                        else if (to.sort == Sort.LONG)   invokeStatic(DEF_UTIL_TYPE, DEF_TO_LONG_EXPLICIT);
                        else if (to.sort == Sort.FLOAT)  invokeStatic(DEF_UTIL_TYPE, DEF_TO_FLOAT_EXPLICIT);
                        else if (to.sort == Sort.DOUBLE) invokeStatic(DEF_UTIL_TYPE, DEF_TO_DOUBLE_EXPLICIT);
                        else throw new IllegalStateException("Illegal tree structure.");
                    } else {
                        if      (to.sort == Sort.BOOL)   invokeStatic(DEF_UTIL_TYPE, DEF_TO_BOOLEAN);
                        else if (to.sort == Sort.BYTE)   invokeStatic(DEF_UTIL_TYPE, DEF_TO_BYTE_IMPLICIT);
                        else if (to.sort == Sort.SHORT)  invokeStatic(DEF_UTIL_TYPE, DEF_TO_SHORT_IMPLICIT);
                        else if (to.sort == Sort.CHAR)   invokeStatic(DEF_UTIL_TYPE, DEF_TO_CHAR_IMPLICIT);
                        else if (to.sort == Sort.INT)    invokeStatic(DEF_UTIL_TYPE, DEF_TO_INT_IMPLICIT);
                        else if (to.sort == Sort.LONG)   invokeStatic(DEF_UTIL_TYPE, DEF_TO_LONG_IMPLICIT);
                        else if (to.sort == Sort.FLOAT)  invokeStatic(DEF_UTIL_TYPE, DEF_TO_FLOAT_IMPLICIT);
                        else if (to.sort == Sort.DOUBLE) invokeStatic(DEF_UTIL_TYPE, DEF_TO_DOUBLE_IMPLICIT);
                        else throw new IllegalStateException("Illegal tree structure.");
                    }
                } else {
                    unbox(from.type);
                    writeCast(from, to);
                }
            } else if (cast.unboxTo) {
                writeCast(from, to);
                unbox(to.type);
            } else if (cast.boxFrom) {
                box(from.type);
                writeCast(from, to);
            } else if (cast.boxTo) {
                writeCast(from, to);
                box(to.type);
            } else {
                writeCast(from, to);
            }
        }
    }

    private void writeCast(final Type from, final Type to) {
        if (from.equals(to)) {
            return;
        }

        if (from.sort.numeric && from.sort.primitive && to.sort.numeric && to.sort.primitive) {
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

    public void writeNewStrings() {
        if (INDY_STRING_CONCAT_BOOTSTRAP_HANDLE != null) {
            // Java 9+: we just push our argument collector onto deque
            stringConcatArgs.push(new ArrayList<>());
        } else {
            // Java 8: create a StringBuilder in bytecode
            newInstance(STRINGBUILDER_TYPE);
            dup();
            invokeConstructor(STRINGBUILDER_TYPE, STRINGBUILDER_CONSTRUCTOR);
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
            switch (type.sort) {
                case BOOL:   invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_BOOLEAN); break;
                case CHAR:   invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_CHAR);    break;
                case BYTE:
                case SHORT:
                case INT:    invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_INT);     break;
                case LONG:   invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_LONG);    break;
                case FLOAT:  invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_FLOAT);   break;
                case DOUBLE: invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_DOUBLE);  break;
                case STRING: invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_STRING);  break;
                default:     invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_OBJECT);
            }
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

    public void writeBinaryInstruction(final String location, final Type type, final Operation operation) {
        final Sort sort = type.sort;

        if ((sort == Sort.FLOAT || sort == Sort.DOUBLE) &&
                (operation == Operation.LSH || operation == Operation.USH ||
                operation == Operation.RSH || operation == Operation.BWAND ||
                operation == Operation.XOR || operation == Operation.BWOR)) {
            throw new IllegalStateException("Error " + location + ": Illegal tree structure.");
        }

        if (sort == Sort.DEF) {
            switch (operation) {
                case MUL:   invokeStatic(DEF_UTIL_TYPE, DEF_MUL_CALL); break;
                case DIV:   invokeStatic(DEF_UTIL_TYPE, DEF_DIV_CALL); break;
                case REM:   invokeStatic(DEF_UTIL_TYPE, DEF_REM_CALL); break;
                case ADD:   invokeStatic(DEF_UTIL_TYPE, DEF_ADD_CALL); break;
                case SUB:   invokeStatic(DEF_UTIL_TYPE, DEF_SUB_CALL); break;
                case LSH:   invokeStatic(DEF_UTIL_TYPE, DEF_LSH_CALL); break;
                case USH:   invokeStatic(DEF_UTIL_TYPE, DEF_RSH_CALL); break;
                case RSH:   invokeStatic(DEF_UTIL_TYPE, DEF_USH_CALL); break;
                case BWAND: invokeStatic(DEF_UTIL_TYPE, DEF_AND_CALL); break;
                case XOR:   invokeStatic(DEF_UTIL_TYPE, DEF_XOR_CALL); break;
                case BWOR:  invokeStatic(DEF_UTIL_TYPE, DEF_OR_CALL);  break;
                default:
                    throw new IllegalStateException("Error " + location + ": Illegal tree structure.");
            }
        } else {
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
                    throw new IllegalStateException("Error " + location + ": Illegal tree structure.");
            }
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
    public void visitEnd() {
        if (stringConcatArgs != null && !stringConcatArgs.isEmpty()) {
            throw new IllegalStateException("String concat bytecode not completed.");
        }
        super.visitEnd();
    }

}

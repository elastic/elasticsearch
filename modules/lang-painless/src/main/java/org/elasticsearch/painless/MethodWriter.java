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
import org.elasticsearch.painless.Definition.Transform;
import org.elasticsearch.painless.Definition.Type;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.elasticsearch.painless.WriterConstants.ADDEXACT_INT;
import static org.elasticsearch.painless.WriterConstants.ADDEXACT_LONG;
import static org.elasticsearch.painless.WriterConstants.ADDWOOVERLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.ADDWOOVERLOW_FLOAT;
import static org.elasticsearch.painless.WriterConstants.DEF_ADD_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_AND_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_DIV_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_LSH_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_MUL_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_OR_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_REM_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_RSH_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_SUB_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_USH_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_XOR_CALL;
import static org.elasticsearch.painless.WriterConstants.DIVWOOVERLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.DIVWOOVERLOW_FLOAT;
import static org.elasticsearch.painless.WriterConstants.DIVWOOVERLOW_INT;
import static org.elasticsearch.painless.WriterConstants.DIVWOOVERLOW_LONG;
import static org.elasticsearch.painless.WriterConstants.INDY_STRING_CONCAT_BOOTSTRAP_HANDLE;
import static org.elasticsearch.painless.WriterConstants.MAX_INDY_STRING_CONCAT_ARGS;
import static org.elasticsearch.painless.WriterConstants.MULEXACT_INT;
import static org.elasticsearch.painless.WriterConstants.MULEXACT_LONG;
import static org.elasticsearch.painless.WriterConstants.MULWOOVERLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.MULWOOVERLOW_FLOAT;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.REMWOOVERLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.REMWOOVERLOW_FLOAT;
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
import static org.elasticsearch.painless.WriterConstants.STRING_TYPE;
import static org.elasticsearch.painless.WriterConstants.SUBEXACT_INT;
import static org.elasticsearch.painless.WriterConstants.SUBEXACT_LONG;
import static org.elasticsearch.painless.WriterConstants.SUBWOOVERLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.SUBWOOVERLOW_FLOAT;
import static org.elasticsearch.painless.WriterConstants.TOBYTEEXACT_INT;
import static org.elasticsearch.painless.WriterConstants.TOBYTEEXACT_LONG;
import static org.elasticsearch.painless.WriterConstants.TOBYTEWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.TOBYTEWOOVERFLOW_FLOAT;
import static org.elasticsearch.painless.WriterConstants.TOCHAREXACT_INT;
import static org.elasticsearch.painless.WriterConstants.TOCHAREXACT_LONG;
import static org.elasticsearch.painless.WriterConstants.TOCHARWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.TOCHARWOOVERFLOW_FLOAT;
import static org.elasticsearch.painless.WriterConstants.TOFLOATWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.TOINTEXACT_LONG;
import static org.elasticsearch.painless.WriterConstants.TOINTWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.TOINTWOOVERFLOW_FLOAT;
import static org.elasticsearch.painless.WriterConstants.TOLONGWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.TOLONGWOOVERFLOW_FLOAT;
import static org.elasticsearch.painless.WriterConstants.TOSHORTEXACT_INT;
import static org.elasticsearch.painless.WriterConstants.TOSHORTEXACT_LONG;
import static org.elasticsearch.painless.WriterConstants.TOSHORTWOOVERFLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.TOSHORTWOOVERFLOW_FLOAT;

/**
 * Extension of {@link GeneratorAdapter} with some utility methods.
 * <p>
 * Set of methods used during the writing phase of compilation
 * shared by the nodes of the Painless tree.
 */
public final class MethodWriter extends GeneratorAdapter {

    private final Deque<List<org.objectweb.asm.Type>> stringConcatArgs = (INDY_STRING_CONCAT_BOOTSTRAP_HANDLE == null) ? null : new ArrayDeque<>();

    MethodWriter(int access, Method method, org.objectweb.asm.Type[] exceptions, ClassVisitor cv) {
        super(Opcodes.ASM5, cv.visitMethod(access, method.getName(), method.getDescriptor(), null, getInternalNames(exceptions)),
                access, method.getName(), method.getDescriptor());
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

    public void writeLoopCounter(final int slot, final int count) {
        if (slot > -1) {
            final Label end = new Label();

            iinc(slot, -count);
            visitVarInsn(Opcodes.ILOAD, slot);
            push(0);
            ifICmp(GeneratorAdapter.GT, end);
            throwException(PAINLESS_ERROR_TYPE,
                "The maximum number of statements that can be executed in a loop has been reached.");
            mark(end);
        }
    }

    public void writeCast(final Cast cast) {
        if (cast instanceof Transform) {
            final Transform transform = (Transform)cast;

            if (transform.upcast != null) {
                checkCast(transform.upcast.type);
            }

            if (java.lang.reflect.Modifier.isStatic(transform.method.reflect.getModifiers())) {
                invokeStatic(transform.method.owner.type, transform.method.method);
            } else if (java.lang.reflect.Modifier.isInterface(transform.method.owner.clazz.getModifiers())) {
                invokeInterface(transform.method.owner.type, transform.method.method);
            } else {
                invokeVirtual(transform.method.owner.type, transform.method.method);
            }

            if (transform.downcast != null) {
                checkCast(transform.downcast.type);
            }
        } else if (cast != null) {
            final Type from = cast.from;
            final Type to = cast.to;

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

    public void writeBinaryInstruction(final CompilerSettings settings, final Definition definition,
                                              final String location,
                                              final Type type, final Operation operation) {
        final Sort sort = type.sort;
        boolean exact = !settings.getNumericOverflow() &&
            ((sort == Sort.INT || sort == Sort.LONG) &&
                (operation == Operation.MUL || operation == Operation.DIV ||
                    operation == Operation.ADD || operation == Operation.SUB) ||
                (sort == Sort.FLOAT || sort == Sort.DOUBLE) &&
                    (operation == Operation.MUL || operation == Operation.DIV || operation == Operation.REM ||
                        operation == Operation.ADD || operation == Operation.SUB));

        if (exact) {
            switch (sort) {
                case INT:
                    switch (operation) {
                        case MUL: invokeStatic(definition.mathType.type,    MULEXACT_INT);     break;
                        case DIV: invokeStatic(definition.utilityType.type, DIVWOOVERLOW_INT); break;
                        case ADD: invokeStatic(definition.mathType.type,    ADDEXACT_INT);     break;
                        case SUB: invokeStatic(definition.mathType.type,    SUBEXACT_INT);     break;
                    }

                    break;
                case LONG:
                    switch (operation) {
                        case MUL: invokeStatic(definition.mathType.type,    MULEXACT_LONG);     break;
                        case DIV: invokeStatic(definition.utilityType.type, DIVWOOVERLOW_LONG); break;
                        case ADD: invokeStatic(definition.mathType.type,    ADDEXACT_LONG);     break;
                        case SUB: invokeStatic(definition.mathType.type,    SUBEXACT_LONG);     break;
                    }

                    break;
                case FLOAT:
                    switch (operation) {
                        case MUL: invokeStatic(definition.utilityType.type, MULWOOVERLOW_FLOAT); break;
                        case DIV: invokeStatic(definition.utilityType.type, DIVWOOVERLOW_FLOAT); break;
                        case REM: invokeStatic(definition.utilityType.type, REMWOOVERLOW_FLOAT); break;
                        case ADD: invokeStatic(definition.utilityType.type, ADDWOOVERLOW_FLOAT); break;
                        case SUB: invokeStatic(definition.utilityType.type, SUBWOOVERLOW_FLOAT); break;
                        default:
                            throw new IllegalStateException("Error " + location + ": Illegal tree structure.");
                    }

                    break;
                case DOUBLE:
                    switch (operation) {
                        case MUL: invokeStatic(definition.utilityType.type, MULWOOVERLOW_DOUBLE); break;
                        case DIV: invokeStatic(definition.utilityType.type, DIVWOOVERLOW_DOUBLE); break;
                        case REM: invokeStatic(definition.utilityType.type, REMWOOVERLOW_DOUBLE); break;
                        case ADD: invokeStatic(definition.utilityType.type, ADDWOOVERLOW_DOUBLE); break;
                        case SUB: invokeStatic(definition.utilityType.type, SUBWOOVERLOW_DOUBLE); break;
                        default:
                            throw new IllegalStateException("Error " + location + ": Illegal tree structure.");
                    }

                    break;
                default:
                    throw new IllegalStateException("Error " + location + ": Illegal tree structure.");
            }
        } else {
            if ((sort == Sort.FLOAT || sort == Sort.DOUBLE) &&
                (operation == Operation.LSH || operation == Operation.USH ||
                    operation == Operation.RSH || operation == Operation.BWAND ||
                    operation == Operation.XOR || operation == Operation.BWOR)) {
                throw new IllegalStateException("Error " + location + ": Illegal tree structure.");
            }

            if (sort == Sort.DEF) {
                switch (operation) {
                    case MUL:   invokeStatic(definition.defobjType.type, DEF_MUL_CALL); break;
                    case DIV:   invokeStatic(definition.defobjType.type, DEF_DIV_CALL); break;
                    case REM:   invokeStatic(definition.defobjType.type, DEF_REM_CALL); break;
                    case ADD:   invokeStatic(definition.defobjType.type, DEF_ADD_CALL); break;
                    case SUB:   invokeStatic(definition.defobjType.type, DEF_SUB_CALL); break;
                    case LSH:   invokeStatic(definition.defobjType.type, DEF_LSH_CALL); break;
                    case USH:   invokeStatic(definition.defobjType.type, DEF_RSH_CALL); break;
                    case RSH:   invokeStatic(definition.defobjType.type, DEF_USH_CALL); break;
                    case BWAND: invokeStatic(definition.defobjType.type, DEF_AND_CALL); break;
                    case XOR:   invokeStatic(definition.defobjType.type, DEF_XOR_CALL); break;
                    case BWOR:  invokeStatic(definition.defobjType.type, DEF_OR_CALL);  break;
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
    }

    /**
     * Called for any compound assignment (including increment/decrement instructions).
     * We have to be stricter than writeBinary and do overflow checks against the original type's size
     * instead of the promoted type's size, since the result will be implicitly cast back.
     *
     * @return This will be true if an instruction is written, false otherwise.
     */
    public boolean writeExactInstruction(
        final Definition definition, final Sort fsort, final Sort tsort) {
        if (fsort == Sort.DOUBLE) {
            if (tsort == Sort.FLOAT) {
                invokeStatic(definition.utilityType.type, TOFLOATWOOVERFLOW_DOUBLE);
            } else if (tsort == Sort.FLOAT_OBJ) {
                invokeStatic(definition.utilityType.type, TOFLOATWOOVERFLOW_DOUBLE);
                checkCast(definition.floatobjType.type);
            } else if (tsort == Sort.LONG) {
                invokeStatic(definition.utilityType.type, TOLONGWOOVERFLOW_DOUBLE);
            } else if (tsort == Sort.LONG_OBJ) {
                invokeStatic(definition.utilityType.type, TOLONGWOOVERFLOW_DOUBLE);
                checkCast(definition.longobjType.type);
            } else if (tsort == Sort.INT) {
                invokeStatic(definition.utilityType.type, TOINTWOOVERFLOW_DOUBLE);
            } else if (tsort == Sort.INT_OBJ) {
                invokeStatic(definition.utilityType.type, TOINTWOOVERFLOW_DOUBLE);
                checkCast(definition.intobjType.type);
            } else if (tsort == Sort.CHAR) {
                invokeStatic(definition.utilityType.type, TOCHARWOOVERFLOW_DOUBLE);
            } else if (tsort == Sort.CHAR_OBJ) {
                invokeStatic(definition.utilityType.type, TOCHARWOOVERFLOW_DOUBLE);
                checkCast(definition.charobjType.type);
            } else if (tsort == Sort.SHORT) {
                invokeStatic(definition.utilityType.type, TOSHORTWOOVERFLOW_DOUBLE);
            } else if (tsort == Sort.SHORT_OBJ) {
                invokeStatic(definition.utilityType.type, TOSHORTWOOVERFLOW_DOUBLE);
                checkCast(definition.shortobjType.type);
            } else if (tsort == Sort.BYTE) {
                invokeStatic(definition.utilityType.type, TOBYTEWOOVERFLOW_DOUBLE);
            } else if (tsort == Sort.BYTE_OBJ) {
                invokeStatic(definition.utilityType.type, TOBYTEWOOVERFLOW_DOUBLE);
                checkCast(definition.byteobjType.type);
            } else {
                return false;
            }
        } else if (fsort == Sort.FLOAT) {
            if (tsort == Sort.LONG) {
                invokeStatic(definition.utilityType.type, TOLONGWOOVERFLOW_FLOAT);
            } else if (tsort == Sort.LONG_OBJ) {
                invokeStatic(definition.utilityType.type, TOLONGWOOVERFLOW_FLOAT);
                checkCast(definition.longobjType.type);
            } else if (tsort == Sort.INT) {
                invokeStatic(definition.utilityType.type, TOINTWOOVERFLOW_FLOAT);
            } else if (tsort == Sort.INT_OBJ) {
                invokeStatic(definition.utilityType.type, TOINTWOOVERFLOW_FLOAT);
                checkCast(definition.intobjType.type);
            } else if (tsort == Sort.CHAR) {
                invokeStatic(definition.utilityType.type, TOCHARWOOVERFLOW_FLOAT);
            } else if (tsort == Sort.CHAR_OBJ) {
                invokeStatic(definition.utilityType.type, TOCHARWOOVERFLOW_FLOAT);
                checkCast(definition.charobjType.type);
            } else if (tsort == Sort.SHORT) {
                invokeStatic(definition.utilityType.type, TOSHORTWOOVERFLOW_FLOAT);
            } else if (tsort == Sort.SHORT_OBJ) {
                invokeStatic(definition.utilityType.type, TOSHORTWOOVERFLOW_FLOAT);
                checkCast(definition.shortobjType.type);
            } else if (tsort == Sort.BYTE) {
                invokeStatic(definition.utilityType.type, TOBYTEWOOVERFLOW_FLOAT);
            } else if (tsort == Sort.BYTE_OBJ) {
                invokeStatic(definition.utilityType.type, TOBYTEWOOVERFLOW_FLOAT);
                checkCast(definition.byteobjType.type);
            } else {
                return false;
            }
        } else if (fsort == Sort.LONG) {
            if (tsort == Sort.INT) {
                invokeStatic(definition.mathType.type, TOINTEXACT_LONG);
            } else if (tsort == Sort.INT_OBJ) {
                invokeStatic(definition.mathType.type, TOINTEXACT_LONG);
                checkCast(definition.intobjType.type);
            } else if (tsort == Sort.CHAR) {
                invokeStatic(definition.utilityType.type, TOCHAREXACT_LONG);
            } else if (tsort == Sort.CHAR_OBJ) {
                invokeStatic(definition.utilityType.type, TOCHAREXACT_LONG);
                checkCast(definition.charobjType.type);
            } else if (tsort == Sort.SHORT) {
                invokeStatic(definition.utilityType.type, TOSHORTEXACT_LONG);
            } else if (tsort == Sort.SHORT_OBJ) {
                invokeStatic(definition.utilityType.type, TOSHORTEXACT_LONG);
                checkCast(definition.shortobjType.type);
            } else if (tsort == Sort.BYTE) {
                invokeStatic(definition.utilityType.type, TOBYTEEXACT_LONG);
            } else if (tsort == Sort.BYTE_OBJ) {
                invokeStatic(definition.utilityType.type, TOBYTEEXACT_LONG);
                checkCast(definition.byteobjType.type);
            } else {
                return false;
            }
        } else if (fsort == Sort.INT) {
            if (tsort == Sort.CHAR) {
                invokeStatic(definition.utilityType.type, TOCHAREXACT_INT);
            } else if (tsort == Sort.CHAR_OBJ) {
                invokeStatic(definition.utilityType.type, TOCHAREXACT_INT);
                checkCast(definition.charobjType.type);
            } else if (tsort == Sort.SHORT) {
                invokeStatic(definition.utilityType.type, TOSHORTEXACT_INT);
            } else if (tsort == Sort.SHORT_OBJ) {
                invokeStatic(definition.utilityType.type, TOSHORTEXACT_INT);
                checkCast(definition.shortobjType.type);
            } else if (tsort == Sort.BYTE) {
                invokeStatic(definition.utilityType.type, TOBYTEEXACT_INT);
            } else if (tsort == Sort.BYTE_OBJ) {
                invokeStatic(definition.utilityType.type, TOBYTEEXACT_INT);
                checkCast(definition.byteobjType.type);
            } else {
                return false;
            }
        } else {
            return false;
        }

        return true;
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

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

import org.antlr.v4.runtime.ParserRuleContext;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.painless.PainlessParser.ADD;
import static org.elasticsearch.painless.PainlessParser.BWAND;
import static org.elasticsearch.painless.PainlessParser.BWOR;
import static org.elasticsearch.painless.PainlessParser.BWXOR;
import static org.elasticsearch.painless.PainlessParser.DIV;
import static org.elasticsearch.painless.PainlessParser.LSH;
import static org.elasticsearch.painless.PainlessParser.MUL;
import static org.elasticsearch.painless.PainlessParser.REM;
import static org.elasticsearch.painless.PainlessParser.RSH;
import static org.elasticsearch.painless.PainlessParser.SUB;
import static org.elasticsearch.painless.PainlessParser.USH;
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
import static org.elasticsearch.painless.WriterConstants.MULEXACT_INT;
import static org.elasticsearch.painless.WriterConstants.MULEXACT_LONG;
import static org.elasticsearch.painless.WriterConstants.MULWOOVERLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.MULWOOVERLOW_FLOAT;
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
import static org.elasticsearch.painless.WriterConstants.SUBEXACT_INT;
import static org.elasticsearch.painless.WriterConstants.SUBEXACT_LONG;
import static org.elasticsearch.painless.WriterConstants.SUBWOOVERLOW_DOUBLE;
import static org.elasticsearch.painless.WriterConstants.SUBWOOVERLOW_FLOAT;

class WriterUtility {
    static class Branch {
        final ParserRuleContext source;

        Label begin = null;
        Label end = null;
        Label tru = null;
        Label fals = null;

        private Branch(final ParserRuleContext source) {
            this.source = source;
        }
    }

    /**
     * A utility method to output consistent error messages.
     * @param ctx The ANTLR node the error occurred in.
     * @return The error message with tacked on line number and character position.
     */
    static String error(final ParserRuleContext ctx) {
        return "Writer Error [" + ctx.getStart().getLine() + ":" + ctx.getStart().getCharPositionInLine() + "]: ";
    }

    private final Definition definition;
    private final CompilerSettings settings;

    private final GeneratorAdapter execute;

    private final Map<ParserRuleContext, Branch> branches = new HashMap<>();
    private final Deque<Branch> jumps = new ArrayDeque<>();
    private final Set<ParserRuleContext> strings = new HashSet<>();

    WriterUtility(final Metadata metadata, final GeneratorAdapter execute) {
        definition = metadata.definition;
        settings = metadata.settings;

        this.execute = execute;
    }

    Branch markBranch(final ParserRuleContext source, final ParserRuleContext... nodes) {
        final Branch branch = new Branch(source);

        for (final ParserRuleContext node : nodes) {
            branches.put(node, branch);
        }

        return branch;
    }

    void copyBranch(final Branch branch, final ParserRuleContext... nodes) {
        for (final ParserRuleContext node : nodes) {
            branches.put(node, branch);
        }
    }

    Branch getBranch(final ParserRuleContext source) {
        return branches.get(source);
    }

    void checkWriteBranch(final ParserRuleContext source) {
        final Branch branch = getBranch(source);

        if (branch != null) {
            if (branch.tru != null) {
                execute.visitJumpInsn(Opcodes.IFNE, branch.tru);
            } else if (branch.fals != null) {
                execute.visitJumpInsn(Opcodes.IFEQ, branch.fals);
            }
        }
    }

    void pushJump(final Branch branch) {
        jumps.push(branch);
    }

    Branch peekJump() {
        return jumps.peek();
    }

    void popJump() {
        jumps.pop();
    }

    void addStrings(final ParserRuleContext source) {
        strings.add(source);
    }

    boolean containsStrings(final ParserRuleContext source) {
        return strings.contains(source);
    }

    void removeStrings(final ParserRuleContext source) {
        strings.remove(source);
    }

    void writeDup(final int size, final boolean x1, final boolean x2) {
        if (size == 1) {
            if (x2) {
                execute.dupX2();
            } else if (x1) {
                execute.dupX1();
            } else {
                execute.dup();
            }
        } else if (size == 2) {
            if (x2) {
                execute.dup2X2();
            } else if (x1) {
                execute.dup2X1();
            } else {
                execute.dup2();
            }
        }
    }

    void writePop(final int size) {
        if (size == 1) {
            execute.pop();
        } else if (size == 2) {
            execute.pop2();
        }
    }

    void writeConstant(final ParserRuleContext source, final Object constant) {
        if (constant instanceof Number) {
            writeNumeric(source, constant);
        } else if (constant instanceof Character) {
            writeNumeric(source, (int)(char)constant);
        } else if (constant instanceof String) {
            writeString(source, constant);
        } else if (constant instanceof Boolean) {
            writeBoolean(source, constant);
        } else if (constant != null) {
            throw new IllegalStateException(WriterUtility.error(source) + "Unexpected state.");
        }
    }

    void writeNumeric(final ParserRuleContext source, final Object numeric) {
        if (numeric instanceof Double) {
            execute.push((double)numeric);
        } else if (numeric instanceof Float) {
            execute.push((float)numeric);
        } else if (numeric instanceof Long) {
            execute.push((long)numeric);
        } else if (numeric instanceof Number) {
            execute.push(((Number)numeric).intValue());
        } else {
            throw new IllegalStateException(WriterUtility.error(source) + "Unexpected state.");
        }
    }

    void writeString(final ParserRuleContext source, final Object string) {
        if (string instanceof String) {
            execute.push((String)string);
        } else {
            throw new IllegalStateException(WriterUtility.error(source) + "Unexpected state.");
        }
    }

    void writeBoolean(final ParserRuleContext source, final Object bool) {
        if (bool instanceof Boolean) {
            execute.push((boolean)bool);
        } else {
            throw new IllegalStateException(WriterUtility.error(source) + "Unexpected state.");
        }
    }

    void writeNewStrings() {
        execute.newInstance(STRINGBUILDER_TYPE);
        execute.dup();
        execute.invokeConstructor(STRINGBUILDER_TYPE, STRINGBUILDER_CONSTRUCTOR);
    }

    void writeAppendStrings(final Sort sort) {
        switch (sort) {
            case BOOL:   execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_BOOLEAN); break;
            case CHAR:   execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_CHAR);    break;
            case BYTE:
            case SHORT:
            case INT:    execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_INT);     break;
            case LONG:   execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_LONG);    break;
            case FLOAT:  execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_FLOAT);   break;
            case DOUBLE: execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_DOUBLE);  break;
            case STRING: execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_STRING);  break;
            default:     execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_APPEND_OBJECT);
        }
    }

    void writeToStrings() {
        execute.invokeVirtual(STRINGBUILDER_TYPE, STRINGBUILDER_TOSTRING);
    }

    void writeBinaryInstruction(final ParserRuleContext source, final Type type, final int token) {
        final Sort sort = type.sort;
        final boolean exact = !settings.getNumericOverflow() &&
            ((sort == Sort.INT || sort == Sort.LONG) &&
                (token == MUL || token == DIV || token == ADD || token == SUB) ||
                (sort == Sort.FLOAT || sort == Sort.DOUBLE) &&
                    (token == MUL || token == DIV || token == REM || token == ADD || token == SUB));

        // If it's a 64-bit shift, fix-up the last argument to truncate to 32-bits.
        // Note that unlike java, this means we still do binary promotion of shifts,
        // but it keeps things simple, and this check works because we promote shifts.
        if (sort == Sort.LONG && (token == LSH || token == USH || token == RSH)) {
            execute.cast(org.objectweb.asm.Type.LONG_TYPE, org.objectweb.asm.Type.INT_TYPE);
        }

        if (exact) {
            switch (sort) {
                case INT:
                    switch (token) {
                        case MUL: execute.invokeStatic(definition.mathType.type,    MULEXACT_INT);     break;
                        case DIV: execute.invokeStatic(definition.utilityType.type, DIVWOOVERLOW_INT); break;
                        case ADD: execute.invokeStatic(definition.mathType.type,    ADDEXACT_INT);     break;
                        case SUB: execute.invokeStatic(definition.mathType.type,    SUBEXACT_INT);     break;
                        default:
                            throw new IllegalStateException(WriterUtility.error(source) + "Unexpected state.");
                    }

                    break;
                case LONG:
                    switch (token) {
                        case MUL: execute.invokeStatic(definition.mathType.type,    MULEXACT_LONG);     break;
                        case DIV: execute.invokeStatic(definition.utilityType.type, DIVWOOVERLOW_LONG); break;
                        case ADD: execute.invokeStatic(definition.mathType.type,    ADDEXACT_LONG);     break;
                        case SUB: execute.invokeStatic(definition.mathType.type,    SUBEXACT_LONG);     break;
                        default:
                            throw new IllegalStateException(WriterUtility.error(source) + "Unexpected state.");
                    }

                    break;
                case FLOAT:
                    switch (token) {
                        case MUL: execute.invokeStatic(definition.utilityType.type, MULWOOVERLOW_FLOAT); break;
                        case DIV: execute.invokeStatic(definition.utilityType.type, DIVWOOVERLOW_FLOAT); break;
                        case REM: execute.invokeStatic(definition.utilityType.type, REMWOOVERLOW_FLOAT); break;
                        case ADD: execute.invokeStatic(definition.utilityType.type, ADDWOOVERLOW_FLOAT); break;
                        case SUB: execute.invokeStatic(definition.utilityType.type, SUBWOOVERLOW_FLOAT); break;
                        default:
                            throw new IllegalStateException(WriterUtility.error(source) + "Unexpected state.");
                    }

                    break;
                case DOUBLE:
                    switch (token) {
                        case MUL: execute.invokeStatic(definition.utilityType.type, MULWOOVERLOW_DOUBLE); break;
                        case DIV: execute.invokeStatic(definition.utilityType.type, DIVWOOVERLOW_DOUBLE); break;
                        case REM: execute.invokeStatic(definition.utilityType.type, REMWOOVERLOW_DOUBLE); break;
                        case ADD: execute.invokeStatic(definition.utilityType.type, ADDWOOVERLOW_DOUBLE); break;
                        case SUB: execute.invokeStatic(definition.utilityType.type, SUBWOOVERLOW_DOUBLE); break;
                        default:
                            throw new IllegalStateException(WriterUtility.error(source) + "Unexpected state.");
                    }

                    break;
                default:
                    throw new IllegalStateException(WriterUtility.error(source) + "Unexpected state.");
            }
        } else {
            if ((sort == Sort.FLOAT || sort == Sort.DOUBLE) &&
                (token == LSH || token == USH || token == RSH || token == BWAND || token == BWXOR || token == BWOR)) {
                throw new IllegalStateException(WriterUtility.error(source) + "Unexpected state.");
            }

            if (sort == Sort.DEF) {
                switch (token) {
                    case MUL:   execute.invokeStatic(definition.defobjType.type, DEF_MUL_CALL); break;
                    case DIV:   execute.invokeStatic(definition.defobjType.type, DEF_DIV_CALL); break;
                    case REM:   execute.invokeStatic(definition.defobjType.type, DEF_REM_CALL); break;
                    case ADD:   execute.invokeStatic(definition.defobjType.type, DEF_ADD_CALL); break;
                    case SUB:   execute.invokeStatic(definition.defobjType.type, DEF_SUB_CALL); break;
                    case LSH:   execute.invokeStatic(definition.defobjType.type, DEF_LSH_CALL); break;
                    case USH:   execute.invokeStatic(definition.defobjType.type, DEF_RSH_CALL); break;
                    case RSH:   execute.invokeStatic(definition.defobjType.type, DEF_USH_CALL); break;
                    case BWAND: execute.invokeStatic(definition.defobjType.type, DEF_AND_CALL); break;
                    case BWXOR: execute.invokeStatic(definition.defobjType.type, DEF_XOR_CALL); break;
                    case BWOR:  execute.invokeStatic(definition.defobjType.type, DEF_OR_CALL);  break;
                    default:
                        throw new IllegalStateException(WriterUtility.error(source) + "Unexpected state.");
                }
            } else {
                switch (token) {
                    case MUL:   execute.math(GeneratorAdapter.MUL,  type.type); break;
                    case DIV:   execute.math(GeneratorAdapter.DIV,  type.type); break;
                    case REM:   execute.math(GeneratorAdapter.REM,  type.type); break;
                    case ADD:   execute.math(GeneratorAdapter.ADD,  type.type); break;
                    case SUB:   execute.math(GeneratorAdapter.SUB,  type.type); break;
                    case LSH:   execute.math(GeneratorAdapter.SHL,  type.type); break;
                    case USH:   execute.math(GeneratorAdapter.USHR, type.type); break;
                    case RSH:   execute.math(GeneratorAdapter.SHR,  type.type); break;
                    case BWAND: execute.math(GeneratorAdapter.AND,  type.type); break;
                    case BWXOR: execute.math(GeneratorAdapter.XOR,  type.type); break;
                    case BWOR:  execute.math(GeneratorAdapter.OR,   type.type); break;
                    default:
                        throw new IllegalStateException(WriterUtility.error(source) + "Unexpected state.");
                }
            }
        }
    }
}

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

package org.elasticsearch.painless.tree.walker.transformer;

import static org.elasticsearch.painless.Definition.Sort;

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.tree.node.Node;

import org.elasticsearch.painless.tree.node.Operation;
import org.objectweb.asm.Label;

import static org.elasticsearch.painless.tree.node.Operation.ADD;
import static org.elasticsearch.painless.tree.node.Operation.XOR;
import static org.elasticsearch.painless.tree.node.Type.BINARY;
import static org.elasticsearch.painless.tree.node.Type.TNEWSTRINGS;
import static org.elasticsearch.painless.tree.node.Type.TSERIES;
import static org.elasticsearch.painless.tree.node.Type.TWRITESTRINGS;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.ADDEXACT_INT;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.ADDEXACT_LONG;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.ADDWOOVERLOW_DOUBLE;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.ADDWOOVERLOW_FLOAT;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.DEF_ADD_CALL;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.DEF_AND_CALL;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.DEF_DIV_CALL;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.DEF_LSH_CALL;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.DEF_MUL_CALL;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.DEF_NEG_CALL;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.DEF_NOT_CALL;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.DEF_OR_CALL;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.DEF_REM_CALL;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.DEF_RSH_CALL;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.DEF_SUB_CALL;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.DEF_USH_CALL;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.DEF_XOR_CALL;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.DIVWOOVERLOW_INT;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.DIVWOOVERLOW_LONG;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.MULEXACT_INT;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.MULEXACT_LONG;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.MULWOOVERLOW_DOUBLE;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.MULWOOVERLOW_FLOAT;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.NEGATEEXACT_INT;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.NEGATEEXACT_LONG;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.SUBEXACT_INT;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.SUBEXACT_LONG;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.SUBWOOVERLOW_DOUBLE;
import static org.elasticsearch.painless.tree.walker.writer.WriterConstants.SUBWOOVERLOW_FLOAT;

class TransformerExpression {
    private final CompilerSettings settings;
    private final Definition definition;
    private final Transformer transformer;
    private final TransformerUtility utility;

    TransformerExpression(final CompilerSettings settings, final Definition definition,
                          final Transformer transformer, final TransformerUtility utility) {
        this.settings = settings;
        this.definition = definition;
        this.transformer = transformer;
        this.utility = utility;
    }

    Node visitConstant(final Node constant, final Label tru, final Label fals) {
        if (tru != null || fals != null) {
            final Object value = constant.data.get("constant");

            if (value instanceof Boolean) {
                if ((boolean)value && tru != null) {
                    return utility.jump(constant.location, tru);
                } else if (!(boolean)value && fals != null) {
                    return utility.jump(constant.location, fals);
                }
            } else {
                constant.children.add(utility.writebranch(constant.location, tru, fals));
            }
        }

        return constant;
    }

    Node visitNull(final Node nul, final Label tru, final Label fals) {
        if (tru != null || fals != null) {
            nul.children.add(utility.writebranch(nul.location, tru, fals));
        }

        return nul;
    }

    void visitExternal(final ExternalContext ctx) {
        final ExpressionMetadata expremd = metadata.getExpressionMetadata(ctx);
        writer.visit(ctx.extstart());
        caster.checkWriteCast(expremd);
        utility.checkWriteBranch(ctx);
    }

    void visitPostinc(final PostincContext ctx) {
        final ExpressionMetadata expremd = metadata.getExpressionMetadata(ctx);
        writer.visit(ctx.extstart());
        caster.checkWriteCast(expremd);
        utility.checkWriteBranch(ctx);
    }

    void visitPreinc(final PreincContext ctx) {
        final ExpressionMetadata expremd = metadata.getExpressionMetadata(ctx);
        writer.visit(ctx.extstart());
        caster.checkWriteCast(expremd);
        utility.checkWriteBranch(ctx);
    }

    Node visitCast(final Node cast, final Label tru, final Label fals) {
        cast.children.set(0, transformer.visit(cast.children.get(0)));

        if (tru != null || fals != null) {
            cast.children.add(utility.writebranch(cast.location, tru, fals));
        }

        return cast;
    }

    Node visitUnaryBoolnot(final Node unary, final Label tru, final Label fals) {
        if (tru == null && fals == null) {
            final Label localfals = new Label();
            final Label end = new Label();

            unary.children.set(0, transformer.visit(unary.children.get(0), null, localfals));
            unary.children.add(utility.constant(unary.location, false));
            unary.children.add(utility.jump(unary.location, end));
            unary.children.add(utility.mark(unary.location, localfals));
            unary.children.add(utility.constant(unary.location, true));
            unary.children.add(utility.mark(unary.location, end));
        } else {
            unary.children.set(0, transformer.visit(unary.children.get(0), fals, tru));
        }

        return unary;
    }

    Node visitUnaryBitwiseNot(final Node unary, final Label tru, final Label fals) {
        final Node rtn;

        final Type type = (Type)unary.data.get("type");
        final Sort sort = type.sort;

        if (sort == Sort.DEF) {
            rtn = new Node(unary.location, TSERIES);

            rtn.children.add(transformer.visit(unary.children.get(0), null, null));
            rtn.children.add(utility.statik(unary.location, definition.defobjType.type, DEF_NOT_CALL));
        } else {
            rtn = new Node(unary.location, BINARY);

            rtn.children.add(transformer.visit(unary.children.get(0), null, null));

            if (sort == Sort.INT) {
                rtn.children.add(utility.constant(unary.location, -1));
            } else if (sort == Sort.LONG) {
                rtn.children.add(utility.constant(unary.location, -1));
            } else {
                throw new IllegalStateException(unary.error("Illegal tree structure."));
            }

            rtn.data.put("operation", XOR);
        }

        if (tru != null || fals != null) {
            rtn.children.add(utility.writebranch(unary.location, tru, fals));
        }

        return rtn;
    }

    Node visitUnaryAdd(final Node unary, final Label tru, final Label fals) {
        return transformer.visit(unary.children.get(0), tru, fals);
    }

    Node visitUnarySub(final Node unary, final Label tru, final Label fals) {
        final Node rtn;

        final Type type = (Type)unary.data.get("type");
        final Sort sort = type.sort;

        if (sort == Sort.DEF) {
            rtn = new Node(unary.location, TSERIES);

            rtn.children.add(transformer.visit(unary.children.get(0)));
            rtn.children.add(utility.statik(unary.location, definition.defobjType.type, DEF_NEG_CALL));
        } else if (settings.getNumericOverflow()) {
            rtn = unary;

            unary.children.set(0, transformer.visit(unary.children.get(0)));
        } else {
            rtn = new Node(unary.location, TSERIES);

            rtn.children.add(transformer.visit(unary.children.get(0)));

            if (sort == Sort.INT) {
                rtn.children.add(utility.statik(unary.location, definition.mathType.type, NEGATEEXACT_INT));
            } else if (sort == Sort.LONG) {
                rtn.children.add(utility.statik(unary.location, definition.mathType.type, NEGATEEXACT_LONG));
            } else {
                throw new IllegalStateException(unary.error("Illegal tree structure."));
            }
        }

        if (tru != null || fals != null) {
            rtn.children.add(utility.writebranch(unary.location, tru, fals));
        }

        return rtn;
    }

    Node visitBinaryMul(final Node binary, final Label tru, final Label fals) {
        final Node rtn;

        final Type type = (Type)binary.data.get("type");
        final Sort sort = type.sort;

        if (sort == Sort.DEF) {
            rtn = new Node(binary.location, TSERIES);

            rtn.children.add(transformer.visit(binary.children.get(0)));
            rtn.children.add(transformer.visit(binary.children.get(1)));
            rtn.children.add(utility.statik(binary.location, definition.defobjType.type, DEF_MUL_CALL));
        } else if (settings.getNumericOverflow()) {
            rtn = binary;

            binary.children.set(0, transformer.visit(binary.children.get(0)));
            binary.children.set(1, transformer.visit(binary.children.get(1)));
        } else {
            rtn = new Node(binary.location, TSERIES);

            rtn.children.add(transformer.visit(binary.children.get(0)));
            rtn.children.add(transformer.visit(binary.children.get(1)));

            if (sort == Sort.INT) {
                rtn.children.add(utility.statik(binary.location, definition.mathType.type, MULEXACT_INT));
            } else if (sort == Sort.LONG) {
                rtn.children.add(utility.statik(binary.location, definition.mathType.type, MULEXACT_LONG));
            } else if (sort == Sort.FLOAT) {
                rtn.children.add(utility.statik(binary.location, definition.utilityType.type, MULWOOVERLOW_FLOAT));
            } else if (sort == Sort.DOUBLE) {
                rtn.children.add(utility.statik(binary.location, definition.utilityType.type, MULWOOVERLOW_DOUBLE));
            } else {
                throw new IllegalStateException(binary.error("Illegal tree structure."));
            }
        }

        if (tru != null || fals != null) {
            rtn.children.add(utility.writebranch(binary.location, tru, fals));
        }

        return rtn;
    }

    Node visitBinaryDiv(final Node binary, final Label tru, final Label fals) {
        final Node rtn;

        final Type type = (Type)binary.data.get("type");
        final Sort sort = type.sort;

        if (sort == Sort.DEF) {
            rtn = new Node(binary.location, TSERIES);

            rtn.children.add(transformer.visit(binary.children.get(0)));
            rtn.children.add(transformer.visit(binary.children.get(1)));
            rtn.children.add(utility.statik(binary.location, definition.defobjType.type, DEF_DIV_CALL));
        } else if (settings.getNumericOverflow()) {
            rtn = binary;

            binary.children.set(0, transformer.visit(binary.children.get(0)));
            binary.children.set(1, transformer.visit(binary.children.get(1)));
        } else {
            rtn = new Node(binary.location, TSERIES);

            rtn.children.add(transformer.visit(binary.children.get(0)));
            rtn.children.add(transformer.visit(binary.children.get(1)));

            if (sort == Sort.INT) {
                rtn.children.add(utility.statik(binary.location, definition.utilityType.type, DIVWOOVERLOW_INT));
            } else if (sort == Sort.LONG) {
                rtn.children.add(utility.statik(binary.location, definition.utilityType.type, DIVWOOVERLOW_LONG));
            } else if (sort == Sort.FLOAT) {
                rtn.children.add(utility.statik(binary.location, definition.utilityType.type, MULWOOVERLOW_FLOAT));
            } else if (sort == Sort.DOUBLE) {
                rtn.children.add(utility.statik(binary.location, definition.utilityType.type, MULWOOVERLOW_DOUBLE));
            } else {
                throw new IllegalStateException(binary.error("Illegal tree structure."));
            }
        }

        if (tru != null || fals != null) {
            rtn.children.add(utility.writebranch(binary.location, tru, fals));
        }

        return rtn;
    }

    Node visitBinaryRem(final Node binary, final Label tru, final Label fals) {
        final Node rtn;

        final Type type = (Type)binary.data.get("type");
        final Sort sort = type.sort;

        if (sort == Sort.DEF) {
            rtn = new Node(binary.location, TSERIES);

            rtn.children.add(transformer.visit(binary.children.get(0)));
            rtn.children.add(transformer.visit(binary.children.get(1)));
            rtn.children.add(utility.statik(binary.location, definition.defobjType.type, DEF_REM_CALL));
        } else if (settings.getNumericOverflow() || sort == Sort.INT || sort == Sort.LONG) {
            rtn = binary;

            binary.children.set(0, transformer.visit(binary.children.get(0)));
            binary.children.set(1, transformer.visit(binary.children.get(1)));
        } else {
            rtn = new Node(binary.location, TSERIES);

            rtn.children.add(transformer.visit(binary.children.get(0)));
            rtn.children.add(transformer.visit(binary.children.get(1)));

            if (sort == Sort.FLOAT) {
                rtn.children.add(utility.statik(binary.location, definition.utilityType.type, MULWOOVERLOW_FLOAT));
            } else if (sort == Sort.DOUBLE) {
                rtn.children.add(utility.statik(binary.location, definition.utilityType.type, MULWOOVERLOW_DOUBLE));
            } else {
                throw new IllegalStateException(binary.error("Illegal tree structure."));
            }
        }

        if (tru != null || fals != null) {
            rtn.children.add(utility.writebranch(binary.location, tru, fals));
        }

        return rtn;
    }

    Node visitBinaryAdd(final Node binary, final Label tru, final Label fals, final boolean strings) {
        final Node rtn;

        final Type type = (Type)binary.data.get("type");
        final Sort sort = type.sort;

        if (sort == Sort.STRING) {
            rtn = new Node(binary.location, TSERIES);

            if (!strings) {
                rtn.children.add(new Node(binary.location, TNEWSTRINGS));
            }

            final Node left = binary.children.get(0);
            final Node right = binary.children.get(1);

            final Operation loperation = (Operation)left.data.get("operation");
            final Operation roperation = (Operation)right.data.get("operation");

            final Type ltype = ((Type)left.data.get("type"));
            final Type rtype = ((Type)right.data.get("type"));

            rtn.children.add(transformer.visit(left, true));

            if (left.type != BINARY || ltype.sort != Sort.STRING || loperation != ADD) {
                rtn.children.add(utility.append(left.location, ltype));
            }

            rtn.children.add(transformer.visit(right, true));

            if (right.type != BINARY || rtype.sort != Sort.STRING || roperation != ADD) {
                rtn.children.add(utility.append(right.location, rtype));
            }

            if (!strings) {
                rtn.children.add(new Node(binary.location, TWRITESTRINGS));
            }
        } else if (sort == Sort.DEF) {
            rtn = new Node(binary.location, TSERIES);

            rtn.children.add(transformer.visit(binary.children.get(0)));
            rtn.children.add(transformer.visit(binary.children.get(1)));
            rtn.children.add(utility.statik(binary.location, definition.defobjType.type, DEF_ADD_CALL));
        } else if (settings.getNumericOverflow()) {
            rtn = binary;

            binary.children.set(0, transformer.visit(binary.children.get(0)));
            binary.children.set(1, transformer.visit(binary.children.get(1)));
        } else {
            rtn = new Node(binary.location, TSERIES);

            rtn.children.add(transformer.visit(binary.children.get(0)));
            rtn.children.add(transformer.visit(binary.children.get(1)));

            if (sort == Sort.INT) {
                rtn.children.add(utility.statik(binary.location, definition.mathType.type, ADDEXACT_INT));
            } else if (sort == Sort.LONG) {
                rtn.children.add(utility.statik(binary.location, definition.mathType.type, ADDEXACT_LONG));
            } else if (sort == Sort.FLOAT) {
                rtn.children.add(utility.statik(binary.location, definition.utilityType.type, ADDWOOVERLOW_FLOAT));
            } else if (sort == Sort.DOUBLE) {
                rtn.children.add(utility.statik(binary.location, definition.utilityType.type, ADDWOOVERLOW_DOUBLE));
            } else {
                throw new IllegalStateException(binary.error("Illegal tree structure."));
            }
        }

        if (tru != null || fals != null) {
            rtn.children.add(utility.writebranch(binary.location, tru, fals));
        }

        return rtn;
    }

    Node visitBinarySub(final Node binary, final Label tru, final Label fals) {
        final Node rtn;

        final Type type = (Type)binary.data.get("type");
        final Sort sort = type.sort;

        if (sort == Sort.DEF) {
            rtn = new Node(binary.location, TSERIES);

            rtn.children.add(transformer.visit(binary.children.get(0)));
            rtn.children.add(transformer.visit(binary.children.get(1)));
            rtn.children.add(utility.statik(binary.location, definition.defobjType.type, DEF_SUB_CALL));
        } else if (settings.getNumericOverflow()) {
            rtn = binary;

            binary.children.set(0, transformer.visit(binary.children.get(0)));
            binary.children.set(1, transformer.visit(binary.children.get(1)));
        } else {
            rtn = new Node(binary.location, TSERIES);

            rtn.children.add(transformer.visit(binary.children.get(0)));
            rtn.children.add(transformer.visit(binary.children.get(1)));

            if (sort == Sort.INT) {
                rtn.children.add(utility.statik(binary.location, definition.mathType.type, SUBEXACT_INT));
            } else if (sort == Sort.LONG) {
                rtn.children.add(utility.statik(binary.location, definition.mathType.type, SUBEXACT_LONG));
            } else if (sort == Sort.FLOAT) {
                rtn.children.add(utility.statik(binary.location, definition.utilityType.type, SUBWOOVERLOW_FLOAT));
            } else if (sort == Sort.DOUBLE) {
                rtn.children.add(utility.statik(binary.location, definition.utilityType.type, SUBWOOVERLOW_DOUBLE));
            } else {
                throw new IllegalStateException(binary.error("Illegal tree structure."));
            }
        }

        if (tru != null || fals != null) {
            rtn.children.add(utility.writebranch(binary.location, tru, fals));
        }

        return rtn;
    }

    Node visitBinaryLeftShift(final Node binary, final Label tru, final Label fals) {
        final Node rtn;

        final Type type = (Type)binary.data.get("type");
        final Sort sort = type.sort;

        if (sort == Sort.DEF) {
            rtn = new Node(binary.location, TSERIES);

            rtn.children.add(transformer.visit(binary.children.get(0)));
            rtn.children.add(transformer.visit(binary.children.get(1)));
            rtn.children.add(utility.statik(binary.location, definition.defobjType.type, DEF_LSH_CALL));
        } else {
            rtn = binary;

            binary.children.set(0, transformer.visit(binary.children.get(0)));
            binary.children.set(1, transformer.visit(binary.children.get(1)));
        }

        if (tru != null || fals != null) {
            rtn.children.add(utility.writebranch(binary.location, tru, fals));
        }

        return rtn;
    }

    Node visitBinaryRightShift(final Node binary, final Label tru, final Label fals) {
        final Node rtn;

        final Type type = (Type)binary.data.get("type");
        final Sort sort = type.sort;

        if (sort == Sort.DEF) {
            rtn = new Node(binary.location, TSERIES);

            rtn.children.add(transformer.visit(binary.children.get(0)));
            rtn.children.add(transformer.visit(binary.children.get(1)));
            rtn.children.add(utility.statik(binary.location, definition.defobjType.type, DEF_RSH_CALL));
        } else {
            rtn = binary;

            binary.children.set(0, transformer.visit(binary.children.get(0)));
            binary.children.set(1, transformer.visit(binary.children.get(1)));
        }

        if (tru != null || fals != null) {
            rtn.children.add(utility.writebranch(binary.location, tru, fals));
        }

        return rtn;
    }

    Node visitBinaryUnsignedShift(final Node binary, final Label tru, final Label fals) {
        final Node rtn;

        final Type type = (Type)binary.data.get("type");
        final Sort sort = type.sort;

        if (sort == Sort.DEF) {
            rtn = new Node(binary.location, TSERIES);

            rtn.children.add(transformer.visit(binary.children.get(0)));
            rtn.children.add(transformer.visit(binary.children.get(1)));
            rtn.children.add(utility.statik(binary.location, definition.defobjType.type, DEF_USH_CALL));
        } else {
            rtn = binary;

            binary.children.set(0, transformer.visit(binary.children.get(0)));
            binary.children.set(1, transformer.visit(binary.children.get(1)));
        }

        if (tru != null || fals != null) {
            rtn.children.add(utility.writebranch(binary.location, tru, fals));
        }

        return rtn;
    }

    Node visitBinaryBitwiseAnd(final Node binary, final Label tru, final Label fals) {
        final Node rtn;

        final Type type = (Type)binary.data.get("type");
        final Sort sort = type.sort;

        if (sort == Sort.DEF) {
            rtn = new Node(binary.location, TSERIES);

            rtn.children.add(transformer.visit(binary.children.get(0)));
            rtn.children.add(transformer.visit(binary.children.get(1)));
            rtn.children.add(utility.statik(binary.location, definition.defobjType.type, DEF_AND_CALL));
        } else {
            rtn = binary;

            binary.children.set(0, transformer.visit(binary.children.get(0)));
            binary.children.set(1, transformer.visit(binary.children.get(1)));
        }

        if (tru != null || fals != null) {
            rtn.children.add(utility.writebranch(binary.location, tru, fals));
        }

        return rtn;
    }

    Node visitBinaryBitwiseXor(final Node binary, final Label tru, final Label fals) {
        final Node rtn;

        final Type type = (Type)binary.data.get("type");
        final Sort sort = type.sort;

        if (sort == Sort.DEF) {
            rtn = new Node(binary.location, TSERIES);

            rtn.children.add(transformer.visit(binary.children.get(0)));
            rtn.children.add(transformer.visit(binary.children.get(1)));
            rtn.children.add(utility.statik(binary.location, definition.defobjType.type, DEF_XOR_CALL));
        } else {
            rtn = binary;

            binary.children.set(0, transformer.visit(binary.children.get(0)));
            binary.children.set(1, transformer.visit(binary.children.get(1)));
        }

        if (tru != null || fals != null) {
            rtn.children.add(utility.writebranch(binary.location, tru, fals));
        }

        return rtn;
    }

    Node visitBinaryBitwiseOr(final Node binary, final Label tru, final Label fals) {
        final Node rtn;

        final Type type = (Type)binary.data.get("type");
        final Sort sort = type.sort;

        if (sort == Sort.DEF) {
            rtn = new Node(binary.location, TSERIES);

            rtn.children.add(transformer.visit(binary.children.get(0)));
            rtn.children.add(transformer.visit(binary.children.get(1)));
            rtn.children.add(utility.statik(binary.location, definition.defobjType.type, DEF_OR_CALL));
        } else {
            rtn = binary;

            binary.children.set(0, transformer.visit(binary.children.get(0)));
            binary.children.set(1, transformer.visit(binary.children.get(1)));
        }

        if (tru != null || fals != null) {
            rtn.children.add(utility.writebranch(binary.location, tru, fals));
        }

        return rtn;
    }

    Node visitEquals(final Node binary, final Label tru, final Label fals) {
        final Node rtn;

        final Label jump = tru != null ? tru : fals != null ? fals : new Label();
        final Label end = new Label();

        if (tru != null || fals != null) {
            rtn = new Node(binary.location, TSERIES);

            rtn.children.add(transformer.visit(binary.children.get(0)));
            rtn.children.add(transformer.visit(binary.children.get(1)));

        } else {
            rtn = binary;

            binary.children.set(0, transformer.visit(binary.children.get(0)));
            binary.children.set(1, transformer.visit(binary.children.get(1)));
        }

        return rtn;
    }

    void visitComp(final CompContext ctx) {
        final ExpressionMetadata compemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = compemd.postConst;
        final Object preConst = compemd.preConst;
        final Branch branch = utility.getBranch(ctx);

        if (postConst != null) {
            if (branch == null) {
                utility.writeConstant(ctx, postConst);
            } else {
                if ((boolean)postConst && branch.tru != null) {
                    execute.mark(branch.tru);
                } else if (!(boolean)postConst && branch.fals != null) {
                    execute.mark(branch.fals);
                }
            }
        } else if (preConst != null) {
            if (branch == null) {
                utility.writeConstant(ctx, preConst);
                caster.checkWriteCast(compemd);
            } else {
                throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
            }
        } else {
            final ExpressionContext exprctx0 = ctx.expression(0);
            final ExpressionMetadata expremd0 = metadata.getExpressionMetadata(exprctx0);

            final ExpressionContext exprctx1 = ctx.expression(1);
            final ExpressionMetadata expremd1 = metadata.getExpressionMetadata(exprctx1);
            final org.objectweb.asm.Type type = expremd1.to.type;
            final Sort sort1 = expremd1.to.sort;

            writer.visit(exprctx0);

            if (!expremd1.isNull) {
                writer.visit(exprctx1);
            }

            final boolean tru = branch != null && branch.tru != null;
            final boolean fals = branch != null && branch.fals != null;
            final Label jump = tru ? branch.tru : fals ? branch.fals : new Label();
            final Label end = new Label();

            final boolean eq = (ctx.EQ() != null || ctx.EQR() != null) && (tru || !fals) ||
                (ctx.NE() != null || ctx.NER() != null) && fals;
            final boolean ne = (ctx.NE() != null || ctx.NER() != null) && (tru || !fals) ||
                (ctx.EQ() != null || ctx.EQR() != null) && fals;
            final boolean lt  = ctx.LT()  != null && (tru || !fals) || ctx.GTE() != null && fals;
            final boolean lte = ctx.LTE() != null && (tru || !fals) || ctx.GT()  != null && fals;
            final boolean gt  = ctx.GT()  != null && (tru || !fals) || ctx.LTE() != null && fals;
            final boolean gte = ctx.GTE() != null && (tru || !fals) || ctx.LT()  != null && fals;

            boolean writejump = true;

            switch (sort1) {
                case VOID:
                case BYTE:
                case SHORT:
                case CHAR:
                    throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
                case BOOL:
                    if      (eq) execute.ifZCmp(GeneratorAdapter.EQ, jump);
                    else if (ne) execute.ifZCmp(GeneratorAdapter.NE, jump);
                    else {
                        throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
                    }

                    break;
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                    if      (eq)  execute.ifCmp(type, GeneratorAdapter.EQ, jump);
                    else if (ne)  execute.ifCmp(type, GeneratorAdapter.NE, jump);
                    else if (lt)  execute.ifCmp(type, GeneratorAdapter.LT, jump);
                    else if (lte) execute.ifCmp(type, GeneratorAdapter.LE, jump);
                    else if (gt)  execute.ifCmp(type, GeneratorAdapter.GT, jump);
                    else if (gte) execute.ifCmp(type, GeneratorAdapter.GE, jump);
                    else {
                        throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
                    }

                    break;
                case DEF:
                    if (eq) {
                        if (expremd1.isNull) {
                            execute.ifNull(jump);
                        } else if (!expremd0.isNull && ctx.EQ() != null) {
                            execute.invokeStatic(definition.defobjType.type, DEF_EQ_CALL);
                        } else {
                            execute.ifCmp(type, GeneratorAdapter.EQ, jump);
                        }
                    } else if (ne) {
                        if (expremd1.isNull) {
                            execute.ifNonNull(jump);
                        } else if (!expremd0.isNull && ctx.NE() != null) {
                            execute.invokeStatic(definition.defobjType.type, DEF_EQ_CALL);
                            execute.ifZCmp(GeneratorAdapter.EQ, jump);
                        } else {
                            execute.ifCmp(type, GeneratorAdapter.NE, jump);
                        }
                    } else if (lt) {
                        execute.invokeStatic(definition.defobjType.type, DEF_LT_CALL);
                    } else if (lte) {
                        execute.invokeStatic(definition.defobjType.type, DEF_LTE_CALL);
                    } else if (gt) {
                        execute.invokeStatic(definition.defobjType.type, DEF_GT_CALL);
                    } else if (gte) {
                        execute.invokeStatic(definition.defobjType.type, DEF_GTE_CALL);
                    } else {
                        throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
                    }

                    writejump = expremd1.isNull || ne || ctx.EQR() != null;

                    if (branch != null && !writejump) {
                        execute.ifZCmp(GeneratorAdapter.NE, jump);
                    }

                    break;
                default:
                    if (eq) {
                        if (expremd1.isNull) {
                            execute.ifNull(jump);
                        } else if (ctx.EQ() != null) {
                            execute.invokeStatic(definition.utilityType.type, CHECKEQUALS);

                            if (branch != null) {
                                execute.ifZCmp(GeneratorAdapter.NE, jump);
                            }

                            writejump = false;
                        } else {
                            execute.ifCmp(type, GeneratorAdapter.EQ, jump);
                        }
                    } else if (ne) {
                        if (expremd1.isNull) {
                            execute.ifNonNull(jump);
                        } else if (ctx.NE() != null) {
                            execute.invokeStatic(definition.utilityType.type, CHECKEQUALS);
                            execute.ifZCmp(GeneratorAdapter.EQ, jump);
                        } else {
                            execute.ifCmp(type, GeneratorAdapter.NE, jump);
                        }
                    } else {
                        throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
                    }
            }

            if (branch == null) {
                if (writejump) {
                    execute.push(false);
                    execute.goTo(end);
                    execute.mark(jump);
                    execute.push(true);
                    execute.mark(end);
                }

                caster.checkWriteCast(compemd);
            }
        }
    }

    void visitBool(final BoolContext ctx) {
        final ExpressionMetadata boolemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = boolemd.postConst;
        final Object preConst = boolemd.preConst;
        final Branch branch = utility.getBranch(ctx);

        if (postConst != null) {
            if (branch == null) {
                utility.writeConstant(ctx, postConst);
            } else {
                if ((boolean)postConst && branch.tru != null) {
                    execute.mark(branch.tru);
                } else if (!(boolean)postConst && branch.fals != null) {
                    execute.mark(branch.fals);
                }
            }
        } else if (preConst != null) {
            if (branch == null) {
                utility.writeConstant(ctx, preConst);
                caster.checkWriteCast(boolemd);
            } else {
                throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
            }
        } else {
            final ExpressionContext exprctx0 = ctx.expression(0);
            final ExpressionContext exprctx1 = ctx.expression(1);

            if (branch == null) {
                if (ctx.BOOLAND() != null) {
                    final Branch local = utility.markBranch(ctx, exprctx0, exprctx1);
                    local.fals = new Label();
                    final Label end = new Label();

                    writer.visit(exprctx0);
                    writer.visit(exprctx1);

                    execute.push(true);
                    execute.goTo(end);
                    execute.mark(local.fals);
                    execute.push(false);
                    execute.mark(end);
                } else if (ctx.BOOLOR() != null) {
                    final Branch branch0 = utility.markBranch(ctx, exprctx0);
                    branch0.tru = new Label();
                    final Branch branch1 = utility.markBranch(ctx, exprctx1);
                    branch1.fals = new Label();
                    final Label aend = new Label();

                    writer.visit(exprctx0);
                    writer.visit(exprctx1);

                    execute.mark(branch0.tru);
                    execute.push(true);
                    execute.goTo(aend);
                    execute.mark(branch1.fals);
                    execute.push(false);
                    execute.mark(aend);
                } else {
                    throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
                }

                caster.checkWriteCast(boolemd);
            } else {
                if (ctx.BOOLAND() != null) {
                    final Branch branch0 = utility.markBranch(ctx, exprctx0);
                    branch0.fals = branch.fals == null ? new Label() : branch.fals;
                    final Branch branch1 = utility.markBranch(ctx, exprctx1);
                    branch1.tru = branch.tru;
                    branch1.fals = branch.fals;

                    writer.visit(exprctx0);
                    writer.visit(exprctx1);

                    if (branch.fals == null) {
                        execute.mark(branch0.fals);
                    }
                } else if (ctx.BOOLOR() != null) {
                    final Branch branch0 = utility.markBranch(ctx, exprctx0);
                    branch0.tru = branch.tru == null ? new Label() : branch.tru;
                    final Branch branch1 = utility.markBranch(ctx, exprctx1);
                    branch1.tru = branch.tru;
                    branch1.fals = branch.fals;

                    writer.visit(exprctx0);
                    writer.visit(exprctx1);

                    if (branch.tru == null) {
                        execute.mark(branch0.tru);
                    }
                } else {
                    throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
                }
            }
        }
    }

    void visitConditional(final ConditionalContext ctx) {
        final ExpressionMetadata condemd = metadata.getExpressionMetadata(ctx);
        final Branch branch = utility.getBranch(ctx);

        final ExpressionContext expr0 = ctx.expression(0);
        final ExpressionContext expr1 = ctx.expression(1);
        final ExpressionContext expr2 = ctx.expression(2);

        final Branch local = utility.markBranch(ctx, expr0);
        local.fals = new Label();
        local.end = new Label();

        if (branch != null) {
            utility.copyBranch(branch, expr1, expr2);
        }

        writer.visit(expr0);
        writer.visit(expr1);
        execute.goTo(local.end);
        execute.mark(local.fals);
        writer.visit(expr2);
        execute.mark(local.end);

        if (branch == null) {
            caster.checkWriteCast(condemd);
        }
    }

    void visitAssignment(final AssignmentContext ctx) {
        final ExpressionMetadata expremd = metadata.getExpressionMetadata(ctx);
        writer.visit(ctx.extstart());
        caster.checkWriteCast(expremd);
        utility.checkWriteBranch(ctx);
    }
}

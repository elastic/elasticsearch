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

import org.elasticsearch.painless.tree.node.Node;
import org.elasticsearch.painless.tree.utility.Variables.Variable;
import org.objectweb.asm.Label;

import static org.elasticsearch.painless.tree.node.Type.NULL;
import static org.elasticsearch.painless.tree.node.Type.RETURN;
import static org.elasticsearch.painless.tree.node.Type.TBRANCH;

class TransformerStatement {
    private final Transformer transformer;
    private final TransformerUtility utility;

    TransformerStatement(final Transformer transformer, final TransformerUtility utility) {
        this.transformer = transformer;
        this.utility = utility;
    }

    Node visitIf(final Node ifelse, final Label continu, final Label brake) {
        final Node branch = new Node(ifelse.location, TBRANCH);

        final Node condition = ifelse.children.get(1);
        final Node ifblock = ifelse.children.get(0);
        final Node elseblock = ifelse.children.get(2);

        final Label end = new Label();
        final Label fals = elseblock != null ? new Label() : end;

        branch.children.add(transformer.visit(condition, end, fals));
        branch.children.add(transformer.visit(ifblock, continu, brake));

        if (elseblock != null) {
            final boolean escape = (boolean)ifelse.data.get("escape");

            if (!escape) {
                branch.children.add(utility.jump(ifelse.location, end));
            }

            branch.children.add(utility.mark(ifelse.location, fals));
            branch.children.add(transformer.visit(elseblock, continu, brake));
        }

        branch.children.add(utility.mark(ifelse.location, end));

        return branch;
    }

    Node visitWhile(final Node whil) {
        final Node branch = new Node(whil.location, TBRANCH);

        final Node condition = whil.children.get(1);
        final Node block = whil.children.get(0);

        final Label begin = new Label();
        final Label end = new Label();

        branch.children.add(utility.mark(whil.location, begin));
        branch.children.add(transformer.visit(condition, null, end));

        final Variable counter = (Variable)whil.data.get("counter");

        if (counter != null) {
            final int count = (int)whil.data.get("count");
            branch.children.add(utility.loopcount(whil.location, counter.slot, count));
        }

        if (block != null) {
            branch.children.add(transformer.visit(block, begin, end));
        }

        final boolean escape = (boolean)whil.data.get("escape");

        if (!escape) {
            branch.children.add(utility.jump(whil.location, begin));
        }

        branch.children.add(utility.mark(whil.location, end));

        return branch;
    }

    Node visitDo(final Node dowhile) {
        final Node branch = new Node(dowhile.location, TBRANCH);

        final Node condition = dowhile.children.get(1);
        final Node block = dowhile.children.get(0);

        final Label start = new Label();
        final Label begin = new Label();
        final Label end = new Label();

        branch.children.add(utility.mark(dowhile.location, start));
        branch.children.add(transformer.visit(block, begin, end));
        branch.children.add(utility.mark(dowhile.location, begin));
        branch.children.add(transformer.visit(condition, null, end));

        final Variable counter = (Variable)dowhile.data.get("counter");

        if (counter != null) {
            final int count = (int)dowhile.data.get("count");
            branch.children.add(utility.loopcount(dowhile.location, counter.slot, count));
        }

        branch.children.add(utility.jump(dowhile.location, start));
        branch.children.add(utility.mark(dowhile.location, end));

        return branch;
    }

    Node visitFor(final Node fr) {
        final Node branch = new Node(fr.location, TBRANCH);

        final Node initializer = fr.children.get(0);
        final Node condition = fr.children.get(1);
        final Node afterthought = fr.children.get(2);
        final Node block = fr.children.get(3);

        final Label start = new Label();
        final Label begin = afterthought == null ? start : new Label();
        final Label end = new Label();

        if (initializer != null) {
            branch.children.add(transformer.visit(initializer));

            final int size = (int)initializer.data.get("size");

            if (size > 0) {
                branch.children.add(utility.pop(initializer.location, size));
            }
        }

        branch.children.add(utility.mark(fr.location, start));

        if (condition != null) {
            branch.children.add(transformer.visit(condition, null, end));
        }

        final Variable counter = (Variable)fr.data.get("counter");

        if (counter != null) {
            final int count = (int)fr.data.get("count");
            branch.children.add(utility.loopcount(fr.location, counter.slot, count));
        }

        if (block != null) {
            branch.children.add(transformer.visit(block));
        }

        if (afterthought != null) {
            branch.children.add(utility.mark(fr.location, begin));
            branch.children.add(transformer.visit(afterthought));

            final int size = (int)afterthought.data.get("size");

            if (size > 0) {
                branch.children.add(utility.pop(afterthought.location, size));
            }
        }

        final boolean escape = afterthought == null && (boolean)fr.data.get("escape");

        if (!escape) {
            branch.children.add(utility.jump(fr.location, start));
        }

        branch.children.add(utility.mark(fr.location, end));

        return branch;
    }

    Node visitContinue(final Node node, final Label continu) {
        return utility.jump(node.location, continu);
    }

    Node visitBreak(final Node node, final Label brake) {
        return utility.jump(node.location, brake);
    }

    Node visitReturn(final Node rtn) {
        rtn.children.set(0, transformer.visit(rtn.children.get(0), null, null));

        return rtn;
    }

    Node visitTry(final Node ty, final Label continu, final Label brake) {
        final Node branch = new Node(ty.location, TBRANCH);

        final Node block = ty.children.get(1);

        final Label begin = new Label();
        final Label end = new Label();
        final Label finish = new Label();

        branch.children.add(utility.mark(ty.location, begin));
        branch.children.add(transformer.visit(block, continu, brake));

        boolean escape = (boolean)ty.data.get("escape");

        if (!escape) {
            branch.children.add(utility.jump(ty.location, finish));
        }

        branch.children.add(utility.mark(ty.location, end));

        for (int index = 1; index < ty.children.size(); ++index) {
            final Node trap = ty.children.get(index);
            final Node tblock = trap.children.get(0);

            final Label jump = new Label();

            branch.children.add(utility.mark(trap.location, jump));

            final Variable variable = (Variable)trap.data.get("variable");

            branch.children.add(utility.varstore(trap.location, variable));

            if (tblock != null) {
                branch.children.add(transformer.visit(tblock, continu, brake));
            }

            branch.children.add(utility.trap(trap.location, variable, begin, end, jump));

            final boolean tescape = (boolean)trap.data.get("escape");

            if (!tescape && index < ty.children.size() - 1) {
                branch.children.add(utility.jump(trap.location, finish));
                escape = false;
            }
        }

        if (!escape) {
            branch.children.add(utility.mark(ty.location, finish));
        }

        return branch;
    }

    Node visitThrow(final Node thro) {
        thro.children.set(0, transformer.visit(thro.children.get(0), null, null));

        return thro;
    }

    Node visitExpr(final Node expr) {
        final Node expression = transformer.visit(expr.children.get(0), null, null);

        final boolean escape = (boolean)expr.data.get("escape");

        if (escape) {
            final Node rtn = new Node(expr.location, RETURN);
            rtn.children.add(expression);
            expr.children.set(0, rtn);
        } else {
            final int size = (int)expr.data.get("size");

            if (size > 0) {
                expr.children.add(utility.pop(expr.location, size));
            }

            expr.children.set(0, expression);
        }

        return expr;
    }

    Node visitBlock(final Node block, final Label continu, final Label brake) {
        int index = 0;

        for (final Node child : block.children) {
            final Node transform = transformer.visit(child, continu, brake);

            if (transform != child) {
                block.children.set(index, transform);
            }

            ++index;
        }

        return block;
    }

    Node visitDeclaration(final Node declaration) {
        for (final Node child : declaration.children) {
            final Variable variable = (Variable)child.data.get("variable");

            final Node expression = child.children.get(0);

            if (expression != null) {
                child.children.set(0, transformer.visit(expression));

            } else {
                switch (variable.type.sort) {
                    case VOID:   throw new IllegalStateException(child.error("Illegal tree structure."));
                    case BOOL:
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:    child.children.add(utility.constant(child.location, 0 )); break;
                    case LONG:   child.children.add(utility.constant(child.location, 0L)); break;
                    case FLOAT:  child.children.add(utility.constant(child.location, 0F)); break;
                    case DOUBLE: child.children.add(utility.constant(child.location, 0D)); break;
                    default:     child.children.add(new Node(child.location, NULL));
                }
            }

            child.children.add(utility.varstore(child.location, variable));
        }

        return declaration;
    }
}

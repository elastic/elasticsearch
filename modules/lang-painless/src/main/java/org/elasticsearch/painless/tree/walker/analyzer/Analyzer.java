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

package org.elasticsearch.painless.tree.walker.analyzer;

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.tree.node.Node;
import org.elasticsearch.painless.tree.node.Operation;
import org.elasticsearch.painless.tree.node.Type;
import org.elasticsearch.painless.tree.utility.Variables;

import static org.elasticsearch.painless.tree.node.Operation.ADD;
import static org.elasticsearch.painless.tree.node.Operation.AND;
import static org.elasticsearch.painless.tree.node.Operation.BWAND;
import static org.elasticsearch.painless.tree.node.Operation.BWNOT;
import static org.elasticsearch.painless.tree.node.Operation.BWOR;
import static org.elasticsearch.painless.tree.node.Operation.DIV;
import static org.elasticsearch.painless.tree.node.Operation.EQ;
import static org.elasticsearch.painless.tree.node.Operation.EQR;
import static org.elasticsearch.painless.tree.node.Operation.GT;
import static org.elasticsearch.painless.tree.node.Operation.GTE;
import static org.elasticsearch.painless.tree.node.Operation.LSH;
import static org.elasticsearch.painless.tree.node.Operation.LT;
import static org.elasticsearch.painless.tree.node.Operation.LTE;
import static org.elasticsearch.painless.tree.node.Operation.MUL;
import static org.elasticsearch.painless.tree.node.Operation.NE;
import static org.elasticsearch.painless.tree.node.Operation.NER;
import static org.elasticsearch.painless.tree.node.Operation.NOT;
import static org.elasticsearch.painless.tree.node.Operation.OR;
import static org.elasticsearch.painless.tree.node.Operation.REM;
import static org.elasticsearch.painless.tree.node.Operation.RSH;
import static org.elasticsearch.painless.tree.node.Operation.SUB;
import static org.elasticsearch.painless.tree.node.Operation.USH;
import static org.elasticsearch.painless.tree.node.Operation.XOR;
import static org.elasticsearch.painless.tree.node.Type.BINARY;
import static org.elasticsearch.painless.tree.node.Type.BLOCK;
import static org.elasticsearch.painless.tree.node.Type.BREAK;
import static org.elasticsearch.painless.tree.node.Type.CAST;
import static org.elasticsearch.painless.tree.node.Type.CHAR;
import static org.elasticsearch.painless.tree.node.Type.CONDITIONAL;
import static org.elasticsearch.painless.tree.node.Type.CONTINUE;
import static org.elasticsearch.painless.tree.node.Type.DECLARATION;
import static org.elasticsearch.painless.tree.node.Type.DO;
import static org.elasticsearch.painless.tree.node.Type.EXPRESSION;
import static org.elasticsearch.painless.tree.node.Type.FALSE;
import static org.elasticsearch.painless.tree.node.Type.FOR;
import static org.elasticsearch.painless.tree.node.Type.IF;
import static org.elasticsearch.painless.tree.node.Type.NULL;
import static org.elasticsearch.painless.tree.node.Type.NUMERIC;
import static org.elasticsearch.painless.tree.node.Type.RETURN;
import static org.elasticsearch.painless.tree.node.Type.SOURCE;
import static org.elasticsearch.painless.tree.node.Type.THROW;
import static org.elasticsearch.painless.tree.node.Type.TRAP;
import static org.elasticsearch.painless.tree.node.Type.TRUE;
import static org.elasticsearch.painless.tree.node.Type.TRY;
import static org.elasticsearch.painless.tree.node.Type.UNARY;
import static org.elasticsearch.painless.tree.node.Type.WHILE;

public final class Analyzer {
    public static void analyze(final CompilerSettings settings, final Definition definition, final Variables variables, final Node source) {
        new Analyzer(settings, definition, variables, source);
    }

    private final AnalyzerStatement statement;
    private final AnalyzerExpression expression;

    private Analyzer(final CompilerSettings settings, final Definition definition, final Variables variables, final Node source) {
        final AnalyzerCaster caster = new AnalyzerCaster(definition);
        final AnalyzerPromoter promoter = new AnalyzerPromoter(definition);

        statement = new AnalyzerStatement(definition, variables, this, caster);
        expression = new AnalyzerExpression(definition, settings, this, caster, promoter);

        if (source.type != SOURCE) {
            throw new IllegalStateException(source.error("Illegal tree structure."));
        }

        visitSource(source, variables);
    }

    private void visitSource(final Node source, final Variables variables) {
        final Node last = source.children.get(source.children.size() - 1);

        boolean methodEscape = false;
        boolean allEscape = false;

        variables.incrementScope();

        for (final Node child : source.children) {
            if (allEscape) {
                throw new IllegalArgumentException(child.error("Unreachable statement."));
            }

            final MetadataStatement childms = new MetadataStatement();
            childms.lastSource = child == last;
            visit(child, childms);

            methodEscape = childms.methodEscape;
            allEscape = childms.allEscape;
        }

        variables.decrementScope();

        source.data.put("escape", methodEscape);
    }

    void visit(final Node node, final MetadataStatement ms) {
        final Type type = node.type;

        if (type == IF) {
            statement.visitIf(node, ms);
        } else if (type == WHILE) {
            statement.visitWhile(node, ms);
        } else if (type == DO) {
            statement.visitDo(node, ms);
        } else if (type == FOR) {
            statement.visitFor(node, ms);
        } else if (type == CONTINUE) {
            statement.visitContinue(node, ms);
        } else if (type == BREAK) {
            statement.visitBreak(node, ms);
        } else if (type == RETURN) {
            statement.visitReturn(node, ms);
        } else if (type == TRY) {
            statement.visitTry(node, ms);
        } else if (type == TRAP) {
            statement.visitTrap(node, ms);
        } else if (type == THROW) {
            statement.visitThrow(node, ms);
        } else if (type == EXPRESSION) {
            statement.visitExpr(node, ms);
        } else if (type == BLOCK) {
            statement.visitBlock(node, ms);
        } else if (type == DECLARATION) {
            statement.visitDeclaration(node, ms);
        } else {
            throw new IllegalStateException(node.error("Illegal tree structure."));
        }
    }

    void visit(final Node node, final MetadataExpression me) {
        final Type type = node.type;

        if (type == NUMERIC) {
            expression.visitNumeric(node, me);
        } else if (type == CHAR) {
            expression.visitChar(node, me);
        } else if (type == TRUE) {
            expression.visitTrue(node, me);
        } else if (type == FALSE) {
            expression.visitFalse(node, me);
        } else if (type == NULL) {
            expression.visitNull(node, me);
        } else if (type == CAST) {
            expression.visitCast(node, me);
        } else if (type == UNARY) {
            final Operation operation = (Operation)node.data.get("operation");

            if (operation == NOT) {
                expression.visitUnaryBoolNot(node, me);
            } else if (operation == BWNOT) {
                expression.visitUnaryBwNot(node, me);
            } else if (operation == ADD) {
                expression.visitUnaryAdd(node, me);
            } else if (operation == SUB) {
                expression.visitUnarySub(node, me);
            } else {
                throw new IllegalStateException(node.error("Illegal tree structure."));
            }
        } else if (type == BINARY) {
            final Operation operation = (Operation)node.data.get("operation");

            if (operation == MUL) {
                expression.visitBinaryMul(node, me);
            } else if (operation == DIV) {
                expression.visitBinaryDiv(node, me);
            } else if (operation == REM) {
                expression.visitBinaryRem(node, me);
            } else if (operation == ADD) {
                expression.visitBinaryAdd(node, me);
            } else if (operation == SUB) {
                expression.visitBinarySub(node, me);
            } else if (operation == LSH) {
                expression.visitBinaryLeftShift(node, me);
            } else if (operation == RSH) {
                expression.visitBinaryRightShift(node, me);
            } else if (operation == USH) {
                expression.visitBinaryUnsignedShift(node, me);
            } else if (operation == BWAND) {
                expression.visitBinaryBitwiseAnd(node, me);
            } else if (operation == XOR) {
                expression.visitBinaryXor(node, me);
            } else if (operation == BWOR) {
                expression.visitBinaryBitwiseOr(node, me);
            } else if (operation == EQ) {
                expression.visitBinaryEquals(node, me);
            } else if (operation == EQR) {
                expression.visitBinaryRefEquals(node, me);
            } else if (operation == NE) {
                expression.visitBinaryNotEquals(node, me);
            } else if (operation == NER) {
                expression.visitBinaryRefNotEquals(node, me);
            } else if (operation == GTE) {
                expression.visitBinaryGreaterEquals(node, me);
            } else if (operation == GT) {
                expression.visitBinaryGreater(node, me);
            } else if (operation == LTE) {
                expression.visitBinaryLessEquals(node, me);
            } else if (operation == LT) {
                expression.visitBinaryLess(node, me);
            } else if (operation == AND) {
                expression.visitBinaryBoolAnd(node, me);
            } else if (operation == OR) {
                expression.visitBinaryBoolOr(node, me);
            } else {
                throw new IllegalStateException(node.error("Illegal tree structure."));
            }
        } else if (type == CONDITIONAL) {
            expression.visitConditional(node, me);
        } else {
            throw new IllegalStateException(node.error("Illegal tree structure."));
        }
    }
}

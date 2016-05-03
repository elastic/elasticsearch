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

import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.tree.node.Node;
import org.elasticsearch.painless.tree.utility.Variables;
import org.elasticsearch.painless.tree.utility.Variables.Variable;

import static org.elasticsearch.painless.tree.utility.Type.ACONSTANT;
import static org.elasticsearch.painless.tree.utility.Type.DECLARATION;
import static org.elasticsearch.painless.tree.utility.Type.EXPRESSION;

class AnalyzerStatement {
    private final Definition definition;
    private final Variables variables;
    private final Analyzer analyzer;
    private final AnalyzerCaster caster;

    AnalyzerStatement(final Definition definition, final Variables variables, final Analyzer analyzer, final AnalyzerCaster caster) {
        this.definition = definition;
        this.variables = variables;
        this.analyzer = analyzer;
        this.caster = caster;
    }

    void visitIf(final Node ifelse, final MetadataStatement ifelsems) {
        final Node condition = ifelse.children.get(0);
        final MetadataExpression conditionme = new MetadataExpression();

        conditionme.expected = definition.booleanType;
        analyzer.visit(condition, conditionme);
        final Node cast = caster.markCast(condition, conditionme);
        ifelse.children.set(0, cast);

        if (cast.type == ACONSTANT) {
            throw new IllegalArgumentException(ifelse.error("Extraneous if statement."));
        }

        final Node ifblock = ifelse.children.get(1);
        final MetadataStatement ifblockms = new MetadataStatement();

        ifblockms.lastSource = ifelsems.lastSource;
        ifblockms.inLoop = ifelsems.inLoop;
        ifblockms.lastLoop = ifelsems.lastLoop;

        variables.incrementScope();
        analyzer.visit(ifblock, ifblockms);
        variables.decrementScope();

        ifblockms.anyContinue = ifelsems.anyContinue;
        ifblockms.anyBreak = ifelsems.anyBreak;
        ifblockms.statementCount = ifelsems.statementCount;

        final Node elseblock = ifelse.children.get(2);

        if (elseblock != null) {
            final MetadataStatement elseblockms = new MetadataStatement();

            elseblockms.lastSource = ifelsems.lastSource;
            elseblockms.inLoop = ifelsems.inLoop;
            elseblockms.lastLoop = ifelsems.lastLoop;

            variables.incrementScope();
            analyzer.visit(elseblock, elseblockms);
            variables.decrementScope();

            ifblockms.methodEscape = ifblockms.methodEscape && elseblockms.methodEscape;
            ifblockms.loopEscape = ifblockms.loopEscape && elseblockms.loopEscape;
            ifblockms.allEscape = ifblockms.allEscape && elseblockms.allEscape;
            ifblockms.anyContinue |= elseblockms.anyContinue;
            ifblockms.anyBreak |= elseblockms.anyBreak;
            ifblockms.statementCount = Math.max(ifblockms.statementCount, elseblockms.statementCount);

            ifelse.data.put("escape", ifblockms.allEscape);
        }
    }

    void visitWhile(final Node whil, final MetadataStatement whilems) {
        variables.incrementScope();

        final Node condition = whil.children.get(0);
        final MetadataExpression conditionme = new MetadataExpression();

        conditionme.expected = definition.booleanType;
        analyzer.visit(condition, conditionme);
        final Node cast = caster.markCast(condition, conditionme);
        whil.children.set(0, cast);

        final Node block = whil.children.get(1);

        boolean continuous = false;

        if (condition.type == ACONSTANT) {
            continuous = (boolean)condition.data.get("constant");

            if (!continuous) {
                throw new IllegalArgumentException(whil.error("Extraneous while loop."));
            }

            if (block == null) {
                throw new IllegalArgumentException(whil.error("While loop has no escape."));
            }
        }

        int count = 1;

        if (block != null) {
            final MetadataStatement blockms = new MetadataStatement();

            blockms.beginLoop = true;
            blockms.inLoop = true;

            analyzer.visit(block, blockms);

            if (blockms.loopEscape && !blockms.anyContinue) {
                throw new IllegalArgumentException(whil.error("Extranous while loop."));
            }

            if (continuous && !blockms.anyBreak) {
                whilems.methodEscape = true;
                whilems.allEscape = true;
            }

            count = Math.max(count, blockms.statementCount);
        }

        whil.data.put("counter", variables.getVariable(null, "#loop"));
        whil.data.put("count", count);
        whil.data.put("escape", whilems.allEscape);

        whilems.statementCount = 1;

        variables.decrementScope();
    }

    void visitDo(final Node dowhile, final MetadataStatement dowhilems) {
        variables.incrementScope();

        final Node block = dowhile.children.get(0);
        final MetadataStatement blockms = new MetadataStatement();

        blockms.beginLoop = true;
        blockms.inLoop = true;

        analyzer.visit(block, blockms);

        if (blockms.loopEscape && !blockms.anyContinue) {
            throw new IllegalArgumentException(dowhile.error("Extraneous do while loop."));
        }

        dowhile.data.put("count", Math.max(1, blockms.statementCount));
        dowhile.data.put("counter", variables.getVariable(null, "#loop"));

        final Node condition = dowhile.children.get(0);
        final MetadataExpression conditionme = new MetadataExpression();

        conditionme.expected = definition.booleanType;
        analyzer.visit(condition, conditionme);
        final Node cast = caster.markCast(condition, conditionme);
        dowhile.children.set(0, cast);

        if (cast.type == ACONSTANT) {
            final boolean continuous = (boolean)cast.data.get("constant");

            if (!continuous) {
                throw new IllegalArgumentException(dowhile.error("Extraneous do while loop."));
            }

            if (!blockms.anyBreak) {
                dowhilems.methodEscape = true;
                dowhilems.allEscape = true;
            }
        }

        dowhilems.statementCount = 1;

        variables.decrementScope();
    }

    void visitFor(final Node fr, final MetadataStatement frms) {
        variables.incrementScope();

        final Node initializer = fr.children.get(0);
        final Node condition = fr.children.get(1);
        final Node afterthought = fr.children.get(2);
        final Node block = fr.children.get(3);

        boolean continuous = false;

        if (initializer != null) {
            if (initializer.type == DECLARATION) {
                analyzer.visit(initializer, new MetadataStatement());

                initializer.data.put("size", 0);
            } else if (initializer.type == EXPRESSION) {
                final MetadataExpression initializerme = new MetadataExpression();

                initializerme.read = false;

                analyzer.visit(initializer, initializerme);

                if (!initializerme.statement) {
                    throw new IllegalArgumentException(initializer.error("Not a statement."));
                }

                initializer.data.put("size", initializerme.actual.sort.size);
            }
        }

        if (condition != null) {
            final MetadataExpression conditionme = new MetadataExpression();

            conditionme.expected = definition.booleanType;
            analyzer.visit(condition, conditionme);
            final Node cast = caster.markCast(condition, conditionme);
            fr.children.set(1, cast);

            if (cast.type == ACONSTANT) {
                continuous = (boolean)cast.data.get("constant");

                if (!continuous) {
                    throw new IllegalArgumentException(fr.error("Extraneous for loop."));
                }

                if (block == null) {
                    throw new IllegalArgumentException(fr.error("For loop has no escape."));
                }
            }
        } else {
            continuous = true;
        }

        if (afterthought != null) {
            final MetadataExpression afterthoughtme = new MetadataExpression();

            afterthoughtme.read = false;

            analyzer.visit(afterthought, afterthoughtme);

            if (!afterthoughtme.statement) {
                throw new IllegalArgumentException(afterthought.error("Not a statement."));
            }

            afterthought.data.put("size", afterthoughtme.actual.sort.size);
        }

        int count = 1;

        if (block != null) {
            final MetadataStatement blockms = new MetadataStatement();

            blockms.beginLoop = true;
            blockms.inLoop = true;

            analyzer.visit(block, blockms);

            if (blockms.loopEscape && !blockms.anyContinue) {
                throw new IllegalArgumentException(fr.error("Extraneous for loop."));
            }

            if (continuous && !blockms.anyBreak) {
                frms.methodEscape = true;
                frms.allEscape = true;
            }

            count = Math.max(count, blockms.statementCount) + (afterthought != null ? 1 : 0);
        }

        fr.data.put("counter", variables.getVariable(null, "#loop"));
        fr.data.put("count", count);
        fr.data.put("escape", frms.allEscape);

        frms.statementCount = 1;

        variables.decrementScope();
    }

    void visitContinue(final Node continu, final MetadataStatement continums) {
        if (!continums.inLoop) {
            throw new IllegalArgumentException(continu.error("Continue statement outside of a loop."));
        }

        if (continums.lastLoop) {
            throw new IllegalArgumentException(continu.error("Extraneous continue statement."));
        }

        continums.allEscape = true;
        continums.anyContinue = true;
        continums.statementCount = 1;
    }

    void visitBreak(final Node brake, final MetadataStatement brakems) {
        if (!brakems.inLoop) {
            throw new IllegalArgumentException(brake.error("Break statement outside of a loop."));
        }

        brakems.loopEscape = true;
        brakems.allEscape = true;
        brakems.anyBreak = true;
        brakems.statementCount = 1;
    }

    void visitReturn(final Node rtn, final MetadataStatement rtnms) {
        final Node expression = rtn.children.get(0);
        final MetadataExpression expressionme = new MetadataExpression();

        expressionme.expected = definition.objectType;
        analyzer.visit(expression, expressionme);
        rtn.children.set(0, caster.markCast(expression, expressionme));

        rtnms.methodEscape = true;
        rtnms.loopEscape = true;
        rtnms.allEscape = true;
        rtnms.statementCount = 1;
    }

    void visitTry(final Node ty, final MetadataStatement tyms) {
        final Node tyblock = ty.children.get(0);
        final MetadataStatement tyblockms = new MetadataStatement();

        tyblockms.lastSource = tyms.lastSource;
        tyblockms.inLoop = tyms.inLoop;
        tyblockms.lastLoop = tyms.lastLoop;

        variables.incrementScope();
        analyzer.visit(tyblock, tyblockms);
        variables.decrementScope();

        tyms.methodEscape = tyblockms.methodEscape;
        tyms.loopEscape = tyblockms.loopEscape;
        tyms.allEscape = tyblockms.allEscape;
        tyms.anyContinue = tyblockms.anyContinue;
        tyms.anyBreak = tyblockms.anyBreak;

        ty.data.put("escape", tyblockms.allEscape);

        int statementCount = 0;

        for (int trapCount = 1; trapCount < ty.children.size(); ++trapCount) {
            final Node trap = ty.children.get(trapCount);
            final MetadataStatement trapms = new MetadataStatement();

            trapms.lastSource = tyms.lastSource;
            trapms.inLoop = tyms.inLoop;
            trapms.lastLoop = tyms.lastLoop;

            variables.incrementScope();
            analyzer.visit(trap, trapms);
            variables.decrementScope();

            tyms.methodEscape &= trapms.methodEscape;
            tyms.loopEscape &= trapms.loopEscape;
            tyms.allEscape &= trapms.allEscape;
            tyms.anyContinue |= trapms.anyContinue;
            tyms.anyBreak |= trapms.anyBreak;

            statementCount = Math.max(statementCount, trapms.statementCount);
        }

        tyms.statementCount = tyblockms.statementCount + statementCount;
    }

    void visitTrap(final Node trap, final MetadataStatement trapms) {
        final String type = (String)trap.data.get("type");
        final String symbol = (String)trap.data.get("symbol");

        final Variable exception = variables.addVariable(definition, trap.location, type, symbol);

        try {
            exception.type.clazz.asSubclass(Exception.class);
        } catch (final ClassCastException cce) {
            throw new IllegalArgumentException(trap.error("Not an exception type [" + exception.type.name + "]."));
        }

        trap.data.put("variable", exception);

        final Node block = trap.children.get(0);

        if (block != null) {
            final MetadataStatement blockms = new MetadataStatement();

            blockms.lastSource = trapms.lastSource;
            blockms.inLoop = trapms.inLoop;
            blockms.lastLoop = trapms.lastLoop;

            analyzer.visit(block, blockms);

            trapms.methodEscape = blockms.methodEscape;
            trapms.loopEscape = blockms.loopEscape;
            trapms.allEscape = blockms.allEscape;
            trapms.anyContinue = blockms.anyContinue;
            trapms.anyBreak = blockms.anyBreak;
            trapms.statementCount = blockms.statementCount;
        }

        trap.data.put("escape", trapms.allEscape);
    }

    void visitThrow(final Node thro, final MetadataStatement throms) {
        final Node expression = thro.children.get(0);
        final MetadataExpression expressionme = new MetadataExpression();

        expressionme.expected = definition.exceptionType;
        analyzer.visit(expression, expressionme);
        thro.children.set(0, caster.markCast(expression, expressionme));

        throms.methodEscape = true;
        throms.loopEscape = true;
        throms.allEscape = true;
        throms.statementCount = 1;
    }

    void visitExpr(final Node expr, final MetadataStatement exprms) {
        final Node expression = expr.children.get(0);
        final MetadataExpression expressionme = new MetadataExpression();

        expressionme.read = exprms.lastSource;
        analyzer.visit(expression, expressionme);

        if (!expressionme.statement && !exprms.lastSource) {
            throw new IllegalArgumentException(expr.error("Not a statement."));
        }

        final boolean rtn = exprms.lastSource && expressionme.actual.sort != Sort.VOID;

        expressionme.expected = rtn ? definition.objectType : expressionme.actual;
        expr.children.set(0, caster.markCast(expression, expressionme));

        exprms.methodEscape = rtn;
        exprms.loopEscape = rtn;
        exprms.allEscape = rtn;
        exprms.statementCount = 1;

        expr.data.put("escape", exprms.methodEscape);
        expr.data.put("size", expressionme.expected.sort.size);
    }

    void visitBlock(final Node block, final MetadataStatement blockms) {
        final Node last = block.children.get(block.children.size() - 1);

        for (final Node child : block.children) {
            if (blockms.allEscape) {
                throw new IllegalArgumentException(child.error("Unreachable statement."));
            }

            final MetadataStatement childms = new MetadataStatement();
            childms.inLoop = blockms.inLoop;
            childms.lastSource = child == last;
            childms.lastLoop = (childms.beginLoop || childms.lastLoop) && child == last;
            analyzer.visit(child, childms);

            blockms.methodEscape = childms.methodEscape;
            blockms.loopEscape = childms.loopEscape;
            blockms.allEscape = childms.allEscape;
            blockms.anyContinue |= childms.anyContinue;
            blockms.anyBreak |= childms.anyBreak;
            blockms.statementCount += childms.statementCount;
        }
    }

    void visitDeclaration(final Node declaration, final MetadataStatement declarationms) {
        for (final Node child : declaration.children) {
            final String type = (String)child.data.get("type");
            final String symbol = (String)child.data.get("symbol");

            final Variable variable = variables.addVariable(definition, child.location, type, symbol);

            child.data.put("variable", variable);

            final Node expression = child.children.get(0);

            if (expression != null) {
                final MetadataExpression expressionme = new MetadataExpression();
                expressionme.expected = variable.type;

                analyzer.visit(expression, expressionme);

                child.children.set(0, caster.markCast(expression, expressionme));
            }
        }

        declarationms.statementCount = declaration.children.size();
    }
}

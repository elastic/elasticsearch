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
import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.PainlessParser.ExpressionContext;
import org.elasticsearch.painless.PainlessParser.IdentifierContext;
import org.elasticsearch.painless.PainlessParser.PrecedenceContext;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

class AnalyzerUtility {
    static class Variable {
        final String name;
        final Type type;
        final int slot;

        private Variable(final String name, final Type type, final int slot) {
            this.name = name;
            this.type = type;
            this.slot = slot;
        }
    }

    /**
     * A utility method to output consistent error messages.
     * @param ctx The ANTLR node the error occurred in.
     * @return The error message with tacked on line number and character position.
     */
    static String error(final ParserRuleContext ctx) {
        return "Analyzer Error [" + ctx.getStart().getLine() + ":" + ctx.getStart().getCharPositionInLine() + "]: ";
    }

    /**
     * A utility method to output consistent error messages for invalid types.
     * @param ctx The ANTLR node the error occurred in.
     * @param type The invalid type.
     * @return The error message with tacked on line number and character position.
     */
    static String typeError(final ParserRuleContext ctx, final String type) {
        return error(ctx) + "Invalid type [" + type + "].";
    }

    /**
     * A utility method to output consistent error messages for invalid identifiers.
     * @param ctx The ANTLR node the error occurred in.
     * @param identifier The invalid identifier.
     * @return The error message with tacked on line number and character position.
     */
    static String identifierError(final ParserRuleContext ctx, final String identifier) {
        return error(ctx) + "Invalid identifier [" + identifier + "].";
    }

    /**
     * The ANTLR parse tree is modified in one single case; a parent node needs to check a child node to see if it's
     * a precedence node, and if so, it must be removed from the tree permanently.  Once the ANTLR tree is built,
     * precedence nodes are no longer necessary to maintain the correct ordering of the tree, so they only
     * add a level of indirection where complicated decisions about metadata passing would have to be made.  This
     * method removes the need for those decisions.
     * @param source The child ANTLR node to check for precedence.
     * @return The updated child ANTLR node.
     */
    static ExpressionContext updateExpressionTree(ExpressionContext source) {
        // Check to see if the ANTLR node is a precedence node.
        if (source instanceof PainlessParser.PrecedenceContext) {
            final ParserRuleContext parent = source.getParent();
            int index = 0;

            // Mark the index of the source node within the list of child nodes from the parent.
            for (final ParseTree child : parent.children) {
                if (child == source) {
                    break;
                }

                ++index;
            }

            // If there are multiple precedence nodes in a row, remove them all.
            while (source instanceof PrecedenceContext) {
                source = ((PrecedenceContext)source).expression();
            }

            // Update the parent node with the child of the precedence node.
            parent.children.set(index, source);
        }

        return source;
    }

    private final Definition definition;

    private final Deque<Integer> scopes = new ArrayDeque<>();
    private final Deque<Variable> variables = new ArrayDeque<>();

    AnalyzerUtility(final Definition definition) {
        this.definition = definition;
    }

    void incrementScope() {
        scopes.push(0);
    }

    void decrementScope() {
        int remove = scopes.pop();

        while (remove > 0) {
            variables.pop();
            --remove;
        }
    }

    Variable getVariable(final String name) {
        final Iterator<Variable> itr = variables.iterator();

        while (itr.hasNext()) {
            final Variable variable = itr.next();

            if (variable.name.equals(name)) {
                return variable;
            }
        }

        return null;
    }

    Variable addVariable(final ParserRuleContext source, final String name, final Type type) {
        if (getVariable(name) != null) {
            if (source == null) {
                throw new IllegalArgumentException("Argument name [" + name + "] already defined within the scope.");
            } else {
                throw new IllegalArgumentException(error(source) + "Variable name [" + name + "] already defined within the scope.");
            }
        }

        final Variable previous = variables.peekFirst();
        int slot = 0;

        if (previous != null) {
            slot += previous.slot + previous.type.type.getSize();
        }

        final Variable variable = new Variable(name, type, slot);
        variables.push(variable);

        final int update = scopes.pop() + 1;
        scopes.push(update);

        return variable;
    }

    boolean isValidType(final IdentifierContext idctx, final boolean error) {
        boolean valid = definition.structs.containsKey(idctx.getText());

        if (!valid && error) {
            throw new IllegalArgumentException(typeError(idctx, idctx.getText()));
        }

        return valid;
    }

    boolean isValidIdentifier(final IdentifierContext idctx, final boolean error) {
        boolean valid = !definition.structs.containsKey(idctx.getText()) && idctx.generic() == null;

        if (!valid && error) {
            throw new IllegalArgumentException(identifierError(idctx, idctx.getText()));
        }

        return valid;
    }
}

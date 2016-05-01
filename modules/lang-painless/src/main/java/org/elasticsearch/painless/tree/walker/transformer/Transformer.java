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

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.tree.node.Node;
import org.elasticsearch.painless.tree.node.Type;
import org.objectweb.asm.Label;

import static org.elasticsearch.painless.tree.node.Type.ACAST;
import static org.elasticsearch.painless.tree.node.Type.ACONSTANT;
import static org.elasticsearch.painless.tree.node.Type.ATRANSFORM;
import static org.elasticsearch.painless.tree.node.Type.BLOCK;
import static org.elasticsearch.painless.tree.node.Type.BREAK;
import static org.elasticsearch.painless.tree.node.Type.CONTINUE;
import static org.elasticsearch.painless.tree.node.Type.DECLARATION;
import static org.elasticsearch.painless.tree.node.Type.DO;
import static org.elasticsearch.painless.tree.node.Type.EXPRESSION;
import static org.elasticsearch.painless.tree.node.Type.FOR;
import static org.elasticsearch.painless.tree.node.Type.IF;
import static org.elasticsearch.painless.tree.node.Type.NULL;
import static org.elasticsearch.painless.tree.node.Type.RETURN;
import static org.elasticsearch.painless.tree.node.Type.SOURCE;
import static org.elasticsearch.painless.tree.node.Type.THROW;
import static org.elasticsearch.painless.tree.node.Type.TRY;
import static org.elasticsearch.painless.tree.node.Type.WHILE;

public class Transformer {
    public static void transform(final CompilerSettings settings, final Definition definition, final Node source) {
        new Transformer(settings, definition, source);
    }

    private TransformerStatement statement;

    private Transformer(final CompilerSettings settings, final Definition definition, final Node source) {
        final TransformerUtility marker = new TransformerUtility();

        statement = new TransformerStatement(this, marker);

        if (source.type != SOURCE) {
            throw new IllegalStateException(source.error("Illegal tree structure."));
        }

        visitSource(source);
    }

    private void visitSource(final Node source) {
        int index = 0;

        for (final Node child : source.children) {
            final Node transform = visit(child, null, null);

            if (transform != child) {
                source.children.set(index, transform);
            }

            ++index;
        }

        final boolean escape = (boolean)source.data.get("escape");

        if (!escape) {
            source.children.add(new Node(source.location, NULL));
            source.children.add(new Node(source.location, RETURN));
        }
    }

    Node visit(final Node node) {
        return visit(node, null, null, false);
    }

    Node visit(final Node node, final boolean strings) {
        return visit(node, null, null, strings);
    }

    Node visit(final Node node, final Label label0, final Label label1) {
        return visit(node, label0, label1, false);
    }

    Node visit(final Node node, final Label label0, final Label label1, final boolean strings) {
        final Type type = node.type;

        if (type == IF) {
            return statement.visitIf(node, label0, label1);
        } else if (type == WHILE) {
            return statement.visitWhile(node);
        } else if (type == DO) {
            return statement.visitDo(node);
        } else if (type == FOR) {
            return statement.visitFor(node);
        } else if (type == CONTINUE) {
            return statement.visitContinue(node, label0);
        } else if (type == BREAK) {
            return statement.visitBreak(node, label1);
        } else if (type == RETURN) {
            return statement.visitReturn(node);
        } else if (type == TRY) {
            return statement.visitTry(node, label0, label1);
        } else if (type == THROW) {
            return statement.visitThrow(node);
        } else if (type == EXPRESSION) {
            return statement.visitExpr(node);
        } else if (type == BLOCK) {
            return statement.visitBlock(node, label0, label1);
        } else if (type == DECLARATION) {
            return statement.visitDeclaration(node);
        } else {
            throw new IllegalStateException(node.error("Illegal tree structure"));
        }
    }
}

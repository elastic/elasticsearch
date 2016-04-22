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
import org.elasticsearch.painless.tree.utility.Variables;

import static org.elasticsearch.painless.tree.node.Type.IF;
import static org.elasticsearch.painless.tree.node.Type.SOURCE;

public final class Analyzer {
    public static void analyze(final CompilerSettings settings, final Definition definition, final Variables variables, final Node source) {
        new Analyzer(settings, definition, variables, source);
    }

    private final AnalyzerStatement statement;

    private Analyzer(final CompilerSettings settings, final Definition definition, final Variables variables, final Node source) {
        final AnalyzerCaster caster = new AnalyzerCaster(definition);

        statement = new AnalyzerStatement(definition, variables, this);

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
                throw new IllegalArgumentException(source.error("Unreachable statement."));
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
        if (node.type == IF) {
            statement.visitIf(node, ms);
        } else {
            throw new IllegalStateException(node.error("Illegal tree structure."));
        }
    }

    void visit(final Node node, final MetadataExpression me) {
        throw new IllegalStateException(node.error("Illegal tree structure."));
    }
}

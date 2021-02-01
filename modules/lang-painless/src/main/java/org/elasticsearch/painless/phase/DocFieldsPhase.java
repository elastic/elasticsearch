/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.painless.node.AExpression;
import org.elasticsearch.painless.node.EBrace;
import org.elasticsearch.painless.node.ECall;
import org.elasticsearch.painless.node.EDot;
import org.elasticsearch.painless.node.EString;
import org.elasticsearch.painless.node.ESymbol;
import org.elasticsearch.painless.symbol.Decorations;
import org.elasticsearch.painless.symbol.ScriptScope;

import java.util.List;

/**
 * Find all document field accesses.
 */
public class DocFieldsPhase extends UserTreeBaseVisitor<ScriptScope> {
    @Override
    public void visitSymbol(ESymbol userSymbolNode, ScriptScope scriptScope) {
        // variables are a leaf node
        if (userSymbolNode.getSymbol().equals("doc")) {
            scriptScope.setCondition(userSymbolNode, Decorations.IsDocument.class);
        }
    }

    @Override
    public void visitBrace(EBrace userBraceNode, ScriptScope scriptScope) {
        userBraceNode.getPrefixNode().visit(this, scriptScope);
        scriptScope.replicateCondition(userBraceNode.getPrefixNode(), userBraceNode.getIndexNode(), Decorations.IsDocument.class);
        userBraceNode.getIndexNode().visit(this, scriptScope);
    }

    @Override
    public void visitDot(EDot userDotNode, ScriptScope scriptScope) {
        AExpression prefixNode = userDotNode.getPrefixNode();
        prefixNode.visit(this, scriptScope);
        if (scriptScope.getCondition(prefixNode, Decorations.IsDocument.class)) {
            scriptScope.addDocField(userDotNode.getIndex());
        }
    }

    @Override
    public void visitCall(ECall userCallNode, ScriptScope scriptScope) {
        // looking for doc.get
        AExpression prefixNode = userCallNode.getPrefixNode();
        prefixNode.visit(this, scriptScope);

        List<AExpression> argumentNodes = userCallNode.getArgumentNodes();
        if (argumentNodes.size() != 1 || userCallNode.getMethodName().equals("get") == false) {
            for (AExpression argumentNode : argumentNodes) {
                argumentNode.visit(this, scriptScope);
            }
        } else {
            AExpression argument = argumentNodes.get(0);
            scriptScope.replicateCondition(prefixNode, argument, Decorations.IsDocument.class);
            argument.visit(this, scriptScope);
        }
    }

    @Override
    public void visitString(EString userStringNode, ScriptScope scriptScope) {
        if (scriptScope.getCondition(userStringNode, Decorations.IsDocument.class)) {
            scriptScope.addDocField(userStringNode.getString());
        }
    }
}

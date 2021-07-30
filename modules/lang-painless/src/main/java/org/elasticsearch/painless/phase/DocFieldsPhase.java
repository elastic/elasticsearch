/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

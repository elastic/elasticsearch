/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.phase.IRTreeVisitor;
import org.elasticsearch.painless.symbol.IRDecorations.IRCAllEscape;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.objectweb.asm.util.Printer;

import java.util.ArrayList;
import java.util.List;

public class ClassNode extends IRNode {

    /* ---- begin tree structure ---- */

    private final List<FieldNode> fieldNodes = new ArrayList<>();
    private final List<FunctionNode> functionNodes = new ArrayList<>();
    private final BlockNode clinitBlockNode;

    public void addFieldNode(FieldNode fieldNode) {
        fieldNodes.add(fieldNode);
    }

    public List<FieldNode> getFieldsNodes() {
        return fieldNodes;
    }

    public void addFunctionNode(FunctionNode functionNode) {
        functionNodes.add(functionNode);
    }

    public List<FunctionNode> getFunctionsNodes() {
        return functionNodes;
    }

    public BlockNode getClinitBlockNode() {
        return clinitBlockNode;
    }

    /* ---- end tree structure, begin node data ---- */

    private Printer debugStream;
    private ScriptScope scriptScope;
    private byte[] bytes;

    public void setDebugStream(Printer debugStream) {
        this.debugStream = debugStream;
    }

    public Printer getDebugStream() {
        return debugStream;
    }

    public void setScriptScope(ScriptScope scriptScope) {
        this.scriptScope = scriptScope;
    }

    public ScriptScope getScriptScope() {
        return scriptScope;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitClass(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        clinitBlockNode.visit(irTreeVisitor, scope);

        for (FunctionNode functionNode : functionNodes) {
            functionNode.visit(irTreeVisitor, scope);
        }
    }

    /* ---- end visitor ---- */

    public ClassNode(Location location) {
        super(location);

        clinitBlockNode = new BlockNode(new Location("internal$clinit$blocknode", 0));
        clinitBlockNode.attachCondition(IRCAllEscape.class);
    }

}

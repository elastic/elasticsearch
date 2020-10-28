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

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.phase.IRTreeVisitor;
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
        clinitBlockNode.setAllEscape(true);
    }

}

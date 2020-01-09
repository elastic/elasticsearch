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

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptClassInfo;
import org.elasticsearch.painless.symbol.ScopeTable;
import org.elasticsearch.painless.symbol.ScriptRoot;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.util.Printer;

import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static org.elasticsearch.painless.WriterConstants.BASE_INTERFACE_TYPE;
import static org.elasticsearch.painless.WriterConstants.CLASS_TYPE;

public class ClassNode extends IRNode {

    /* ---- begin tree structure ---- */

    private final List<FieldNode> fieldNodes = new ArrayList<>();
    private final List<FunctionNode> functionNodes = new ArrayList<>();
    private final FunctionNode clinitNode;

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

    public FunctionNode getClinitNode() {
        return clinitNode;
    }

    /* ---- end tree structure, begin node data ---- */

    private ScriptClassInfo scriptClassInfo;
    private String name;
    private String sourceText;
    private Printer debugStream;
    private ScriptRoot scriptRoot;

    public void setScriptClassInfo(ScriptClassInfo scriptClassInfo) {
        this.scriptClassInfo = scriptClassInfo;
    }

    public ScriptClassInfo getScriptClassInfo() {
        return scriptClassInfo;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setSourceText(String sourceText) {
        this.sourceText = sourceText;
    }

    public String getSourceText() {
        return sourceText;
    }

    public void setDebugStream(Printer debugStream) {
        this.debugStream = debugStream;
    }

    public Printer getDebugStream() {
        return debugStream;
    }

    public void setScriptRoot(ScriptRoot scriptRoot) {
        this.scriptRoot = scriptRoot;
    }

    public ScriptRoot getScriptRoot() {
        return scriptRoot;
    }

    /* ---- end node data ---- */

    public ClassNode() {
        clinitNode = new FunctionNode();
        clinitNode.setLocation(new Location("internal$clinit", 0));
        clinitNode.setName("<clinit>");
        clinitNode.setReturnType(void.class);
        clinitNode.setStatic(true);
        clinitNode.setVarArgs(false);
        clinitNode.setSynthetic(false);
        clinitNode.setMaxLoopCounter(0);

        BlockNode blockNode = new BlockNode();
        blockNode.setLocation(new Location("internal$clinit$blocknode", 0));
        blockNode.setAllEscape(true);
        blockNode.setStatementCount(1);

        clinitNode.setBlockNode(blockNode);
    }

    public byte[] write() {
        BitSet statements = new BitSet(sourceText.length());
        scriptRoot.addStaticConstant("$STATEMENTS", statements);

        // Create the ClassWriter.

        int classFrames = org.objectweb.asm.ClassWriter.COMPUTE_FRAMES | org.objectweb.asm.ClassWriter.COMPUTE_MAXS;
        int classAccess = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL;
        String interfaceBase = BASE_INTERFACE_TYPE.getInternalName();
        String className = CLASS_TYPE.getInternalName();
        String[] classInterfaces = new String[] { interfaceBase };

        ClassWriter classWriter = new ClassWriter(scriptRoot.getCompilerSettings(), statements, debugStream,
                scriptClassInfo.getBaseClass(), classFrames, classAccess, className, classInterfaces);
        ClassVisitor classVisitor = classWriter.getClassVisitor();
        classVisitor.visitSource(Location.computeSourceName(name), null);

        org.objectweb.asm.commons.Method init;

        if (scriptClassInfo.getBaseClass().getConstructors().length == 0) {
            init = new org.objectweb.asm.commons.Method("<init>", MethodType.methodType(void.class).toMethodDescriptorString());
        } else {
            init = new org.objectweb.asm.commons.Method("<init>", MethodType.methodType(void.class,
                scriptClassInfo.getBaseClass().getConstructors()[0].getParameterTypes()).toMethodDescriptorString());
        }

        // Write the constructor:
        MethodWriter constructor = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, init);
        constructor.visitCode();
        constructor.loadThis();
        constructor.loadArgs();
        constructor.invokeConstructor(Type.getType(scriptClassInfo.getBaseClass()), init);
        constructor.returnValue();
        constructor.endMethod();

        if (clinitNode.getBlockNode().getStatementsNodes().isEmpty() == false) {
            ReturnNode returnNode = new ReturnNode();
            returnNode.setLocation(new Location("internal$clinit$return", 0));
            clinitNode.getBlockNode().addStatementNode(returnNode);

            clinitNode.write(classWriter, null, new ScopeTable());
        }

        // Write all fields:
        for (FieldNode fieldNode : fieldNodes) {
            fieldNode.write(classWriter, null, null);
        }

        // Write all functions:
        for (FunctionNode functionNode : functionNodes) {
            functionNode.write(classWriter, null, new ScopeTable());
        }

        // End writing the class and store the generated bytes.
        classVisitor.visitEnd();
        return classWriter.getClassBytes();
    }
}

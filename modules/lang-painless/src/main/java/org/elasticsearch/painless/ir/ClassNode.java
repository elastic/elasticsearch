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
import org.elasticsearch.painless.phase.IRTreeVisitor;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.WriteScope;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;
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

    public ClassNode() {
        clinitBlockNode = new BlockNode();
        clinitBlockNode.setLocation(new Location("internal$clinit$blocknode", 0));
        clinitBlockNode.setAllEscape(true);
        clinitBlockNode.setStatementCount(1);
    }

    public byte[] write() {
        ScriptClassInfo scriptClassInfo = scriptScope.getScriptClassInfo();
        BitSet statements = new BitSet(scriptScope.getScriptSource().length());
        scriptScope.addStaticConstant("$STATEMENTS", statements);

        // Create the ClassWriter.

        int classFrames = org.objectweb.asm.ClassWriter.COMPUTE_FRAMES | org.objectweb.asm.ClassWriter.COMPUTE_MAXS;
        int classAccess = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL;
        String interfaceBase = BASE_INTERFACE_TYPE.getInternalName();
        String className = CLASS_TYPE.getInternalName();
        String[] classInterfaces = new String[] { interfaceBase };

        ClassWriter classWriter = new ClassWriter(scriptScope.getCompilerSettings(), statements, debugStream,
                scriptClassInfo.getBaseClass(), classFrames, classAccess, className, classInterfaces);
        ClassVisitor classVisitor = classWriter.getClassVisitor();
        classVisitor.visitSource(Location.computeSourceName(scriptScope.getScriptName()), null);

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

        if (clinitBlockNode.getStatementsNodes().isEmpty() == false) {
            MethodWriter methodWriter = classWriter.newMethodWriter(
                    Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC,
                    new Method("<clinit>", Type.getType(void.class), new Type[0]));
            clinitBlockNode.write(classWriter, methodWriter, new WriteScope());
            methodWriter.returnValue();
            methodWriter.endMethod();
        }

        // Write all fields:
        for (FieldNode fieldNode : fieldNodes) {
            fieldNode.write(classWriter, null, null);
        }

        // Write all functions:
        for (FunctionNode functionNode : functionNodes) {
            functionNode.write(classWriter, null, new WriteScope());
        }

        // End writing the class and store the generated bytes.
        classVisitor.visitEnd();
        return classWriter.getClassBytes();
    }

    @Override
    public void write(ClassWriter classWriter, MethodWriter methodWriter, WriteScope writeScope) {
        throw new UnsupportedOperationException("use write() instead");
    }
}

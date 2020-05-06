package org.elasticsearch.painless;/*
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

import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.CallNode;
import org.elasticsearch.painless.ir.CallSubNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.FieldNode;
import org.elasticsearch.painless.ir.FunctionNode;
import org.elasticsearch.painless.ir.MemberFieldLoadNode;
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.ir.StaticNode;
import org.elasticsearch.painless.ir.VariableNode;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.symbol.FunctionTable;
import org.objectweb.asm.Opcodes;

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.util.Arrays;

/**
 * This injects additional ir nodes required for
 * resolving the def type at runtime. This includes injection
 * of ir nodes to add a function to call
 * {@link DefBootstrap#bootstrap(PainlessLookup, FunctionTable, Lookup, String, MethodType, int, int, Object...)}
 * to do the runtime resolution.
 */
public class DefBootstrapInjectionPhase {

    public static void phase(ClassNode classNode) {
        injectStaticFields(classNode);
        injectDefBootstrapMethod(classNode);
    }

    // adds static fields required for def bootstrapping
    protected static void injectStaticFields(ClassNode classNode) {
        Location internalLocation = new Location("$internal$DefBootstrapInjectionPhase$injectStaticFields", 0);
        int modifiers = Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC;

        FieldNode fieldNode = new FieldNode();
        fieldNode.setLocation(internalLocation);
        fieldNode.setModifiers(modifiers);
        fieldNode.setFieldType(PainlessLookup.class);
        fieldNode.setName("$DEFINITION");

        classNode.addFieldNode(fieldNode);

        fieldNode = new FieldNode();
        fieldNode.setLocation(internalLocation);
        fieldNode.setModifiers(modifiers);
        fieldNode.setFieldType(FunctionTable.class);
        fieldNode.setName("$FUNCTIONS");

        classNode.addFieldNode(fieldNode);
    }

    // adds the bootstrap method required for dynamic binding for def type resolution
    protected static void injectDefBootstrapMethod(ClassNode classNode) {
        Location internalLocation = new Location("$internal$DefBootstrapInjectionPhase$injectDefBootstrapMethod", 0);

        try {
            FunctionNode functionNode = new FunctionNode();
            functionNode.setLocation(internalLocation);
            functionNode.setReturnType(CallSite.class);
            functionNode.setName("$bootstrapDef");
            functionNode.getTypeParameters().addAll(
                    Arrays.asList(Lookup.class, String.class, MethodType.class, int.class, int.class, Object[].class));
            functionNode.getParameterNames().addAll(
                    Arrays.asList("methodHandlesLookup", "name", "type", "initialDepth", "flavor", "args"));
            functionNode.setStatic(true);
            functionNode.setVarArgs(true);
            functionNode.setSynthetic(true);
            functionNode.setMaxLoopCounter(0);

            classNode.addFunctionNode(functionNode);

            BlockNode blockNode = new BlockNode();
            blockNode.setLocation(internalLocation);
            blockNode.setAllEscape(true);
            blockNode.setStatementCount(1);

            functionNode.setBlockNode(blockNode);

            ReturnNode returnNode = new ReturnNode();
            returnNode.setLocation(internalLocation);

            blockNode.addStatementNode(returnNode);

            CallNode callNode = new CallNode();
            callNode.setLocation(internalLocation);
            callNode.setExpressionType(CallSite.class);

            returnNode.setExpressionNode(callNode);

            StaticNode staticNode = new StaticNode();
            staticNode.setLocation(internalLocation);
            staticNode.setExpressionType(DefBootstrap.class);

            callNode.setLeftNode(staticNode);

            CallSubNode callSubNode = new CallSubNode();
            callSubNode.setLocation(internalLocation);
            callSubNode.setExpressionType(CallSite.class);
            callSubNode.setMethod(new PainlessMethod(
                    DefBootstrap.class.getMethod("bootstrap",
                            PainlessLookup.class,
                            FunctionTable.class,
                            Lookup.class,
                            String.class,
                            MethodType.class,
                            int.class,
                            int.class,
                            Object[].class),
                    DefBootstrap.class,
                    CallSite.class,
                    Arrays.asList(
                            PainlessLookup.class,
                            FunctionTable.class,
                            Lookup.class,
                            String.class,
                            MethodType.class,
                            int.class,
                            int.class,
                            Object[].class),
                    null,
                    null,
                    null
                    )
            );
            callSubNode.setBox(DefBootstrap.class);

            callNode.setRightNode(callSubNode);

            MemberFieldLoadNode memberFieldLoadNode = new MemberFieldLoadNode();
            memberFieldLoadNode.setLocation(internalLocation);
            memberFieldLoadNode.setExpressionType(PainlessLookup.class);
            memberFieldLoadNode.setName("$DEFINITION");
            memberFieldLoadNode.setStatic(true);

            callSubNode.addArgumentNode(memberFieldLoadNode);

            memberFieldLoadNode = new MemberFieldLoadNode();
            memberFieldLoadNode.setLocation(internalLocation);
            memberFieldLoadNode.setExpressionType(FunctionTable.class);
            memberFieldLoadNode.setName("$FUNCTIONS");
            memberFieldLoadNode.setStatic(true);

            callSubNode.addArgumentNode(memberFieldLoadNode);

            VariableNode variableNode = new VariableNode();
            variableNode.setLocation(internalLocation);
            variableNode.setExpressionType(Lookup.class);
            variableNode.setName("methodHandlesLookup");

            callSubNode.addArgumentNode(variableNode);

            variableNode = new VariableNode();
            variableNode.setLocation(internalLocation);
            variableNode.setExpressionType(String.class);
            variableNode.setName("name");

            callSubNode.addArgumentNode(variableNode);

            variableNode = new VariableNode();
            variableNode.setLocation(internalLocation);
            variableNode.setExpressionType(MethodType.class);
            variableNode.setName("type");

            callSubNode.addArgumentNode(variableNode);

            variableNode = new VariableNode();
            variableNode.setLocation(internalLocation);
            variableNode.setExpressionType(int.class);
            variableNode.setName("initialDepth");

            callSubNode.addArgumentNode(variableNode);

            variableNode = new VariableNode();
            variableNode.setLocation(internalLocation);
            variableNode.setExpressionType(int.class);
            variableNode.setName("flavor");

            callSubNode.addArgumentNode(variableNode);

            variableNode = new VariableNode();
            variableNode.setLocation(internalLocation);
            variableNode.setExpressionType(Object[].class);
            variableNode.setName("args");

            callSubNode.addArgumentNode(variableNode);
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private DefBootstrapInjectionPhase() {
        // do nothing
    }
}

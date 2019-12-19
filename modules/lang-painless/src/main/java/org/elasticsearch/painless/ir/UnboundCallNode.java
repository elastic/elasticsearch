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
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

import java.util.Objects;

import static org.elasticsearch.painless.WriterConstants.CLASS_TYPE;

public class UnboundCallNode extends ArgumentsNode {

    /* ---- begin node data ---- */

    protected LocalFunction localFunction;
    protected PainlessMethod importedMethod;
    protected PainlessClassBinding classBinding;
    protected int classBindingOffset;
    protected PainlessInstanceBinding instanceBinding;
    protected String bindingName;

    public UnboundCallNode setLocalFunction(LocalFunction localFunction) {
        this.localFunction = localFunction;
        return this;
    }

    public LocalFunction getLocalFunction() {
        return localFunction;
    }

    public UnboundCallNode setImportedMethod(PainlessMethod importedMethod) {
        this.importedMethod = importedMethod;
        return this;
    }

    public PainlessMethod getImportedMethod() {
        return importedMethod;
    }

    public UnboundCallNode setClassBinding(PainlessClassBinding classBinding) {
        this.classBinding = classBinding;
        return this;
    }

    public PainlessClassBinding getClassBinding() {
        return classBinding;
    }

    public UnboundCallNode setClassBindingOffset(int classBindingOffset) {
        this.classBindingOffset = classBindingOffset;
        return this;
    }

    public int getClassBindingOffset() {
        return classBindingOffset;
    }

    public UnboundCallNode setInstanceBinding(PainlessInstanceBinding instanceBinding) {
        this.instanceBinding = instanceBinding;
        return this;
    }

    public PainlessInstanceBinding getInstanceBinding() {
        return instanceBinding;
    }

    public UnboundCallNode setBindingName(String bindingName) {
        this.bindingName = bindingName;
        return this;
    }

    public String getBindingName() {
        return bindingName;
    }

    /* ---- end node data ---- */

    public UnboundCallNode() {
        // do nothing
    }

    @Override
    public void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        if (localFunction != null) {
            for (ExpressionNode argumentNode : argumentNodes) {
                argumentNode.write(classWriter, methodWriter, globals);
            }

            methodWriter.invokeStatic(CLASS_TYPE, localFunction.getAsmMethod());
        } else if (importedMethod != null) {
            for (ExpressionNode argumentNode : argumentNodes) {
                argumentNode.write(classWriter, methodWriter, globals);
            }

            methodWriter.invokeStatic(Type.getType(importedMethod.targetClass),
                    new Method(importedMethod.javaMethod.getName(), importedMethod.methodType.toMethodDescriptorString()));
        } else if (classBinding != null) {
            Type type = Type.getType(classBinding.javaConstructor.getDeclaringClass());
            int javaConstructorParameterCount = classBinding.javaConstructor.getParameterCount() - classBindingOffset;

            Label nonNull = new Label();

            methodWriter.loadThis();
            methodWriter.getField(CLASS_TYPE, bindingName, type);
            methodWriter.ifNonNull(nonNull);
            methodWriter.loadThis();
            methodWriter.newInstance(type);
            methodWriter.dup();

            if (classBindingOffset == 1) {
                methodWriter.loadThis();
            }

            for (int argument = 0; argument < javaConstructorParameterCount; ++argument) {
                argumentNodes.get(argument).write(classWriter, methodWriter, globals);
            }

            methodWriter.invokeConstructor(type, Method.getMethod(classBinding.javaConstructor));
            methodWriter.putField(CLASS_TYPE, bindingName, type);

            methodWriter.mark(nonNull);
            methodWriter.loadThis();
            methodWriter.getField(CLASS_TYPE, bindingName, type);

            for (int argument = 0; argument < classBinding.javaMethod.getParameterCount(); ++argument) {
                argumentNodes.get(argument + javaConstructorParameterCount).write(classWriter, methodWriter, globals);
            }

            methodWriter.invokeVirtual(type, Method.getMethod(classBinding.javaMethod));
        } else if (instanceBinding != null) {
            Type type = Type.getType(instanceBinding.targetInstance.getClass());

            methodWriter.loadThis();
            methodWriter.getStatic(CLASS_TYPE, bindingName, type);

            for (int argument = 0; argument < instanceBinding.javaMethod.getParameterCount(); ++argument) {
                argumentNodes.get(argument).write(classWriter, methodWriter, globals);
            }

            methodWriter.invokeVirtual(type, Method.getMethod(instanceBinding.javaMethod));
        } else {
            throw new IllegalStateException("invalid unbound call");
        }
    }
}

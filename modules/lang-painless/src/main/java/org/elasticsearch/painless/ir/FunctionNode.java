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
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.symbol.ScopeTable;
import org.elasticsearch.painless.symbol.ScopeTable.Variable;
import org.elasticsearch.painless.symbol.ScriptRoot;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.painless.WriterConstants.BOOTSTRAP_METHOD_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.CLASS_TYPE;
import static org.elasticsearch.painless.WriterConstants.COLLECTIONS_TYPE;
import static org.elasticsearch.painless.WriterConstants.CONVERT_TO_SCRIPT_EXCEPTION_METHOD;
import static org.elasticsearch.painless.WriterConstants.DEFINITION_TYPE;
import static org.elasticsearch.painless.WriterConstants.EMPTY_MAP_METHOD;
import static org.elasticsearch.painless.WriterConstants.EXCEPTION_TYPE;
import static org.elasticsearch.painless.WriterConstants.OUT_OF_MEMORY_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_EXPLAIN_ERROR_GET_HEADERS_METHOD;
import static org.elasticsearch.painless.WriterConstants.PAINLESS_EXPLAIN_ERROR_TYPE;
import static org.elasticsearch.painless.WriterConstants.STACK_OVERFLOW_ERROR_TYPE;

public class FunctionNode extends IRNode {

    /* ---- begin tree structure ---- */

    private BlockNode blockNode;

    public void setBlockNode(BlockNode blockNode) {
        this.blockNode = blockNode;
    }

    public BlockNode getBlockNode() {
        return blockNode;
    }

    /* ---- end tree structure, begin node data ---- */

    private ScriptRoot scriptRoot;
    private String name;
    private Class<?> returnType;
    private List<Class<?>> typeParameters = new ArrayList<>();
    private List<String> parameterNames = new ArrayList<>();
    private boolean isStatic;
    private boolean isSynthetic;
    private int maxLoopCounter;

    public void setScriptRoot(ScriptRoot scriptRoot) {
        this.scriptRoot = scriptRoot;
    }

    public ScriptRoot getScriptRoot() {
        return scriptRoot;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setReturnType(Class<?> returnType) {
        this.returnType = returnType;
    }

    public Class<?> getReturnType() {
        return returnType;
    }

    public void addTypeParameter(Class<?> typeParameter) {
        typeParameters.add(typeParameter);
    }

    public List<Class<?>> getTypeParameters() {
        return typeParameters;
    }

    public void addParameterName(String parameterName) {
        parameterNames.add(parameterName);
    }

    public List<String> getParameterNames() {
        return parameterNames;
    }

    public void setStatic(boolean isStatic) {
        this.isStatic = isStatic;
    }

    public boolean isStatic() {
        return isStatic;
    }

    public void setSynthetic(boolean isSythetic) {
        this.isSynthetic = isSythetic;
    }

    public boolean isSynthetic() {
        return isSynthetic;
    }

    public void setMaxLoopCounter(int maxLoopCounter) {
        this.maxLoopCounter = maxLoopCounter;
    }

    public int getMaxLoopCounter() {
        return maxLoopCounter;
    }

    /* ---- end node data ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        int access = Opcodes.ACC_PUBLIC;

        if (isStatic) {
            access |= Opcodes.ACC_STATIC;
        } else {
            scopeTable.defineInternalVariable(Object.class, "this");
        }

        if (isSynthetic) {
            access |= Opcodes.ACC_SYNTHETIC;
        }

        Type asmReturnType = MethodWriter.getType(returnType);
        Type[] asmParameterTypes = new Type[typeParameters.size()];

        for (int index = 0; index < asmParameterTypes.length; ++index) {
            Class<?> type = typeParameters.get(index);
            String name = parameterNames.get(index);
            scopeTable.defineVariable(type, name);
            asmParameterTypes[index] = MethodWriter.getType(typeParameters.get(index));
        }

        Method method = new Method(name, asmReturnType, asmParameterTypes);

        methodWriter = classWriter.newMethodWriter(access, method);
        methodWriter.visitCode();

        // TODO: do not specialize for execute
        // TODO: https://github.com/elastic/elasticsearch/issues/51841
        // create labels for the potential try/catch blocks in "execute"
        Label startTry = new Label();
        Label endTry = new Label();
        Label startExplainCatch = new Label();
        Label startOtherCatch = new Label();
        Label endCatch = new Label();

        if ("execute".equals(name)) {
            methodWriter.mark(startTry);
        }
        // TODO: end

        if (maxLoopCounter > 0) {
            // if there is infinite loop protection, we do this once:
            // int #loop = settings.getMaxLoopCounter()

            Variable loop = scopeTable.defineInternalVariable(int.class, "loop");

            methodWriter.push(maxLoopCounter);
            methodWriter.visitVarInsn(Opcodes.ISTORE, loop.getSlot());
        }

        blockNode.write(classWriter, methodWriter, globals, scopeTable.newScope());

        // TODO: do not specialize for execute
        // TODO: https://github.com/elastic/elasticsearch/issues/51841
        if ("execute".equals(name)) {
            methodWriter.mark(endTry);
            methodWriter.goTo(endCatch);
            // This looks like:
            // } catch (PainlessExplainError e) {
            //   throw this.convertToScriptException(e, e.getHeaders($DEFINITION))
            // }
            methodWriter.visitTryCatchBlock(startTry, endTry, startExplainCatch, PAINLESS_EXPLAIN_ERROR_TYPE.getInternalName());
            methodWriter.mark(startExplainCatch);
            methodWriter.loadThis();
            methodWriter.swap();
            methodWriter.dup();
            methodWriter.getStatic(CLASS_TYPE, "$DEFINITION", DEFINITION_TYPE);
            methodWriter.invokeVirtual(PAINLESS_EXPLAIN_ERROR_TYPE, PAINLESS_EXPLAIN_ERROR_GET_HEADERS_METHOD);
            methodWriter.invokeVirtual(CLASS_TYPE, CONVERT_TO_SCRIPT_EXCEPTION_METHOD);
            methodWriter.throwException();
            // This looks like:
            // } catch (PainlessError | BootstrapMethodError | OutOfMemoryError | StackOverflowError | Exception e) {
            //   throw this.convertToScriptException(e, e.getHeaders())
            // }
            // We *think* it is ok to catch OutOfMemoryError and StackOverflowError because Painless is stateless
            methodWriter.visitTryCatchBlock(startTry, endTry, startOtherCatch, PAINLESS_ERROR_TYPE.getInternalName());
            methodWriter.visitTryCatchBlock(startTry, endTry, startOtherCatch, BOOTSTRAP_METHOD_ERROR_TYPE.getInternalName());
            methodWriter.visitTryCatchBlock(startTry, endTry, startOtherCatch, OUT_OF_MEMORY_ERROR_TYPE.getInternalName());
            methodWriter.visitTryCatchBlock(startTry, endTry, startOtherCatch, STACK_OVERFLOW_ERROR_TYPE.getInternalName());
            methodWriter.visitTryCatchBlock(startTry, endTry, startOtherCatch, EXCEPTION_TYPE.getInternalName());
            methodWriter.mark(startOtherCatch);
            methodWriter.loadThis();
            methodWriter.swap();
            methodWriter.invokeStatic(COLLECTIONS_TYPE, EMPTY_MAP_METHOD);
            methodWriter.invokeVirtual(CLASS_TYPE, CONVERT_TO_SCRIPT_EXCEPTION_METHOD);
            methodWriter.throwException();
            methodWriter.mark(endCatch);
        }
        // TODO: end

        methodWriter.endMethod();
    }
}

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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Constant;
import org.elasticsearch.painless.Def;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Parameter;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.WriterConstants;
import org.elasticsearch.painless.node.SSource.Reserved;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;

import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.painless.WriterConstants.CLASS_TYPE;

/**
 * Represents a user-defined function.
 */
public final class SFunction extends AStatement {
    public static final class FunctionReserved implements Reserved {
        public static final String THIS = "#this";
        public static final String LOOP = "#loop";

        private int maxLoopCounter = 0;

        public void markReserved(String name) {
            // Do nothing.
        }

        public boolean isReserved(String name) {
            return name.equals(THIS) || name.equals(LOOP);
        }

        @Override
        public void setMaxLoopCounter(int max) {
            maxLoopCounter = max;
        }

        @Override
        public int getMaxLoopCounter() {
            return maxLoopCounter;
        }
    }

    final FunctionReserved reserved;
    private final String rtnTypeStr;
    public final String name;
    private final List<String> paramTypeStrs;
    private final List<String> paramNameStrs;
    private final List<AStatement> statements;
    public final boolean synthetic;

    Type rtnType = null;
    List<Parameter> parameters = new ArrayList<>();
    Method method = null;

    private Variable loop = null;

    public SFunction(FunctionReserved reserved, Location location, String rtnType, String name,
                     List<String> paramTypes, List<String> paramNames, List<AStatement> statements,
                     boolean synthetic) {
        super(location);

        this.reserved = Objects.requireNonNull(reserved);
        this.rtnTypeStr = Objects.requireNonNull(rtnType);
        this.name = Objects.requireNonNull(name);
        this.paramTypeStrs = Collections.unmodifiableList(paramTypes);
        this.paramNameStrs = Collections.unmodifiableList(paramNames);
        this.statements = Collections.unmodifiableList(statements);
        this.synthetic = synthetic;
    }

    @Override
    void extractVariables(Set<String> variables) {
        // we should never be extracting from a function, as functions are top-level!
        throw new IllegalStateException("Illegal tree structure");
    }

    void generateSignature() {
        try {
            rtnType = Definition.getType(rtnTypeStr);
        } catch (IllegalArgumentException exception) {
            throw createError(new IllegalArgumentException("Illegal return type [" + rtnTypeStr + "] for function [" + name + "]."));
        }

        if (paramTypeStrs.size() != paramNameStrs.size()) {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }

        Class<?>[] paramClasses = new Class<?>[this.paramTypeStrs.size()];
        List<Type> paramTypes = new ArrayList<>();

        for (int param = 0; param < this.paramTypeStrs.size(); ++param) {
            try {
                Type paramType = Definition.getType(this.paramTypeStrs.get(param));

                paramClasses[param] = paramType.clazz;
                paramTypes.add(paramType);
                parameters.add(new Parameter(location, paramNameStrs.get(param), paramType));
            } catch (IllegalArgumentException exception) {
                throw createError(new IllegalArgumentException(
                    "Illegal parameter type [" + this.paramTypeStrs.get(param) + "] for function [" + name + "]."));
            }
        }

        org.objectweb.asm.commons.Method method =
            new org.objectweb.asm.commons.Method(name, MethodType.methodType(rtnType.clazz, paramClasses).toMethodDescriptorString());
        this.method = new Method(name, null, false, rtnType, paramTypes, method, Modifier.STATIC | Modifier.PRIVATE, null);
    }

    @Override
    void analyze(Locals locals) {
        if (statements == null || statements.isEmpty()) {
            throw createError(new IllegalArgumentException("Cannot generate an empty function [" + name + "]."));
        }

        locals = Locals.newLocalScope(locals);

        AStatement last = statements.get(statements.size() - 1);

        for (AStatement statement : statements) {
            // Note that we do not need to check after the last statement because
            // there is no statement that can be unreachable after the last.
            if (allEscape) {
                throw createError(new IllegalArgumentException("Unreachable statement."));
            }

            statement.lastSource = statement == last;

            statement.analyze(locals);

            methodEscape = statement.methodEscape;
            allEscape = statement.allEscape;
        }

        if (!methodEscape && rtnType.sort != Sort.VOID) {
            throw createError(new IllegalArgumentException("Not all paths provide a return value for method [" + name + "]."));
        }

        if (reserved.getMaxLoopCounter() > 0) {
            loop = locals.getVariable(null, FunctionReserved.LOOP);
        }
    }

    /** Writes the function to given ClassVisitor. */
    void write (ClassVisitor writer, CompilerSettings settings, Globals globals) {
        int access = Opcodes.ACC_PRIVATE | Opcodes.ACC_STATIC;
        if (synthetic) {
            access |= Opcodes.ACC_SYNTHETIC;
        }
        final MethodWriter function = new MethodWriter(access, method.method, writer, globals.getStatements(), settings);
        function.visitCode();
        write(function, globals);
        function.endMethod();
    }

    @Override
    void write(MethodWriter function, Globals globals) {
        if (reserved.getMaxLoopCounter() > 0) {
            // if there is infinite loop protection, we do this once:
            // int #loop = settings.getMaxLoopCounter()
            function.push(reserved.getMaxLoopCounter());
            function.visitVarInsn(Opcodes.ISTORE, loop.getSlot());
        }

        for (AStatement statement : statements) {
            statement.write(function, globals);
        }

        if (!methodEscape) {
            if (rtnType.sort == Sort.VOID) {
                function.returnValue();
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        String staticHandleFieldName = Def.getUserFunctionHandleFieldName(name, parameters.size());
        globals.addConstantInitializer(new Constant(location, WriterConstants.METHOD_HANDLE_TYPE,
                                                    staticHandleFieldName, this::initializeConstant));
    }

    private void initializeConstant(MethodWriter writer) {
        final Handle handle = new Handle(Opcodes.H_INVOKESTATIC,
                CLASS_TYPE.getInternalName(),
                name,
                method.method.getDescriptor(),
                false);
        writer.push(handle);
    }
}

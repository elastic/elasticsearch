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

import org.elasticsearch.painless.Def;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Parameter;
import org.elasticsearch.painless.Locals.FunctionReserved;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.WriterConstants;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;

import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.painless.WriterConstants.CLASS_TYPE;

/**
 * Represents a user-defined function.
 */
public class SFunction extends AStatement {
    final FunctionReserved reserved;
    final String rtnTypeStr;
    final String name;
    final List<String> paramTypeStrs;
    final List<String> paramNameStrs;
    final List<AStatement> statements;
    final boolean synthetic;

    Type rtnType = null;
    List<Parameter> parameters = new ArrayList<>();
    Method method = null;

    Locals locals = null;

    public SFunction(FunctionReserved reserved, Location location,
                     String rtnType, String name, List<String> paramTypes, 
                     List<String> paramNames, List<AStatement> statements, boolean synthetic) {
        super(location);

        this.reserved = reserved;
        this.rtnTypeStr = rtnType;
        this.name = name;
        this.paramTypeStrs = Collections.unmodifiableList(paramTypes);
        this.paramNameStrs = Collections.unmodifiableList(paramNames);
        this.statements = Collections.unmodifiableList(statements);
        this.synthetic = synthetic;
    }

    void generate() {
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
        this.method = new Method(name, null, rtnType, paramTypes, method, Modifier.STATIC | Modifier.PRIVATE, null);
    }

    @Override
    void analyze(Locals locals) {
        if (statements == null || statements.isEmpty()) {
            throw createError(new IllegalArgumentException("Cannot generate an empty function [" + name + "]."));
        }

        this.locals = new Locals(reserved, locals, rtnType, parameters);
        locals = this.locals;

        locals.incrementScope();

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

        locals.decrementScope();

        String staticHandleFieldName = Def.getUserFunctionHandleFieldName(name, parameters.size());
        locals.addConstant(location, WriterConstants.METHOD_HANDLE_TYPE, staticHandleFieldName, this::initializeConstant);
    }
    
    /** Writes the function to given ClassVisitor. */
    void write (ClassVisitor writer, BitSet statements) {
        int access = Opcodes.ACC_PRIVATE | Opcodes.ACC_STATIC;
        if (synthetic) {
            access |= Opcodes.ACC_SYNTHETIC;
        }
        final MethodWriter function = new MethodWriter(access, method.method, writer, statements);
        write(function);
        function.endMethod();
    }

    @Override
    void write(MethodWriter function) {
        if (reserved.getMaxLoopCounter() > 0) {
            // if there is infinite loop protection, we do this once:
            // int #loop = settings.getMaxLoopCounter()

            Variable loop = locals.getVariable(null, FunctionReserved.LOOP);

            function.push(reserved.getMaxLoopCounter());
            function.visitVarInsn(Opcodes.ISTORE, loop.slot);
        }

        for (AStatement statement : statements) {
            statement.write(function);
        }

        if (!methodEscape) {
            if (rtnType.sort == Sort.VOID) {
                function.returnValue();
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }
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

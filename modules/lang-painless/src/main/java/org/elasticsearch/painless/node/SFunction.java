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
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Parameter;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;

import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptyList;

/**
 * Represents a user-defined function.
 */
public final class SFunction extends AStatement {

    private final String rtnTypeStr;
    public final String name;
    private final List<String> paramTypeStrs;
    private final List<String> paramNameStrs;
    private final List<AStatement> statements;
    public final boolean synthetic;

    private CompilerSettings settings;

    Class<?> returnType;
    List<Class<?>> typeParameters;
    MethodType methodType;

    org.objectweb.asm.commons.Method method;
    List<Parameter> parameters = new ArrayList<>();

    private Variable loop = null;

    public SFunction(Location location, String rtnType, String name,
                     List<String> paramTypes, List<String> paramNames, List<AStatement> statements,
                     boolean synthetic) {
        super(location);

        this.rtnTypeStr = Objects.requireNonNull(rtnType);
        this.name = Objects.requireNonNull(name);
        this.paramTypeStrs = Collections.unmodifiableList(paramTypes);
        this.paramNameStrs = Collections.unmodifiableList(paramNames);
        this.statements = Collections.unmodifiableList(statements);
        this.synthetic = synthetic;
    }

    @Override
    void storeSettings(CompilerSettings settings) {
        for (AStatement statement : statements) {
            statement.storeSettings(settings);
        }

        this.settings = settings;
    }

    @Override
    void extractVariables(Set<String> variables) {
        for (AStatement statement : statements) {
            // we reset the list for function scope
            // note this is not stored for this node
            // but still required for lambdas
            statement.extractVariables(new HashSet<>());
        }
    }

    void generateSignature(PainlessLookup painlessLookup) {
        returnType = painlessLookup.canonicalTypeNameToType(rtnTypeStr);

        if (returnType == null) {
            throw createError(new IllegalArgumentException("Illegal return type [" + rtnTypeStr + "] for function [" + name + "]."));
        }

        if (paramTypeStrs.size() != paramNameStrs.size()) {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }

        Class<?>[] paramClasses = new Class<?>[this.paramTypeStrs.size()];
        List<Class<?>> paramTypes = new ArrayList<>();

        for (int param = 0; param < this.paramTypeStrs.size(); ++param) {
            Class<?> paramType = painlessLookup.canonicalTypeNameToType(this.paramTypeStrs.get(param));

            if (paramType == null) {
                throw createError(new IllegalArgumentException(
                    "Illegal parameter type [" + this.paramTypeStrs.get(param) + "] for function [" + name + "]."));
            }

            paramClasses[param] = PainlessLookupUtility.typeToJavaType(paramType);
            paramTypes.add(paramType);
            parameters.add(new Parameter(location, paramNameStrs.get(param), paramType));
        }

        typeParameters = paramTypes;
        methodType = MethodType.methodType(PainlessLookupUtility.typeToJavaType(returnType), paramClasses);
        method = new org.objectweb.asm.commons.Method(name, MethodType.methodType(
                PainlessLookupUtility.typeToJavaType(returnType), paramClasses).toMethodDescriptorString());
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

        if (!methodEscape && returnType != void.class) {
            throw createError(new IllegalArgumentException("Not all paths provide a return value for method [" + name + "]."));
        }

        if (settings.getMaxLoopCounter() > 0) {
            loop = locals.getVariable(null, Locals.LOOP);
        }
    }

    /** Writes the function to given ClassVisitor. */
    void write(ClassVisitor writer, CompilerSettings settings, Globals globals) {
        int access = Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC;
        if (synthetic) {
            access |= Opcodes.ACC_SYNTHETIC;
        }
        final MethodWriter function = new MethodWriter(access, method, writer, globals.getStatements(), settings);
        function.visitCode();
        write(function, globals);
        function.endMethod();
    }

    @Override
    void write(MethodWriter function, Globals globals) {
        if (settings.getMaxLoopCounter() > 0) {
            // if there is infinite loop protection, we do this once:
            // int #loop = settings.getMaxLoopCounter()
            function.push(settings.getMaxLoopCounter());
            function.visitVarInsn(Opcodes.ISTORE, loop.getSlot());
        }

        for (AStatement statement : statements) {
            statement.write(function, globals);
        }

        if (!methodEscape) {
            if (returnType == void.class) {
                function.returnValue();
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }
    }

    @Override
    public String toString() {
        List<Object> description = new ArrayList<>();
        description.add(rtnTypeStr);
        description.add(name);
        if (false == (paramTypeStrs.isEmpty() && paramNameStrs.isEmpty())) {
            description.add(joinWithName("Args", pairwiseToString(paramTypeStrs, paramNameStrs), emptyList()));
        }
        return multilineToString(description, statements);
    }
}

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

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptRoot;
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.spi.annotation.NonDeterministicAnnotation;
import org.elasticsearch.painless.symbol.FunctionTable;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.painless.WriterConstants.CLASS_TYPE;

/**
 * Represents a user-defined call.
 */
public final class ECallLocal extends AExpression {

    private final String name;
    private final List<AExpression> arguments;

    private FunctionTable.LocalFunction localFunction = null;
    private PainlessMethod importedMethod = null;
    private PainlessClassBinding classBinding = null;
    private int classBindingOffset = 0;
    private PainlessInstanceBinding instanceBinding = null;
    private String bindingName = null;

    public ECallLocal(Location location, String name, List<AExpression> arguments) {
        super(location);

        this.name = Objects.requireNonNull(name);
        this.arguments = Objects.requireNonNull(arguments);
    }

    @Override
    void extractVariables(Set<String> variables) {
        for (AExpression argument : arguments) {
            argument.extractVariables(variables);
        }
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        localFunction = scriptRoot.getFunctionTable().getFunction(name, arguments.size());

        // user cannot call internal functions, reset to null if an internal function is found
        if (localFunction != null && localFunction.isInternal()) {
            localFunction = null;
        }

        if (localFunction == null) {
            importedMethod = scriptRoot.getPainlessLookup().lookupImportedPainlessMethod(name, arguments.size());

            if (importedMethod == null) {
                classBinding = scriptRoot.getPainlessLookup().lookupPainlessClassBinding(name, arguments.size());

                // check to see if this class binding requires an implicit this reference
                if (classBinding != null && classBinding.typeParameters.isEmpty() == false &&
                        classBinding.typeParameters.get(0) == scriptRoot.getScriptClassInfo().getBaseClass()) {
                    classBinding = null;
                }

                if (classBinding == null) {
                    // This extra check looks for a possible match where the class binding requires an implicit this
                    // reference.  This is a temporary solution to allow the class binding access to data from the
                    // base script class without need for a user to add additional arguments.  A long term solution
                    // will likely involve adding a class instance binding where any instance can have a class binding
                    // as part of its API.  However, the situation at run-time is difficult and will modifications that
                    // are a substantial change if even possible to do.
                    classBinding = scriptRoot.getPainlessLookup().lookupPainlessClassBinding(name, arguments.size() + 1);

                    if (classBinding != null) {
                        if (classBinding.typeParameters.isEmpty() == false &&
                                classBinding.typeParameters.get(0) == scriptRoot.getScriptClassInfo().getBaseClass()) {
                            classBindingOffset = 1;
                        } else {
                            classBinding = null;
                        }
                    }

                    if (classBinding == null) {
                        instanceBinding = scriptRoot.getPainlessLookup().lookupPainlessInstanceBinding(name, arguments.size());

                        if (instanceBinding == null) {
                            throw createError(new IllegalArgumentException(
                                    "Unknown call [" + name + "] with [" + arguments.size() + "] arguments."));
                        }
                    }
                }
            }
        }

        List<Class<?>> typeParameters;

        if (localFunction != null) {
            typeParameters = new ArrayList<>(localFunction.getTypeParameters());
            actual = localFunction.getReturnType();
        } else if (importedMethod != null) {
            scriptRoot.markNonDeterministic(importedMethod.annotations.containsKey(NonDeterministicAnnotation.class));
            typeParameters = new ArrayList<>(importedMethod.typeParameters);
            actual = importedMethod.returnType;
        } else if (classBinding != null) {
            scriptRoot.markNonDeterministic(classBinding.annotations.containsKey(NonDeterministicAnnotation.class));
            typeParameters = new ArrayList<>(classBinding.typeParameters);
            actual = classBinding.returnType;
            bindingName = scriptRoot.getNextSyntheticName("class_binding");
            scriptRoot.getClassNode().addField(new SField(location,
                    Modifier.PRIVATE, bindingName, classBinding.javaConstructor.getDeclaringClass(), null));
        } else if (instanceBinding != null) {
            typeParameters = new ArrayList<>(instanceBinding.typeParameters);
            actual = instanceBinding.returnType;
            bindingName = scriptRoot.getNextSyntheticName("instance_binding");
            scriptRoot.getClassNode().addField(new SField(location, Modifier.STATIC | Modifier.PUBLIC,
                    bindingName, instanceBinding.targetInstance.getClass(), instanceBinding.targetInstance));
        } else {
            throw new IllegalStateException("Illegal tree structure.");
        }

        // if the class binding is using an implicit this reference then the arguments counted must
        // be incremented by 1 as the this reference will not be part of the arguments passed into
        // the class binding call
        for (int argument = 0; argument < arguments.size(); ++argument) {
            AExpression expression = arguments.get(argument);

            expression.expected = typeParameters.get(argument + classBindingOffset);
            expression.internal = true;
            expression.analyze(scriptRoot, locals);
            arguments.set(argument, expression.cast(scriptRoot, locals));
        }

        statement = true;
    }

    @Override
    void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        if (localFunction != null) {
            for (AExpression argument : arguments) {
                argument.write(classWriter, methodWriter, globals);
            }

            methodWriter.invokeStatic(CLASS_TYPE, localFunction.getAsmMethod());
        } else if (importedMethod != null) {
            for (AExpression argument : arguments) {
                argument.write(classWriter, methodWriter, globals);
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
                arguments.get(argument).write(classWriter, methodWriter, globals);
            }

            methodWriter.invokeConstructor(type, Method.getMethod(classBinding.javaConstructor));
            methodWriter.putField(CLASS_TYPE, bindingName, type);

            methodWriter.mark(nonNull);
            methodWriter.loadThis();
            methodWriter.getField(CLASS_TYPE, bindingName, type);

            for (int argument = 0; argument < classBinding.javaMethod.getParameterCount(); ++argument) {
                arguments.get(argument + javaConstructorParameterCount).write(classWriter, methodWriter, globals);
            }

            methodWriter.invokeVirtual(type, Method.getMethod(classBinding.javaMethod));
        } else if (instanceBinding != null) {
            Type type = Type.getType(instanceBinding.targetInstance.getClass());

            methodWriter.loadThis();
            methodWriter.getStatic(CLASS_TYPE, bindingName, type);

            for (int argument = 0; argument < instanceBinding.javaMethod.getParameterCount(); ++argument) {
                arguments.get(argument).write(classWriter, methodWriter, globals);
            }

            methodWriter.invokeVirtual(type, Method.getMethod(instanceBinding.javaMethod));
        } else {
            throw new IllegalStateException("Illegal tree structure.");
        }
    }

    @Override
    public String toString() {
        return singleLineToStringWithOptionalArgs(arguments, name);
    }
}

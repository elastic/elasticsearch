/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation.impl;

import org.elasticsearch.entitlement.instrumentation.CheckMethod;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.instrumentation.Instrumenter;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class InstrumentationServiceImpl implements InstrumentationService {

    @Override
    public Instrumenter newInstrumenter(Class<?> clazz, Map<MethodKey, CheckMethod> methods) {
        return InstrumenterImpl.create(clazz, methods);
    }

    @Override
    public Map<MethodKey, CheckMethod> lookupMethods(Class<?> checkerClass) throws IOException {
        var methodsToInstrument = new HashMap<MethodKey, CheckMethod>();
        var classFileInfo = InstrumenterImpl.getClassFileInfo(checkerClass);
        ClassReader reader = new ClassReader(classFileInfo.bytecodes());
        ClassVisitor visitor = new ClassVisitor(Opcodes.ASM9) {
            @Override
            public MethodVisitor visitMethod(
                int access,
                String checkerMethodName,
                String checkerMethodDescriptor,
                String signature,
                String[] exceptions
            ) {
                var mv = super.visitMethod(access, checkerMethodName, checkerMethodDescriptor, signature, exceptions);

                var checkerMethodArgumentTypes = Type.getArgumentTypes(checkerMethodDescriptor);
                var methodToInstrument = parseCheckerMethodSignature(checkerMethodName, checkerMethodArgumentTypes);

                var checkerParameterDescriptors = Arrays.stream(checkerMethodArgumentTypes).map(Type::getDescriptor).toList();
                var checkMethod = new CheckMethod(Type.getInternalName(checkerClass), checkerMethodName, checkerParameterDescriptors);

                methodsToInstrument.put(methodToInstrument, checkMethod);

                return mv;
            }
        };
        reader.accept(visitor, 0);
        return methodsToInstrument;
    }

    private static final Type CLASS_TYPE = Type.getType(Class.class);

    static MethodKey parseCheckerMethodSignature(String checkerMethodName, Type[] checkerMethodArgumentTypes) {
        var classNameStartIndex = checkerMethodName.indexOf('$');
        var classNameEndIndex = checkerMethodName.lastIndexOf('$');

        if (classNameStartIndex == -1 || classNameStartIndex >= classNameEndIndex) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Checker method %s has incorrect name format. "
                        + "It should be either check$$methodName (instance), check$package_ClassName$methodName (static) or "
                        + "check$package_ClassName$ (ctor)",
                    checkerMethodName
                )
            );
        }

        // No "className" (check$$methodName) -> method is instance, and we'll get the class from the actual typed argument
        final boolean targetMethodIsStatic = classNameStartIndex + 1 != classNameEndIndex;
        // No "methodName" (check$package_ClassName$) -> method is ctor
        final boolean targetMethodIsCtor = classNameEndIndex + 1 == checkerMethodName.length();
        final String targetMethodName = targetMethodIsCtor ? "<init>" : checkerMethodName.substring(classNameEndIndex + 1);

        final String targetClassName;
        final List<String> targetParameterTypes;
        if (targetMethodIsStatic) {
            if (checkerMethodArgumentTypes.length < 1 || CLASS_TYPE.equals(checkerMethodArgumentTypes[0]) == false) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Checker method %s has incorrect argument types. " + "It must have a first argument of Class<?> type.",
                        checkerMethodName
                    )
                );
            }

            targetClassName = checkerMethodName.substring(classNameStartIndex + 1, classNameEndIndex).replace('_', '/');
            targetParameterTypes = Arrays.stream(checkerMethodArgumentTypes).skip(1).map(Type::getInternalName).toList();
        } else {
            if (checkerMethodArgumentTypes.length < 2
                || CLASS_TYPE.equals(checkerMethodArgumentTypes[0]) == false
                || checkerMethodArgumentTypes[1].getSort() != Type.OBJECT) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Checker method %s has incorrect argument types. "
                            + "It must have a first argument of Class<?> type, and a second argument of the class containing the method to "
                            + "instrument",
                        checkerMethodName
                    )
                );
            }
            var targetClassType = checkerMethodArgumentTypes[1];
            targetClassName = targetClassType.getInternalName();
            targetParameterTypes = Arrays.stream(checkerMethodArgumentTypes).skip(2).map(Type::getInternalName).toList();
        }
        return new MethodKey(targetClassName, targetMethodName, targetParameterTypes);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation.impl;

import org.elasticsearch.core.SuppressForbidden;
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
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InstrumentationServiceImpl implements InstrumentationService {

    private static final String OBJECT_INTERNAL_NAME = Type.getInternalName(Object.class);

    @Override
    public Instrumenter newInstrumenter(Class<?> clazz, Map<MethodKey, CheckMethod> methods) {
        return InstrumenterImpl.create(clazz, methods);
    }

    private interface CheckerMethodVisitor {
        void visit(Class<?> currentClass, int access, String checkerMethodName, String checkerMethodDescriptor);
    }

    private void visitClassAndSupers(Class<?> checkerClass, CheckerMethodVisitor checkerMethodVisitor) throws ClassNotFoundException {
        Set<Class<?>> visitedClasses = new HashSet<>();
        ArrayDeque<Class<?>> classesToVisit = new ArrayDeque<>(Collections.singleton(checkerClass));
        while (classesToVisit.isEmpty() == false) {
            var currentClass = classesToVisit.remove();
            if (visitedClasses.contains(currentClass)) {
                continue;
            }
            visitedClasses.add(currentClass);

            try {
                var classFileInfo = InstrumenterImpl.getClassFileInfo(currentClass);
                ClassReader reader = new ClassReader(classFileInfo.bytecodes());
                ClassVisitor visitor = new ClassVisitor(Opcodes.ASM9) {

                    @Override
                    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
                        super.visit(version, access, name, signature, superName, interfaces);
                        try {
                            if (OBJECT_INTERNAL_NAME.equals(superName) == false) {
                                classesToVisit.add(Class.forName(Type.getObjectType(superName).getClassName()));
                            }
                            for (var interfaceName : interfaces) {
                                classesToVisit.add(Class.forName(Type.getObjectType(interfaceName).getClassName()));
                            }
                        } catch (ClassNotFoundException e) {
                            throw new IllegalArgumentException("Cannot inspect checker class " + currentClass.getName(), e);
                        }
                    }

                    @Override
                    public MethodVisitor visitMethod(
                        int access,
                        String checkerMethodName,
                        String checkerMethodDescriptor,
                        String signature,
                        String[] exceptions
                    ) {
                        var mv = super.visitMethod(access, checkerMethodName, checkerMethodDescriptor, signature, exceptions);
                        checkerMethodVisitor.visit(currentClass, access, checkerMethodName, checkerMethodDescriptor);
                        return mv;
                    }
                };
                reader.accept(visitor, 0);
            } catch (IOException e) {
                throw new ClassNotFoundException("Cannot find a definition for class [" + checkerClass.getName() + "]", e);
            }
        }
    }

    @Override
    public Map<MethodKey, CheckMethod> lookupMethods(Class<?> checkerClass) throws ClassNotFoundException {
        Map<MethodKey, CheckMethod> methodsToInstrument = new HashMap<>();

        visitClassAndSupers(checkerClass, (currentClass, access, checkerMethodName, checkerMethodDescriptor) -> {
            if (checkerMethodName.startsWith(InstrumentationService.CHECK_METHOD_PREFIX)) {
                var checkerMethodArgumentTypes = Type.getArgumentTypes(checkerMethodDescriptor);
                var methodToInstrument = parseCheckerMethodSignature(checkerMethodName, checkerMethodArgumentTypes);

                var checkerParameterDescriptors = Arrays.stream(checkerMethodArgumentTypes).map(Type::getDescriptor).toList();
                var checkMethod = new CheckMethod(Type.getInternalName(currentClass), checkerMethodName, checkerParameterDescriptors);
                methodsToInstrument.putIfAbsent(methodToInstrument, checkMethod);
            }
        });

        return methodsToInstrument;
    }

    @SuppressForbidden(reason = "Need access to abstract methods (protected/package internal) in base class")
    @Override
    public InstrumentationInfo lookupImplementationMethod(
        Class<?> targetSuperclass,
        String targetMethodName,
        Class<?> implementationClass,
        Class<?> checkerClass,
        String checkMethodName,
        Class<?>... parameterTypes
    ) throws NoSuchMethodException, ClassNotFoundException {

        var targetMethod = targetSuperclass.getDeclaredMethod(targetMethodName, parameterTypes);
        var implementationMethod = implementationClass.getMethod(targetMethod.getName(), targetMethod.getParameterTypes());
        validateTargetMethod(implementationClass, targetMethod, implementationMethod);

        var checkerAdditionalArguments = Stream.of(Class.class, targetSuperclass);
        var checkMethodArgumentTypes = Stream.concat(checkerAdditionalArguments, Arrays.stream(parameterTypes))
            .map(Type::getType)
            .toArray(Type[]::new);

        CheckMethod[] checkMethod = new CheckMethod[1];

        visitClassAndSupers(checkerClass, (currentClass, access, methodName, methodDescriptor) -> {
            if (methodName.equals(checkMethodName)) {
                var methodArgumentTypes = Type.getArgumentTypes(methodDescriptor);
                if (Arrays.equals(methodArgumentTypes, checkMethodArgumentTypes)) {
                    var checkerParameterDescriptors = Arrays.stream(methodArgumentTypes).map(Type::getDescriptor).toList();
                    checkMethod[0] = new CheckMethod(Type.getInternalName(currentClass), methodName, checkerParameterDescriptors);
                }
            }
        });

        if (checkMethod[0] == null) {
            throw new NoSuchMethodException(
                String.format(
                    Locale.ROOT,
                    "Cannot find a method with name [%s] and arguments [%s] in class [%s]",
                    checkMethodName,
                    Arrays.stream(checkMethodArgumentTypes).map(Type::toString).collect(Collectors.joining()),
                    checkerClass.getName()
                )
            );
        }

        return new InstrumentationInfo(
            new MethodKey(
                Type.getInternalName(implementationMethod.getDeclaringClass()),
                implementationMethod.getName(),
                Arrays.stream(parameterTypes).map(c -> Type.getType(c).getInternalName()).toList()
            ),
            checkMethod[0]
        );
    }

    private static void validateTargetMethod(Class<?> implementationClass, Method targetMethod, Method implementationMethod) {
        if (targetMethod.getDeclaringClass().isAssignableFrom(implementationClass) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Not an implementation class for %s: %s does not implement %s",
                    targetMethod.getName(),
                    implementationClass.getName(),
                    targetMethod.getDeclaringClass().getName()
                )
            );
        }
        if (Modifier.isPrivate(targetMethod.getModifiers())) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Not a valid instrumentation method: %s is private in %s",
                    targetMethod.getName(),
                    targetMethod.getDeclaringClass().getName()
                )
            );
        }
        if (Modifier.isStatic(targetMethod.getModifiers())) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Not a valid instrumentation method: %s is static in %s",
                    targetMethod.getName(),
                    targetMethod.getDeclaringClass().getName()
                )
            );
        }
        var methodModifiers = implementationMethod.getModifiers();
        if (Modifier.isAbstract(methodModifiers)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Not a valid instrumentation method: %s is abstract in %s",
                    targetMethod.getName(),
                    implementationClass.getName()
                )
            );
        }
        if (Modifier.isPublic(methodModifiers) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Not a valid instrumentation method: %s is not public in %s",
                    targetMethod.getName(),
                    implementationClass.getName()
                )
            );
        }
    }

    private static final Type CLASS_TYPE = Type.getType(Class.class);

    static ParsedCheckerMethod parseCheckerMethodName(String checkerMethodName) {
        boolean targetMethodIsStatic;
        int classNameEndIndex = checkerMethodName.lastIndexOf("$$");
        int methodNameStartIndex;
        if (classNameEndIndex == -1) {
            targetMethodIsStatic = false;
            classNameEndIndex = checkerMethodName.lastIndexOf('$');
            methodNameStartIndex = classNameEndIndex + 1;
        } else {
            targetMethodIsStatic = true;
            methodNameStartIndex = classNameEndIndex + 2;
        }

        var classNameStartIndex = checkerMethodName.indexOf('$');
        if (classNameStartIndex == -1 || classNameStartIndex >= classNameEndIndex) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Checker method %s has incorrect name format. "
                        + "It should be either check$package_ClassName$methodName (instance), check$package_ClassName$$methodName (static) "
                        + "or check$package_ClassName$ (ctor)",
                    checkerMethodName
                )
            );
        }

        // No "methodName" (check$package_ClassName$) -> method is ctor
        final boolean targetMethodIsCtor = classNameEndIndex + 1 == checkerMethodName.length();
        final String targetMethodName = targetMethodIsCtor ? "<init>" : checkerMethodName.substring(methodNameStartIndex);

        final String targetClassName = checkerMethodName.substring(classNameStartIndex + 1, classNameEndIndex).replace('_', '/');
        if (targetClassName.isBlank()) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Checker method %s has no class name", checkerMethodName));
        }
        return new ParsedCheckerMethod(targetClassName, targetMethodName, targetMethodIsStatic, targetMethodIsCtor);
    }

    static MethodKey parseCheckerMethodSignature(String checkerMethodName, Type[] checkerMethodArgumentTypes) {
        ParsedCheckerMethod checkerMethod = parseCheckerMethodName(checkerMethodName);

        final List<String> targetParameterTypes;
        if (checkerMethod.targetMethodIsStatic() || checkerMethod.targetMethodIsCtor()) {
            if (checkerMethodArgumentTypes.length < 1 || CLASS_TYPE.equals(checkerMethodArgumentTypes[0]) == false) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Checker method %s has incorrect argument types. " + "It must have a first argument of Class<?> type.",
                        checkerMethodName
                    )
                );
            }

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
            targetParameterTypes = Arrays.stream(checkerMethodArgumentTypes).skip(2).map(Type::getInternalName).toList();
        }
        return new MethodKey(checkerMethod.targetClassName(), checkerMethod.targetMethodName(), targetParameterTypes);
    }

    private record ParsedCheckerMethod(
        String targetClassName,
        String targetMethodName,
        boolean targetMethodIsStatic,
        boolean targetMethodIsCtor
    ) {}
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.lookup;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Strings;
import org.elasticsearch.painless.Def;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistClass;
import org.elasticsearch.painless.spi.WhitelistClassBinding;
import org.elasticsearch.painless.spi.WhitelistConstructor;
import org.elasticsearch.painless.spi.WhitelistField;
import org.elasticsearch.painless.spi.WhitelistInstanceBinding;
import org.elasticsearch.painless.spi.WhitelistMethod;
import org.elasticsearch.painless.spi.annotation.AliasAnnotation;
import org.elasticsearch.painless.spi.annotation.AugmentedAnnotation;
import org.elasticsearch.painless.spi.annotation.CompileTimeOnlyAnnotation;
import org.elasticsearch.painless.spi.annotation.InjectConstantAnnotation;
import org.elasticsearch.painless.spi.annotation.NoImportAnnotation;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.DEF_CLASS_NAME;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.buildPainlessConstructorKey;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.buildPainlessFieldKey;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.buildPainlessMethodKey;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToJavaType;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typesToCanonicalTypeNames;

public final class PainlessLookupBuilder {

    private static final Map<PainlessConstructor, PainlessConstructor> painlessConstructorCache = new HashMap<>();
    private static final Map<PainlessMethod, PainlessMethod> painlessMethodCache = new HashMap<>();
    private static final Map<PainlessField, PainlessField> painlessFieldCache = new HashMap<>();
    private static final Map<PainlessClassBinding, PainlessClassBinding> painlessClassBindingCache = new HashMap<>();
    private static final Map<PainlessInstanceBinding, PainlessInstanceBinding> painlessInstanceBindingCache = new HashMap<>();
    private static final Map<PainlessMethod, PainlessMethod> painlessFilteredCache = new HashMap<>();

    private static final Pattern CLASS_NAME_PATTERN = Pattern.compile("^[_a-zA-Z][._a-zA-Z0-9]*$");
    private static final Pattern METHOD_NAME_PATTERN = Pattern.compile("^[_a-zA-Z][_a-zA-Z0-9]*$");
    private static final Pattern FIELD_NAME_PATTERN = Pattern.compile("^[_a-zA-Z][_a-zA-Z0-9]*$");

    public static PainlessLookup buildFromWhitelists(List<Whitelist> whitelists) {
        PainlessLookupBuilder painlessLookupBuilder = new PainlessLookupBuilder();
        String origin = "internal error";

        try {
            for (Whitelist whitelist : whitelists) {
                for (WhitelistClass whitelistClass : whitelist.whitelistClasses) {
                    origin = whitelistClass.origin;
                    painlessLookupBuilder.addPainlessClass(
                        whitelist.classLoader,
                        whitelistClass.javaClassName,
                        whitelistClass.painlessAnnotations
                    );
                }
            }

            for (Whitelist whitelist : whitelists) {
                for (WhitelistClass whitelistClass : whitelist.whitelistClasses) {
                    String targetCanonicalClassName = whitelistClass.javaClassName.replace('$', '.');

                    for (WhitelistConstructor whitelistConstructor : whitelistClass.whitelistConstructors) {
                        origin = whitelistConstructor.origin;
                        painlessLookupBuilder.addPainlessConstructor(
                            targetCanonicalClassName,
                            whitelistConstructor.canonicalTypeNameParameters,
                            whitelistConstructor.painlessAnnotations
                        );
                    }

                    for (WhitelistMethod whitelistMethod : whitelistClass.whitelistMethods) {
                        origin = whitelistMethod.origin;
                        painlessLookupBuilder.addPainlessMethod(
                            whitelist.classLoader,
                            targetCanonicalClassName,
                            whitelistMethod.augmentedCanonicalClassName,
                            whitelistMethod.methodName,
                            whitelistMethod.returnCanonicalTypeName,
                            whitelistMethod.canonicalTypeNameParameters,
                            whitelistMethod.painlessAnnotations
                        );
                    }

                    for (WhitelistField whitelistField : whitelistClass.whitelistFields) {
                        origin = whitelistField.origin;
                        painlessLookupBuilder.addPainlessField(
                            whitelist.classLoader,
                            targetCanonicalClassName,
                            whitelistField.fieldName,
                            whitelistField.canonicalTypeNameParameter,
                            whitelistField.painlessAnnotations
                        );
                    }
                }

                for (WhitelistMethod whitelistStatic : whitelist.whitelistImportedMethods) {
                    origin = whitelistStatic.origin;
                    painlessLookupBuilder.addImportedPainlessMethod(
                        whitelist.classLoader,
                        whitelistStatic.augmentedCanonicalClassName,
                        whitelistStatic.methodName,
                        whitelistStatic.returnCanonicalTypeName,
                        whitelistStatic.canonicalTypeNameParameters,
                        whitelistStatic.painlessAnnotations
                    );
                }

                for (WhitelistClassBinding whitelistClassBinding : whitelist.whitelistClassBindings) {
                    origin = whitelistClassBinding.origin;
                    painlessLookupBuilder.addPainlessClassBinding(
                        whitelist.classLoader,
                        whitelistClassBinding.targetJavaClassName,
                        whitelistClassBinding.methodName,
                        whitelistClassBinding.returnCanonicalTypeName,
                        whitelistClassBinding.canonicalTypeNameParameters,
                        whitelistClassBinding.painlessAnnotations
                    );
                }

                for (WhitelistInstanceBinding whitelistInstanceBinding : whitelist.whitelistInstanceBindings) {
                    origin = whitelistInstanceBinding.origin;
                    painlessLookupBuilder.addPainlessInstanceBinding(
                        whitelistInstanceBinding.targetInstance,
                        whitelistInstanceBinding.methodName,
                        whitelistInstanceBinding.returnCanonicalTypeName,
                        whitelistInstanceBinding.canonicalTypeNameParameters,
                        whitelistInstanceBinding.painlessAnnotations
                    );
                }
            }
        } catch (Exception exception) {
            throw new IllegalArgumentException("error loading whitelist(s) " + origin, exception);
        }

        return painlessLookupBuilder.build();
    }

    // javaClassNamesToClasses is all the classes that need to be available to the custom classloader
    // including classes used as part of imported methods and class bindings but not necessarily whitelisted
    // individually. The values of javaClassNamesToClasses are a superset of the values of
    // canonicalClassNamesToClasses.
    private final Map<String, Class<?>> javaClassNamesToClasses;
    // canonicalClassNamesToClasses is all the whitelisted classes available in a Painless script including
    // classes with imported canonical names but does not include classes from imported methods or class
    // bindings unless also whitelisted separately. The values of canonicalClassNamesToClasses are a subset
    // of the values of javaClassNamesToClasses.
    private final Map<String, Class<?>> canonicalClassNamesToClasses;
    private final Map<Class<?>, PainlessClassBuilder> classesToPainlessClassBuilders;
    private final Map<Class<?>, Set<Class<?>>> classesToDirectSubClasses;

    private final Map<String, PainlessMethod> painlessMethodKeysToImportedPainlessMethods;
    private final Map<String, PainlessClassBinding> painlessMethodKeysToPainlessClassBindings;
    private final Map<String, PainlessInstanceBinding> painlessMethodKeysToPainlessInstanceBindings;

    public PainlessLookupBuilder() {
        javaClassNamesToClasses = new HashMap<>();
        canonicalClassNamesToClasses = new HashMap<>();
        classesToPainlessClassBuilders = new HashMap<>();
        classesToDirectSubClasses = new HashMap<>();

        painlessMethodKeysToImportedPainlessMethods = new HashMap<>();
        painlessMethodKeysToPainlessClassBindings = new HashMap<>();
        painlessMethodKeysToPainlessInstanceBindings = new HashMap<>();
    }

    private Class<?> canonicalTypeNameToType(String canonicalTypeName) {
        return PainlessLookupUtility.canonicalTypeNameToType(canonicalTypeName, canonicalClassNamesToClasses);
    }

    private boolean isValidType(Class<?> type) {
        while (type.getComponentType() != null) {
            type = type.getComponentType();
        }

        return type == def.class || classesToPainlessClassBuilders.containsKey(type);
    }

    private Class<?> loadClass(ClassLoader classLoader, String javaClassName, Supplier<String> errorMessage) {
        try {
            return Class.forName(javaClassName, true, classLoader);
        } catch (ClassNotFoundException cnfe) {
            try {
                // Painless provides some api classes that are available only through the painless implementation.
                return Class.forName(javaClassName);
            } catch (ClassNotFoundException cnfe2) {
                IllegalArgumentException iae = new IllegalArgumentException(errorMessage.get(), cnfe2);
                cnfe2.addSuppressed(cnfe);
                throw iae;
            }
        }
    }

    /**
     * Returns a lookup with the capability of looking up members in the target
     * class.
     *
     * <p> If the target class is in the same module as this module, then the
     * returned lookup has this class as its lookup class and holds the
     * {@link Lookup#MODULE} mode. If the target class is not in this module,
     * then the returned lookup has the target class as its lookup class and
     * holds the {@link Lookup#UNCONDITIONAL} mode.
     */
    private static Lookup lookup(Class<?> targetClass) {
        if (targetClass.getModule() == PainlessLookupBuilder.class.getModule()) {
            var l = MethodHandles.lookup().dropLookupMode(Lookup.PACKAGE);
            assert l.lookupModes() == (Lookup.PUBLIC | Lookup.MODULE) : "lookup modes:" + Integer.toHexString(l.lookupModes());
            return l;
        } else {
            return MethodHandles.publicLookup().in(targetClass);
        }
    }

    public void addPainlessClass(ClassLoader classLoader, String javaClassName, Map<Class<?>, Object> annotations) {

        Objects.requireNonNull(classLoader);
        Objects.requireNonNull(javaClassName);

        Class<?> clazz;

        if ("void".equals(javaClassName)) clazz = void.class;
        else if ("boolean".equals(javaClassName)) clazz = boolean.class;
        else if ("byte".equals(javaClassName)) clazz = byte.class;
        else if ("short".equals(javaClassName)) clazz = short.class;
        else if ("char".equals(javaClassName)) clazz = char.class;
        else if ("int".equals(javaClassName)) clazz = int.class;
        else if ("long".equals(javaClassName)) clazz = long.class;
        else if ("float".equals(javaClassName)) clazz = float.class;
        else if ("double".equals(javaClassName)) clazz = double.class;
        else {
            clazz = loadClass(classLoader, javaClassName, () -> "class [" + javaClassName + "] not found");
        }

        addPainlessClass(clazz, annotations);
    }

    private static IllegalArgumentException lookupException(String formatText, Object... args) {
        return new IllegalArgumentException(Strings.format(formatText, args));
    }

    private static IllegalArgumentException lookupException(Throwable cause, String formatText, Object... args) {
        return new IllegalArgumentException(Strings.format(formatText, args), cause);
    }

    public void addPainlessClass(Class<?> clazz, Map<Class<?>, Object> annotations) {
        Objects.requireNonNull(clazz);
        Objects.requireNonNull(annotations);

        if (clazz == def.class) {
            throw new IllegalArgumentException("cannot add reserved class [" + DEF_CLASS_NAME + "]");
        }

        String canonicalClassName = typeToCanonicalTypeName(clazz);

        if (clazz.isArray()) {
            throw new IllegalArgumentException("cannot add array type [" + canonicalClassName + "] as a class");
        }

        if (CLASS_NAME_PATTERN.matcher(canonicalClassName).matches() == false) {
            throw new IllegalArgumentException("invalid class name [" + canonicalClassName + "]");
        }

        Class<?> existingClass = javaClassNamesToClasses.get(clazz.getName());

        if (existingClass == null) {
            javaClassNamesToClasses.put(clazz.getName().intern(), clazz);
        } else if (existingClass != clazz) {
            throw lookupException(
                "class [%s] cannot represent multiple java classes with the same name from different class loaders",
                canonicalClassName
            );
        }

        existingClass = canonicalClassNamesToClasses.get(canonicalClassName);

        if (existingClass != null && existingClass != clazz) {
            throw lookupException(
                "class [%s] cannot represent multiple java classes with the same name from different class loaders",
                canonicalClassName
            );
        }

        PainlessClassBuilder existingPainlessClassBuilder = classesToPainlessClassBuilders.get(clazz);

        if (existingPainlessClassBuilder == null) {
            PainlessClassBuilder painlessClassBuilder = new PainlessClassBuilder();
            painlessClassBuilder.annotations.putAll(annotations);

            canonicalClassNamesToClasses.put(canonicalClassName.intern(), clazz);
            classesToPainlessClassBuilders.put(clazz, painlessClassBuilder);
        }

        String javaClassName = clazz.getName();
        String importedCanonicalClassName = javaClassName.substring(javaClassName.lastIndexOf('.') + 1).replace('$', '.');
        boolean importClassName = annotations.containsKey(NoImportAnnotation.class) == false;

        if (canonicalClassName.equals(importedCanonicalClassName)) {
            if (importClassName) {
                throw new IllegalArgumentException("must use no_import parameter on class [" + canonicalClassName + "] with no package");
            }
        } else {
            Class<?> importedClass = canonicalClassNamesToClasses.get(importedCanonicalClassName);

            if (importedClass == null) {
                if (importClassName) {
                    if (existingPainlessClassBuilder != null) {
                        throw new IllegalArgumentException("inconsistent no_import parameter found for class [" + canonicalClassName + "]");
                    }

                    canonicalClassNamesToClasses.put(importedCanonicalClassName.intern(), clazz);
                    if (annotations.get(AliasAnnotation.class) instanceof AliasAnnotation alias) {
                        Class<?> existing = canonicalClassNamesToClasses.put(alias.alias(), clazz);
                        if (existing != null) {
                            throw lookupException("Cannot add alias [%s] for [%s] that shadows class [%s]", alias.alias(), clazz, existing);
                        }
                    }
                }
            } else if (importedClass != clazz) {
                throw lookupException(
                    "imported class [%s] cannot represent multiple classes [%s] and [%s]",
                    importedCanonicalClassName,
                    canonicalClassName,
                    typeToCanonicalTypeName(importedClass)
                );
            } else if (importClassName == false) {
                throw new IllegalArgumentException("inconsistent no_import parameter found for class [" + canonicalClassName + "]");
            }
        }
    }

    public void addPainlessConstructor(
        String targetCanonicalClassName,
        List<String> canonicalTypeNameParameters,
        Map<Class<?>, Object> annotations
    ) {
        Objects.requireNonNull(targetCanonicalClassName);
        Objects.requireNonNull(canonicalTypeNameParameters);

        Class<?> targetClass = canonicalClassNamesToClasses.get(targetCanonicalClassName);

        if (targetClass == null) {
            throw lookupException(
                "target class [%s] not found for constructor [[%s], %s]",
                targetCanonicalClassName,
                targetCanonicalClassName,
                canonicalTypeNameParameters
            );
        }

        List<Class<?>> typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());

        for (String canonicalTypeNameParameter : canonicalTypeNameParameters) {
            Class<?> typeParameter = canonicalTypeNameToType(canonicalTypeNameParameter);

            if (typeParameter == null) {
                throw lookupException(
                    "type parameter [%s] not found for constructor [[%s], %s]",
                    canonicalTypeNameParameter,
                    targetCanonicalClassName,
                    canonicalTypeNameParameters
                );
            }

            typeParameters.add(typeParameter);
        }

        addPainlessConstructor(targetClass, typeParameters, annotations);
    }

    public void addPainlessConstructor(Class<?> targetClass, List<Class<?>> typeParameters, Map<Class<?>, Object> annotations) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(typeParameters);

        if (targetClass == def.class) {
            throw new IllegalArgumentException("cannot add constructor to reserved class [" + DEF_CLASS_NAME + "]");
        }

        String targetCanonicalClassName = targetClass.getCanonicalName();
        PainlessClassBuilder painlessClassBuilder = classesToPainlessClassBuilders.get(targetClass);

        if (painlessClassBuilder == null) {
            throw lookupException(
                "target class [%s] not found for constructor [[%s], %s]",
                targetCanonicalClassName,
                targetCanonicalClassName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        int typeParametersSize = typeParameters.size();
        List<Class<?>> javaTypeParameters = new ArrayList<>(typeParametersSize);

        for (Class<?> typeParameter : typeParameters) {
            if (isValidType(typeParameter) == false) {
                throw lookupException(
                    "type parameter [%s] not found for constructor [[%s], %s]",
                    typeToCanonicalTypeName(typeParameter),
                    targetCanonicalClassName,
                    typesToCanonicalTypeNames(typeParameters)
                );
            }

            javaTypeParameters.add(typeToJavaType(typeParameter));
        }

        Constructor<?> javaConstructor;

        try {
            javaConstructor = targetClass.getConstructor(javaTypeParameters.toArray(Class<?>[]::new));
        } catch (NoSuchMethodException nsme) {
            throw lookupException(
                nsme,
                "reflection object not found for constructor [[%s], %s]",
                targetCanonicalClassName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        MethodHandle methodHandle;

        try {
            methodHandle = lookup(targetClass).unreflectConstructor(javaConstructor);
        } catch (IllegalAccessException iae) {
            throw lookupException(
                iae,
                "method handle not found for constructor [[%s], %s]",
                targetCanonicalClassName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        if (annotations.containsKey(CompileTimeOnlyAnnotation.class)) {
            throw new IllegalArgumentException("constructors can't have @" + CompileTimeOnlyAnnotation.NAME);
        }

        MethodType methodType = methodHandle.type();

        String painlessConstructorKey = buildPainlessConstructorKey(typeParametersSize);
        PainlessConstructor existingPainlessConstructor = painlessClassBuilder.constructors.get(painlessConstructorKey);
        PainlessConstructor newPainlessConstructor = new PainlessConstructor(
            javaConstructor,
            typeParameters,
            methodHandle,
            methodType,
            annotations
        );

        if (existingPainlessConstructor == null) {
            newPainlessConstructor = painlessConstructorCache.computeIfAbsent(newPainlessConstructor, Function.identity());
            painlessClassBuilder.constructors.put(painlessConstructorKey.intern(), newPainlessConstructor);
        } else if (newPainlessConstructor.equals(existingPainlessConstructor) == false) {
            throw lookupException(
                "cannot add constructors with the same arity but are not equivalent for constructors [[%s], %s] and [[%s], %s]",
                targetCanonicalClassName,
                typesToCanonicalTypeNames(typeParameters),
                targetCanonicalClassName,
                typesToCanonicalTypeNames(existingPainlessConstructor.typeParameters())
            );
        }
    }

    public void addPainlessMethod(
        ClassLoader classLoader,
        String targetCanonicalClassName,
        String augmentedCanonicalClassName,
        String methodName,
        String returnCanonicalTypeName,
        List<String> canonicalTypeNameParameters,
        Map<Class<?>, Object> annotations
    ) {

        Objects.requireNonNull(classLoader);
        Objects.requireNonNull(targetCanonicalClassName);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnCanonicalTypeName);
        Objects.requireNonNull(canonicalTypeNameParameters);
        Objects.requireNonNull(annotations);

        Class<?> targetClass = canonicalClassNamesToClasses.get(targetCanonicalClassName);

        if (targetClass == null) {
            throw lookupException(
                "target class [%s] not found for method [[%s], [%s], %s]",
                targetCanonicalClassName,
                targetCanonicalClassName,
                methodName,
                canonicalTypeNameParameters
            );
        }

        Class<?> augmentedClass = null;

        if (augmentedCanonicalClassName != null) {
            augmentedClass = loadClass(
                classLoader,
                augmentedCanonicalClassName,
                () -> Strings.format(
                    "augmented class [%s] not found for method [[%s], [%s], %s]",
                    augmentedCanonicalClassName,
                    targetCanonicalClassName,
                    methodName,
                    canonicalTypeNameParameters
                )
            );
        }

        List<Class<?>> typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());

        for (String canonicalTypeNameParameter : canonicalTypeNameParameters) {
            Class<?> typeParameter = canonicalTypeNameToType(canonicalTypeNameParameter);

            if (typeParameter == null) {
                throw lookupException(
                    "type parameter [%s] not found for method [[%s], [%s], %s]",
                    canonicalTypeNameParameter,
                    targetCanonicalClassName,
                    methodName,
                    canonicalTypeNameParameters
                );
            }

            typeParameters.add(typeParameter);
        }

        Class<?> returnType = canonicalTypeNameToType(returnCanonicalTypeName);

        if (returnType == null) {
            throw lookupException(
                "return type [%s] not found for method [[%s], [%s], %s]",
                returnCanonicalTypeName,
                targetCanonicalClassName,
                methodName,
                canonicalTypeNameParameters
            );
        }

        addPainlessMethod(targetClass, augmentedClass, methodName, returnType, typeParameters, annotations);
    }

    public void addPainlessMethod(
        Class<?> targetClass,
        Class<?> augmentedClass,
        String methodName,
        Class<?> returnType,
        List<Class<?>> typeParameters,
        Map<Class<?>, Object> annotations
    ) {

        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnType);
        Objects.requireNonNull(typeParameters);
        Objects.requireNonNull(annotations);

        if (targetClass == def.class) {
            throw new IllegalArgumentException("cannot add method to reserved class [" + DEF_CLASS_NAME + "]");
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);

        if (METHOD_NAME_PATTERN.matcher(methodName).matches() == false) {
            throw new IllegalArgumentException(
                "invalid method name [" + methodName + "] for target class [" + targetCanonicalClassName + "]."
            );
        }

        PainlessClassBuilder painlessClassBuilder = classesToPainlessClassBuilders.get(targetClass);

        if (painlessClassBuilder == null) {
            throw lookupException(
                "target class [%s] not found for method [[%s], [%s], %s]",
                targetCanonicalClassName,
                targetCanonicalClassName,
                methodName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        int typeParametersSize = typeParameters.size();
        int augmentedParameterOffset = augmentedClass == null ? 0 : 1;
        List<Class<?>> javaTypeParameters = new ArrayList<>(typeParametersSize + augmentedParameterOffset);

        if (augmentedClass != null) {
            javaTypeParameters.add(targetClass);
        }

        for (Class<?> typeParameter : typeParameters) {
            if (isValidType(typeParameter) == false) {
                throw lookupException(
                    "type parameter [%s] not found for method [[%s], [%s], %s]",
                    typeToCanonicalTypeName(typeParameter),
                    targetCanonicalClassName,
                    methodName,
                    typesToCanonicalTypeNames(typeParameters)
                );
            }

            javaTypeParameters.add(typeToJavaType(typeParameter));
        }

        if (isValidType(returnType) == false) {
            throw lookupException(
                "return type [%s] not found for method [[%s], [%s], %s]",
                typeToCanonicalTypeName(returnType),
                targetCanonicalClassName,
                methodName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        Method javaMethod;

        if (augmentedClass == null) {
            try {
                javaMethod = targetClass.getMethod(methodName, javaTypeParameters.toArray(Class<?>[]::new));
            } catch (NoSuchMethodException nsme) {
                throw lookupException(
                    nsme,
                    "reflection object not found for method [[%s], [%s], %s]",
                    targetCanonicalClassName,
                    methodName,
                    typesToCanonicalTypeNames(typeParameters)
                );
            }
        } else {
            try {
                javaMethod = augmentedClass.getMethod(methodName, javaTypeParameters.toArray(Class<?>[]::new));

                if (Modifier.isStatic(javaMethod.getModifiers()) == false) {
                    throw lookupException(
                        "method [[%s], [%s], %s] with augmented class [%s] must be static",
                        targetCanonicalClassName,
                        methodName,
                        typesToCanonicalTypeNames(typeParameters),
                        typeToCanonicalTypeName(augmentedClass)
                    );
                }
            } catch (NoSuchMethodException nsme) {
                throw lookupException(
                    nsme,
                    "reflection object not found for method [[%s], [%s], %s] with augmented class [%s]",
                    targetCanonicalClassName,
                    methodName,
                    typesToCanonicalTypeNames(typeParameters),
                    typeToCanonicalTypeName(augmentedClass)
                );
            }
        }

        // injections alter the type parameters required for the user to call this method, since some are injected by compiler
        InjectConstantAnnotation inject = (InjectConstantAnnotation) annotations.get(InjectConstantAnnotation.class);
        if (inject != null) {
            int numInjections = inject.injects().size();

            if (numInjections > 0) {
                typeParameters.subList(0, numInjections).clear();
            }

            typeParametersSize = typeParameters.size();
        }

        if (javaMethod.getReturnType() != typeToJavaType(returnType)) {
            throw lookupException(
                "return type [%s] does not match the specified returned type [%s] for method [[%s], [%s], %s]",
                typeToCanonicalTypeName(javaMethod.getReturnType()),
                typeToCanonicalTypeName(returnType),
                targetClass.getCanonicalName(),
                methodName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        MethodHandle methodHandle;

        if (augmentedClass == null) {
            try {
                methodHandle = lookup(targetClass).unreflect(javaMethod);
            } catch (IllegalAccessException iae) {
                throw lookupException(
                    iae,
                    "method handle not found for method [[%s], [%s], %s], with lookup [%s]",
                    targetClass.getCanonicalName(),
                    methodName,
                    typesToCanonicalTypeNames(typeParameters),
                    lookup(targetClass)
                );
            }
        } else {
            try {
                methodHandle = lookup(augmentedClass).unreflect(javaMethod);
            } catch (IllegalAccessException iae) {
                throw lookupException(
                    iae,
                    "method handle not found for method [[%s], [%s], %s] with augmented class [%s]",
                    targetClass.getCanonicalName(),
                    methodName,
                    typesToCanonicalTypeNames(typeParameters),
                    typeToCanonicalTypeName(augmentedClass)
                );
            }
        }

        if (annotations.containsKey(CompileTimeOnlyAnnotation.class)) {
            throw new IllegalArgumentException("regular methods can't have @" + CompileTimeOnlyAnnotation.NAME);
        }

        MethodType methodType = methodHandle.type();
        boolean isStatic = augmentedClass == null && Modifier.isStatic(javaMethod.getModifiers());
        String painlessMethodKey = buildPainlessMethodKey(methodName, typeParametersSize);
        PainlessMethod existingPainlessMethod = isStatic
            ? painlessClassBuilder.staticMethods.get(painlessMethodKey)
            : painlessClassBuilder.methods.get(painlessMethodKey);
        PainlessMethod newPainlessMethod = new PainlessMethod(
            javaMethod,
            targetClass,
            returnType,
            typeParameters,
            methodHandle,
            methodType,
            annotations
        );

        if (existingPainlessMethod == null) {
            newPainlessMethod = painlessMethodCache.computeIfAbsent(newPainlessMethod, key -> key);

            if (isStatic) {
                painlessClassBuilder.staticMethods.put(painlessMethodKey.intern(), newPainlessMethod);
            } else {
                painlessClassBuilder.methods.put(painlessMethodKey.intern(), newPainlessMethod);
            }
        } else if (newPainlessMethod.equals(existingPainlessMethod) == false) {
            throw lookupException(
                "cannot add methods with the same name and arity but are not equivalent for methods "
                    + "[[%s], [%s], [%s], %s] and [[%s], [%s], [%s], %s]",
                targetCanonicalClassName,
                methodName,
                typeToCanonicalTypeName(returnType),
                typesToCanonicalTypeNames(typeParameters),
                targetCanonicalClassName,
                methodName,
                typeToCanonicalTypeName(existingPainlessMethod.returnType()),
                typesToCanonicalTypeNames(existingPainlessMethod.typeParameters())
            );
        }
    }

    public void addPainlessField(
        ClassLoader classLoader,
        String targetCanonicalClassName,
        String fieldName,
        String canonicalTypeNameParameter,
        Map<Class<?>, Object> annotations
    ) {

        Objects.requireNonNull(classLoader);
        Objects.requireNonNull(targetCanonicalClassName);
        Objects.requireNonNull(fieldName);
        Objects.requireNonNull(canonicalTypeNameParameter);
        Objects.requireNonNull(annotations);

        Class<?> targetClass = canonicalClassNamesToClasses.get(targetCanonicalClassName);

        if (targetClass == null) {
            throw lookupException(
                "target class [%s] not found for field [[%s], [%s], [%s]]",
                targetCanonicalClassName,
                targetCanonicalClassName,
                fieldName,
                canonicalTypeNameParameter
            );
        }

        String augmentedCanonicalClassName = annotations.containsKey(AugmentedAnnotation.class)
            ? ((AugmentedAnnotation) annotations.get(AugmentedAnnotation.class)).augmentedCanonicalClassName()
            : null;

        Class<?> augmentedClass = null;

        if (augmentedCanonicalClassName != null) {
            augmentedClass = loadClass(
                classLoader,
                augmentedCanonicalClassName,
                () -> Strings.format(
                    "augmented class [%s] not found for field [[%s], [%s]]",
                    augmentedCanonicalClassName,
                    targetCanonicalClassName,
                    fieldName
                )
            );
        }

        Class<?> typeParameter = canonicalTypeNameToType(canonicalTypeNameParameter);

        if (typeParameter == null) {
            throw lookupException(
                "type parameter [%s] not found for field [[%s], [%s]]",
                canonicalTypeNameParameter,
                targetCanonicalClassName,
                fieldName
            );
        }

        addPainlessField(targetClass, augmentedClass, fieldName, typeParameter, annotations);
    }

    public void addPainlessField(
        Class<?> targetClass,
        Class<?> augmentedClass,
        String fieldName,
        Class<?> typeParameter,
        Map<Class<?>, Object> annotations
    ) {

        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(fieldName);
        Objects.requireNonNull(typeParameter);
        Objects.requireNonNull(annotations);

        if (targetClass == def.class) {
            throw new IllegalArgumentException("cannot add field to reserved class [" + DEF_CLASS_NAME + "]");
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);

        if (FIELD_NAME_PATTERN.matcher(fieldName).matches() == false) {
            throw new IllegalArgumentException(
                "invalid field name [" + fieldName + "] for target class [" + targetCanonicalClassName + "]."
            );
        }

        PainlessClassBuilder painlessClassBuilder = classesToPainlessClassBuilders.get(targetClass);

        if (painlessClassBuilder == null) {
            throw lookupException(
                "target class [%s] not found for field [[%s], [%s], [%s]]",
                targetCanonicalClassName,
                targetCanonicalClassName,
                fieldName,
                typeToCanonicalTypeName(typeParameter)
            );
        }

        if (isValidType(typeParameter) == false) {
            throw lookupException(
                "type parameter [%s] not found for field [[%s], [%s], [%s]]",
                typeToCanonicalTypeName(typeParameter),
                targetCanonicalClassName,
                fieldName,
                typeToCanonicalTypeName(typeParameter)
            );
        }

        Field javaField;

        if (augmentedClass == null) {
            try {
                javaField = targetClass.getField(fieldName);
            } catch (NoSuchFieldException nsfe) {
                throw lookupException(
                    nsfe,
                    "reflection object not found for field [[%s], [%s], [%s]]",
                    targetCanonicalClassName,
                    fieldName,
                    typeToCanonicalTypeName(typeParameter)
                );
            }
        } else {
            try {
                javaField = augmentedClass.getField(fieldName);

                if (Modifier.isStatic(javaField.getModifiers()) == false || Modifier.isFinal(javaField.getModifiers()) == false) {
                    throw lookupException(
                        "field [[%s], [%s]] with augmented class [%s] must be static and final",
                        targetCanonicalClassName,
                        fieldName,
                        typeToCanonicalTypeName(augmentedClass)
                    );
                }
            } catch (NoSuchFieldException nsfe) {
                throw lookupException(
                    nsfe,
                    "reflection object not found for field [[%s], [%s], [%s]] with augmented class [%s]",
                    targetCanonicalClassName,
                    fieldName,
                    typeToCanonicalTypeName(typeParameter),
                    typeToCanonicalTypeName(augmentedClass)
                );
            }
        }

        if (javaField.getType() != typeToJavaType(typeParameter)) {
            throw lookupException(
                "type parameter [%s] does not match the specified type parameter [%s] for field [[%s], [%s]]",
                typeToCanonicalTypeName(javaField.getType()),
                typeToCanonicalTypeName(typeParameter),
                targetCanonicalClassName,
                fieldName
            );
        }

        MethodHandle methodHandleGetter;

        try {
            methodHandleGetter = MethodHandles.publicLookup().unreflectGetter(javaField);
        } catch (IllegalAccessException iae) {
            throw new IllegalArgumentException(
                "getter method handle not found for field [[" + targetCanonicalClassName + "], [" + fieldName + "]]"
            );
        }

        String painlessFieldKey = buildPainlessFieldKey(fieldName);

        if (Modifier.isStatic(javaField.getModifiers())) {
            if (Modifier.isFinal(javaField.getModifiers()) == false) {
                throw new IllegalArgumentException("static field [[" + targetCanonicalClassName + "], [" + fieldName + "]] must be final");
            }

            PainlessField existingPainlessField = painlessClassBuilder.staticFields.get(painlessFieldKey);
            PainlessField newPainlessField = new PainlessField(javaField, typeParameter, annotations, methodHandleGetter, null);

            if (existingPainlessField == null) {
                newPainlessField = painlessFieldCache.computeIfAbsent(newPainlessField, Function.identity());
                painlessClassBuilder.staticFields.put(painlessFieldKey.intern(), newPainlessField);
            } else if (newPainlessField.equals(existingPainlessField) == false) {
                throw lookupException(
                    "cannot add fields with the same name but are not equivalent for fields [[%s], [%s], [%s]] and [[%s], [%s], [%s]]"
                        + " with the same name and different type parameters",
                    targetCanonicalClassName,
                    fieldName,
                    typeToCanonicalTypeName(typeParameter),
                    targetCanonicalClassName,
                    existingPainlessField.javaField().getName(),
                    typeToCanonicalTypeName(existingPainlessField.typeParameter())
                );
            }
        } else {
            MethodHandle methodHandleSetter;

            try {
                methodHandleSetter = MethodHandles.publicLookup().unreflectSetter(javaField);
            } catch (IllegalAccessException iae) {
                throw new IllegalArgumentException(
                    "setter method handle not found for field [[" + targetCanonicalClassName + "], [" + fieldName + "]]"
                );
            }

            PainlessField existingPainlessField = painlessClassBuilder.fields.get(painlessFieldKey);
            PainlessField newPainlessField = new PainlessField(
                javaField,
                typeParameter,
                annotations,
                methodHandleGetter,
                methodHandleSetter
            );

            if (existingPainlessField == null) {
                newPainlessField = painlessFieldCache.computeIfAbsent(newPainlessField, key -> key);
                painlessClassBuilder.fields.put(painlessFieldKey.intern(), newPainlessField);
            } else if (newPainlessField.equals(existingPainlessField) == false) {
                throw lookupException(
                    "cannot add fields with the same name but are not equivalent for fields [[%s], [%s], [%s]] and [[%s], [%s], [%s]]"
                        + " with the same name and different type parameters",
                    targetCanonicalClassName,
                    fieldName,
                    typeToCanonicalTypeName(typeParameter),
                    targetCanonicalClassName,
                    existingPainlessField.javaField().getName(),
                    typeToCanonicalTypeName(existingPainlessField.typeParameter())
                );
            }
        }
    }

    public void addImportedPainlessMethod(
        ClassLoader classLoader,
        String targetJavaClassName,
        String methodName,
        String returnCanonicalTypeName,
        List<String> canonicalTypeNameParameters,
        Map<Class<?>, Object> annotations
    ) {

        Objects.requireNonNull(classLoader);
        Objects.requireNonNull(targetJavaClassName);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnCanonicalTypeName);
        Objects.requireNonNull(canonicalTypeNameParameters);

        Class<?> targetClass = loadClass(classLoader, targetJavaClassName, () -> "class [" + targetJavaClassName + "] not found");
        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);

        List<Class<?>> typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());

        for (String canonicalTypeNameParameter : canonicalTypeNameParameters) {
            Class<?> typeParameter = canonicalTypeNameToType(canonicalTypeNameParameter);

            if (typeParameter == null) {
                throw lookupException(
                    "type parameter [%s] not found for imported method [[%s], [%s], %s]",
                    canonicalTypeNameParameter,
                    targetCanonicalClassName,
                    methodName,
                    canonicalTypeNameParameters
                );
            }

            typeParameters.add(typeParameter);
        }

        Class<?> returnType = canonicalTypeNameToType(returnCanonicalTypeName);

        if (returnType == null) {
            throw lookupException(
                "return type [%s] not found for imported method [[%s], [%s], %s]",
                returnCanonicalTypeName,
                targetCanonicalClassName,
                methodName,
                canonicalTypeNameParameters
            );
        }

        addImportedPainlessMethod(targetClass, methodName, returnType, typeParameters, annotations);
    }

    public void addImportedPainlessMethod(
        Class<?> targetClass,
        String methodName,
        Class<?> returnType,
        List<Class<?>> typeParameters,
        Map<Class<?>, Object> annotations
    ) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnType);
        Objects.requireNonNull(typeParameters);

        if (targetClass == def.class) {
            throw new IllegalArgumentException("cannot add imported method from reserved class [" + DEF_CLASS_NAME + "]");
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);
        Class<?> existingTargetClass = javaClassNamesToClasses.get(targetClass.getName());

        if (existingTargetClass == null) {
            javaClassNamesToClasses.put(targetClass.getName().intern(), targetClass);
        } else if (existingTargetClass != targetClass) {
            throw lookupException(
                "class [%s] cannot represent multiple java classes with the same name from different class loaders",
                targetCanonicalClassName
            );
        }

        if (METHOD_NAME_PATTERN.matcher(methodName).matches() == false) {
            throw new IllegalArgumentException(
                "invalid imported method name [" + methodName + "] for target class [" + targetCanonicalClassName + "]."
            );
        }

        int typeParametersSize = typeParameters.size();
        List<Class<?>> javaTypeParameters = new ArrayList<>(typeParametersSize);

        for (Class<?> typeParameter : typeParameters) {
            if (isValidType(typeParameter) == false) {
                throw lookupException(
                    "type parameter [%s] not found for imported method [[%s], [%s], %s]",
                    typeToCanonicalTypeName(typeParameter),
                    targetCanonicalClassName,
                    methodName,
                    typesToCanonicalTypeNames(typeParameters)
                );
            }

            javaTypeParameters.add(typeToJavaType(typeParameter));
        }

        if (isValidType(returnType) == false) {
            throw lookupException(
                "return type [%s] not found for imported method [[%s], [%s], %s]",
                typeToCanonicalTypeName(returnType),
                targetCanonicalClassName,
                methodName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        Method javaMethod;

        try {
            javaMethod = targetClass.getMethod(methodName, javaTypeParameters.toArray(new Class<?>[typeParametersSize]));
        } catch (NoSuchMethodException nsme) {
            throw lookupException(
                nsme,
                "imported method reflection object [[%s], [%s], %s] not found",
                targetCanonicalClassName,
                methodName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        if (javaMethod.getReturnType() != typeToJavaType(returnType)) {
            throw lookupException(
                "return type [%s] does not match the specified returned type [%s] for imported method [[%s], [%s], %s]",
                typeToCanonicalTypeName(javaMethod.getReturnType()),
                typeToCanonicalTypeName(returnType),
                targetClass.getCanonicalName(),
                methodName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        if (Modifier.isStatic(javaMethod.getModifiers()) == false) {
            throw lookupException(
                "imported method [[%s], [%s], %s] must be static",
                targetClass.getCanonicalName(),
                methodName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        String painlessMethodKey = buildPainlessMethodKey(methodName, typeParametersSize);

        if (painlessMethodKeysToPainlessClassBindings.containsKey(painlessMethodKey)) {
            throw new IllegalArgumentException("imported method and class binding cannot have the same name [" + methodName + "]");
        }

        if (painlessMethodKeysToPainlessInstanceBindings.containsKey(painlessMethodKey)) {
            throw new IllegalArgumentException("imported method and instance binding cannot have the same name [" + methodName + "]");
        }

        MethodHandle methodHandle;

        try {
            methodHandle = lookup(targetClass).unreflect(javaMethod);
        } catch (IllegalAccessException iae) {
            throw lookupException(
                iae,
                "imported method handle [[%s], [%s], %s] not found",
                targetClass.getCanonicalName(),
                methodName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        MethodType methodType = methodHandle.type();

        PainlessMethod existingImportedPainlessMethod = painlessMethodKeysToImportedPainlessMethods.get(painlessMethodKey);
        PainlessMethod newImportedPainlessMethod = new PainlessMethod(
            javaMethod,
            targetClass,
            returnType,
            typeParameters,
            methodHandle,
            methodType,
            annotations
        );

        if (existingImportedPainlessMethod == null) {
            newImportedPainlessMethod = painlessMethodCache.computeIfAbsent(newImportedPainlessMethod, key -> key);
            painlessMethodKeysToImportedPainlessMethods.put(painlessMethodKey.intern(), newImportedPainlessMethod);
        } else if (newImportedPainlessMethod.equals(existingImportedPainlessMethod) == false) {
            throw lookupException(
                "cannot add imported methods with the same name and arity but do not have equivalent methods "
                    + "[[%s], [%s], [%s], %s] and [[%s], [%s], [%s], %s]",
                targetCanonicalClassName,
                methodName,
                typeToCanonicalTypeName(returnType),
                typesToCanonicalTypeNames(typeParameters),
                targetCanonicalClassName,
                methodName,
                typeToCanonicalTypeName(existingImportedPainlessMethod.returnType()),
                typesToCanonicalTypeNames(existingImportedPainlessMethod.typeParameters())
            );
        }
    }

    public void addPainlessClassBinding(
        ClassLoader classLoader,
        String targetJavaClassName,
        String methodName,
        String returnCanonicalTypeName,
        List<String> canonicalTypeNameParameters,
        Map<Class<?>, Object> annotations
    ) {

        Objects.requireNonNull(classLoader);
        Objects.requireNonNull(targetJavaClassName);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnCanonicalTypeName);
        Objects.requireNonNull(canonicalTypeNameParameters);

        Class<?> targetClass = loadClass(classLoader, targetJavaClassName, () -> "class [" + targetJavaClassName + "] not found");
        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);
        List<Class<?>> typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());

        for (String canonicalTypeNameParameter : canonicalTypeNameParameters) {
            Class<?> typeParameter = canonicalTypeNameToType(canonicalTypeNameParameter);

            if (typeParameter == null) {
                throw lookupException(
                    "type parameter [%s] not found for class binding [[%s], [%s], %s]",
                    canonicalTypeNameParameter,
                    targetCanonicalClassName,
                    methodName,
                    canonicalTypeNameParameters
                );
            }

            typeParameters.add(typeParameter);
        }

        Class<?> returnType = canonicalTypeNameToType(returnCanonicalTypeName);

        if (returnType == null) {
            throw lookupException(
                "return type [%s] not found for class binding [[%s], [%s], %s]",
                returnCanonicalTypeName,
                targetCanonicalClassName,
                methodName,
                canonicalTypeNameParameters
            );
        }

        addPainlessClassBinding(targetClass, methodName, returnType, typeParameters, annotations);
    }

    public void addPainlessClassBinding(
        Class<?> targetClass,
        String methodName,
        Class<?> returnType,
        List<Class<?>> typeParameters,
        Map<Class<?>, Object> annotations
    ) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnType);
        Objects.requireNonNull(typeParameters);

        if (targetClass == def.class) {
            throw new IllegalArgumentException("cannot add class binding as reserved class [" + DEF_CLASS_NAME + "]");
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);
        Class<?> existingTargetClass = javaClassNamesToClasses.get(targetClass.getName());

        if (existingTargetClass == null) {
            javaClassNamesToClasses.put(targetClass.getName().intern(), targetClass);
        } else if (existingTargetClass != targetClass) {
            throw lookupException(
                "class [%s] cannot represent multiple java classes with the same name from different class loaders",
                targetCanonicalClassName
            );
        }

        Constructor<?>[] javaConstructors = targetClass.getConstructors();
        Constructor<?> javaConstructor = null;

        for (Constructor<?> eachJavaConstructor : javaConstructors) {
            if (eachJavaConstructor.getDeclaringClass() == targetClass) {
                if (javaConstructor != null) {
                    throw new IllegalArgumentException(
                        "class binding [" + targetCanonicalClassName + "] cannot have multiple constructors"
                    );
                }

                javaConstructor = eachJavaConstructor;
            }
        }

        if (javaConstructor == null) {
            throw new IllegalArgumentException("class binding [" + targetCanonicalClassName + "] must have exactly one constructor");
        }

        Class<?>[] constructorParameterTypes = javaConstructor.getParameterTypes();

        for (int typeParameterIndex = 0; typeParameterIndex < constructorParameterTypes.length; ++typeParameterIndex) {
            Class<?> typeParameter = typeParameters.get(typeParameterIndex);

            if (isValidType(typeParameter) == false) {
                throw lookupException(
                    "type parameter [%s] not found for class binding [[%s], %s]",
                    typeToCanonicalTypeName(typeParameter),
                    targetCanonicalClassName,
                    typesToCanonicalTypeNames(typeParameters)
                );
            }

            Class<?> javaTypeParameter = constructorParameterTypes[typeParameterIndex];

            if (isValidType(javaTypeParameter) == false) {
                throw lookupException(
                    "type parameter [%s] not found for class binding [[%s], %s]",
                    typeToCanonicalTypeName(typeParameter),
                    targetCanonicalClassName,
                    typesToCanonicalTypeNames(typeParameters)
                );
            }

            if (javaTypeParameter != typeToJavaType(typeParameter)) {
                throw lookupException(
                    "type parameter [%s] does not match the specified type parameter [%s] for class binding [[%s], %s]",
                    typeToCanonicalTypeName(javaTypeParameter),
                    typeToCanonicalTypeName(typeParameter),
                    targetClass.getCanonicalName(),
                    typesToCanonicalTypeNames(typeParameters)
                );
            }
        }

        if (METHOD_NAME_PATTERN.matcher(methodName).matches() == false) {
            throw new IllegalArgumentException(
                "invalid method name [" + methodName + "] for class binding [" + targetCanonicalClassName + "]."
            );
        }

        if (annotations.containsKey(CompileTimeOnlyAnnotation.class)) {
            throw new IllegalArgumentException("class bindings can't have @" + CompileTimeOnlyAnnotation.NAME);
        }

        Method[] javaMethods = targetClass.getMethods();
        Method javaMethod = null;

        for (Method eachJavaMethod : javaMethods) {
            if (eachJavaMethod.getDeclaringClass() == targetClass) {
                if (javaMethod != null) {
                    throw new IllegalArgumentException("class binding [" + targetCanonicalClassName + "] cannot have multiple methods");
                }

                javaMethod = eachJavaMethod;
            }
        }

        if (javaMethod == null) {
            throw new IllegalArgumentException("class binding [" + targetCanonicalClassName + "] must have exactly one method");
        }

        Class<?>[] methodParameterTypes = javaMethod.getParameterTypes();

        for (int typeParameterIndex = 0; typeParameterIndex < methodParameterTypes.length; ++typeParameterIndex) {
            Class<?> typeParameter = typeParameters.get(constructorParameterTypes.length + typeParameterIndex);

            if (isValidType(typeParameter) == false) {
                throw lookupException(
                    "type parameter [%s] not found for class binding [[%s], %s]",
                    typeToCanonicalTypeName(typeParameter),
                    targetCanonicalClassName,
                    typesToCanonicalTypeNames(typeParameters)
                );
            }

            Class<?> javaTypeParameter = javaMethod.getParameterTypes()[typeParameterIndex];

            if (isValidType(javaTypeParameter) == false) {
                throw lookupException(
                    "type parameter [%s] not found for class binding [[%s], %s]",
                    typeToCanonicalTypeName(typeParameter),
                    targetCanonicalClassName,
                    typesToCanonicalTypeNames(typeParameters)
                );
            }

            if (javaTypeParameter != typeToJavaType(typeParameter)) {
                throw lookupException(
                    "type parameter [%s] does not match the specified type parameter [%s] for class binding [[%s], %s]",
                    typeToCanonicalTypeName(javaTypeParameter),
                    typeToCanonicalTypeName(typeParameter),
                    targetClass.getCanonicalName(),
                    typesToCanonicalTypeNames(typeParameters)
                );
            }
        }

        if (isValidType(returnType) == false) {
            throw lookupException(
                "return type [%s] not found for class binding [[%s], [%s], %s]",
                typeToCanonicalTypeName(returnType),
                targetCanonicalClassName,
                methodName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        if (javaMethod.getReturnType() != typeToJavaType(returnType)) {
            throw lookupException(
                "return type [%s] does not match the specified returned type [%s] for class binding [[%s], [%s], %s]",
                typeToCanonicalTypeName(javaMethod.getReturnType()),
                typeToCanonicalTypeName(returnType),
                targetClass.getCanonicalName(),
                methodName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        String painlessMethodKey = buildPainlessMethodKey(methodName, constructorParameterTypes.length + methodParameterTypes.length);

        if (painlessMethodKeysToImportedPainlessMethods.containsKey(painlessMethodKey)) {
            throw new IllegalArgumentException("class binding and imported method cannot have the same name [" + methodName + "]");
        }

        if (painlessMethodKeysToPainlessInstanceBindings.containsKey(painlessMethodKey)) {
            throw new IllegalArgumentException("class binding and instance binding cannot have the same name [" + methodName + "]");
        }

        if (Modifier.isStatic(javaMethod.getModifiers())) {
            throw lookupException(
                "class binding [[%s], [%s], %s] cannot be static",
                targetClass.getCanonicalName(),
                methodName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        PainlessClassBinding existingPainlessClassBinding = painlessMethodKeysToPainlessClassBindings.get(painlessMethodKey);
        PainlessClassBinding newPainlessClassBinding = new PainlessClassBinding(
            javaConstructor,
            javaMethod,
            returnType,
            typeParameters,
            annotations
        );

        if (existingPainlessClassBinding == null) {
            newPainlessClassBinding = painlessClassBindingCache.computeIfAbsent(newPainlessClassBinding, Function.identity());
            painlessMethodKeysToPainlessClassBindings.put(painlessMethodKey.intern(), newPainlessClassBinding);
        } else if (newPainlessClassBinding.equals(existingPainlessClassBinding) == false) {
            throw lookupException(
                "cannot add class bindings with the same name and arity but do not have equivalent methods "
                    + "[[%s], [%s], [%s], %s] and [[%s], [%s], [%s], %s]",
                targetCanonicalClassName,
                methodName,
                typeToCanonicalTypeName(returnType),
                typesToCanonicalTypeNames(typeParameters),
                targetCanonicalClassName,
                methodName,
                typeToCanonicalTypeName(existingPainlessClassBinding.returnType()),
                typesToCanonicalTypeNames(existingPainlessClassBinding.typeParameters())
            );
        }
    }

    public void addPainlessInstanceBinding(
        Object targetInstance,
        String methodName,
        String returnCanonicalTypeName,
        List<String> canonicalTypeNameParameters,
        Map<Class<?>, Object> painlessAnnotations
    ) {

        Objects.requireNonNull(targetInstance);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnCanonicalTypeName);
        Objects.requireNonNull(canonicalTypeNameParameters);

        Class<?> targetClass = targetInstance.getClass();
        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);
        List<Class<?>> typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());

        for (String canonicalTypeNameParameter : canonicalTypeNameParameters) {
            Class<?> typeParameter = canonicalTypeNameToType(canonicalTypeNameParameter);

            if (typeParameter == null) {
                throw lookupException(
                    "type parameter [%s] not found for instance binding [[%s], [%s], %s]",
                    canonicalTypeNameParameter,
                    targetCanonicalClassName,
                    methodName,
                    canonicalTypeNameParameters
                );
            }

            typeParameters.add(typeParameter);
        }

        Class<?> returnType = canonicalTypeNameToType(returnCanonicalTypeName);

        if (returnType == null) {
            throw lookupException(
                "return type [%s] not found for class binding [[%s], [%s], %s]",
                returnCanonicalTypeName,
                targetCanonicalClassName,
                methodName,
                canonicalTypeNameParameters
            );
        }

        addPainlessInstanceBinding(targetInstance, methodName, returnType, typeParameters, painlessAnnotations);
    }

    public void addPainlessInstanceBinding(
        Object targetInstance,
        String methodName,
        Class<?> returnType,
        List<Class<?>> typeParameters,
        Map<Class<?>, Object> painlessAnnotations
    ) {
        Objects.requireNonNull(targetInstance);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnType);
        Objects.requireNonNull(typeParameters);

        Class<?> targetClass = targetInstance.getClass();

        if (targetClass == def.class) {
            throw new IllegalArgumentException("cannot add instance binding as reserved class [" + DEF_CLASS_NAME + "]");
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);
        Class<?> existingTargetClass = javaClassNamesToClasses.get(targetClass.getName());

        if (existingTargetClass == null) {
            javaClassNamesToClasses.put(targetClass.getName().intern(), targetClass);
        } else if (existingTargetClass != targetClass) {
            throw lookupException(
                "class [%s] cannot represent multiple java classes with the same name from different class loaders",
                targetCanonicalClassName
            );
        }

        if (METHOD_NAME_PATTERN.matcher(methodName).matches() == false) {
            throw new IllegalArgumentException(
                "invalid method name [" + methodName + "] for instance binding [" + targetCanonicalClassName + "]."
            );
        }

        int typeParametersSize = typeParameters.size();
        List<Class<?>> javaTypeParameters = new ArrayList<>(typeParametersSize);

        for (Class<?> typeParameter : typeParameters) {
            if (isValidType(typeParameter) == false) {
                throw lookupException(
                    "type parameter [%s] not found for instance binding [[%s], [%s], %s]",
                    typeToCanonicalTypeName(typeParameter),
                    targetCanonicalClassName,
                    methodName,
                    typesToCanonicalTypeNames(typeParameters)
                );
            }

            javaTypeParameters.add(typeToJavaType(typeParameter));
        }

        if (isValidType(returnType) == false) {
            throw lookupException(
                "return type [%s] not found for imported method [[%s], [%s], %s]",
                typeToCanonicalTypeName(returnType),
                targetCanonicalClassName,
                methodName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        Method javaMethod;

        try {
            javaMethod = targetClass.getMethod(methodName, javaTypeParameters.toArray(new Class<?>[typeParametersSize]));
        } catch (NoSuchMethodException nsme) {
            throw lookupException(
                nsme,
                "instance binding reflection object [[%s], [%s], %s] not found",
                targetCanonicalClassName,
                methodName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        if (javaMethod.getReturnType() != typeToJavaType(returnType)) {
            throw lookupException(
                "return type [%s] does not match the specified returned type [%s] for instance binding [[%s], [%s], %s]",
                typeToCanonicalTypeName(javaMethod.getReturnType()),
                typeToCanonicalTypeName(returnType),
                targetClass.getCanonicalName(),
                methodName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        if (Modifier.isStatic(javaMethod.getModifiers())) {
            throw lookupException(
                "instance binding [[%s], [%s], %s] cannot be static",
                targetClass.getCanonicalName(),
                methodName,
                typesToCanonicalTypeNames(typeParameters)
            );
        }

        String painlessMethodKey = buildPainlessMethodKey(methodName, typeParametersSize);

        if (painlessMethodKeysToImportedPainlessMethods.containsKey(painlessMethodKey)) {
            throw new IllegalArgumentException("instance binding and imported method cannot have the same name [" + methodName + "]");
        }

        if (painlessMethodKeysToPainlessClassBindings.containsKey(painlessMethodKey)) {
            throw new IllegalArgumentException("instance binding and class binding cannot have the same name [" + methodName + "]");
        }

        PainlessInstanceBinding existingPainlessInstanceBinding = painlessMethodKeysToPainlessInstanceBindings.get(painlessMethodKey);
        PainlessInstanceBinding newPainlessInstanceBinding = new PainlessInstanceBinding(
            targetInstance,
            javaMethod,
            returnType,
            typeParameters,
            painlessAnnotations
        );

        if (existingPainlessInstanceBinding == null) {
            newPainlessInstanceBinding = painlessInstanceBindingCache.computeIfAbsent(newPainlessInstanceBinding, key -> key);
            painlessMethodKeysToPainlessInstanceBindings.put(painlessMethodKey.intern(), newPainlessInstanceBinding);
        } else if (newPainlessInstanceBinding.equals(existingPainlessInstanceBinding) == false) {
            throw lookupException(
                "cannot add instances bindings with the same name and arity but do not have equivalent methods "
                    + "[[%s], [%s], [%s], %s], %s and [[%s], [%s], [%s], %s], %s",
                targetCanonicalClassName,
                methodName,
                typeToCanonicalTypeName(returnType),
                typesToCanonicalTypeNames(typeParameters),
                painlessAnnotations,
                targetCanonicalClassName,
                methodName,
                typeToCanonicalTypeName(existingPainlessInstanceBinding.returnType()),
                typesToCanonicalTypeNames(existingPainlessInstanceBinding.typeParameters()),
                existingPainlessInstanceBinding.annotations()
            );
        }
    }

    public PainlessLookup build() {
        buildPainlessClassHierarchy();
        setFunctionalInterfaceMethods();
        generateRuntimeMethods();
        cacheRuntimeHandles();

        Map<Class<?>, PainlessClass> classesToPainlessClasses = Maps.newMapWithExpectedSize(classesToPainlessClassBuilders.size());

        for (Map.Entry<Class<?>, PainlessClassBuilder> painlessClassBuilderEntry : classesToPainlessClassBuilders.entrySet()) {
            classesToPainlessClasses.put(painlessClassBuilderEntry.getKey(), painlessClassBuilderEntry.getValue().build());
        }

        if (javaClassNamesToClasses.values().containsAll(canonicalClassNamesToClasses.values()) == false) {
            throw new IllegalArgumentException(
                "the values of java class names to classes must be a superset of the values of canonical class names to classes"
            );
        }

        if (javaClassNamesToClasses.values().containsAll(classesToPainlessClasses.keySet()) == false) {
            throw new IllegalArgumentException(
                "the values of java class names to classes must be a superset of the keys of classes to painless classes"
            );
        }

        if (canonicalClassNamesToClasses.values().containsAll(classesToPainlessClasses.keySet()) == false
            || classesToPainlessClasses.keySet().containsAll(canonicalClassNamesToClasses.values()) == false) {
            throw new IllegalArgumentException(
                "the values of canonical class names to classes must have the same classes as the keys of classes to painless classes"
            );
        }

        return new PainlessLookup(
            javaClassNamesToClasses,
            canonicalClassNamesToClasses,
            classesToPainlessClasses,
            classesToDirectSubClasses,
            painlessMethodKeysToImportedPainlessMethods,
            painlessMethodKeysToPainlessClassBindings,
            painlessMethodKeysToPainlessInstanceBindings
        );
    }

    private void buildPainlessClassHierarchy() {
        for (Class<?> targetClass : classesToPainlessClassBuilders.keySet()) {
            classesToDirectSubClasses.put(targetClass, new HashSet<>());
        }

        for (Class<?> subClass : classesToPainlessClassBuilders.keySet()) {
            Deque<Class<?>> superInterfaces = new ArrayDeque<>(Arrays.asList(subClass.getInterfaces()));

            // we check for Object.class as part of the allow listed classes because
            // it is possible for the compiler to work without Object
            if (subClass.isInterface() && superInterfaces.isEmpty() && classesToPainlessClassBuilders.containsKey(Object.class)) {
                classesToDirectSubClasses.get(Object.class).add(subClass);
            } else {
                Class<?> superClass = subClass.getSuperclass();

                // this finds the nearest super class for a given sub class
                // because the allow list may have gaps between classes
                // example:
                // class A {} // allowed
                // class B extends A // not allowed
                // class C extends B // allowed
                // in this case C is considered a direct sub class of A
                while (superClass != null) {
                    if (classesToPainlessClassBuilders.containsKey(superClass)) {
                        break;
                    } else {
                        // this ensures all interfaces from a sub class that
                        // is not allow listed are checked if they are
                        // considered a direct super class of the sub class
                        // because these interfaces may still be allow listed
                        // even if their sub class is not
                        superInterfaces.addAll(Arrays.asList(superClass.getInterfaces()));
                    }

                    superClass = superClass.getSuperclass();
                }

                if (superClass != null) {
                    classesToDirectSubClasses.get(superClass).add(subClass);
                }
            }

            Set<Class<?>> resolvedInterfaces = new HashSet<>();

            while (superInterfaces.isEmpty() == false) {
                Class<?> superInterface = superInterfaces.removeFirst();

                if (resolvedInterfaces.add(superInterface)) {
                    if (classesToPainlessClassBuilders.containsKey(superInterface)) {
                        classesToDirectSubClasses.get(superInterface).add(subClass);
                    } else {
                        superInterfaces.addAll(Arrays.asList(superInterface.getInterfaces()));
                    }
                }
            }
        }
    }

    private void setFunctionalInterfaceMethods() {
        classesToPainlessClassBuilders.forEach(this::setFunctionalInterfaceMethod);
    }

    private void setFunctionalInterfaceMethod(Class<?> targetClass, PainlessClassBuilder targetPainlessClassBuilder) {
        if (targetClass.isInterface()) {
            List<java.lang.reflect.Method> javaMethods = new ArrayList<>();

            for (java.lang.reflect.Method javaMethod : targetClass.getMethods()) {
                if (javaMethod.isDefault() == false && Modifier.isStatic(javaMethod.getModifiers()) == false) {
                    try {
                        Object.class.getMethod(javaMethod.getName(), javaMethod.getParameterTypes());
                    } catch (ReflectiveOperationException roe) {
                        javaMethods.add(javaMethod);
                    }
                }
            }

            if (javaMethods.size() != 1 && targetClass.isAnnotationPresent(FunctionalInterface.class)) {
                throw lookupException(
                    "class [%s] is illegally marked as a FunctionalInterface with java methods %s",
                    typeToCanonicalTypeName(targetClass),
                    javaMethods
                );
            } else if (javaMethods.size() == 1) {
                java.lang.reflect.Method javaMethod = javaMethods.get(0);
                String painlessMethodKey = buildPainlessMethodKey(javaMethod.getName(), javaMethod.getParameterCount());

                Deque<Class<?>> superInterfaces = new ArrayDeque<>();
                Set<Class<?>> resolvedInterfaces = new HashSet<>();

                superInterfaces.addLast(targetClass);

                Class<?> superInterface;
                while ((superInterface = superInterfaces.pollFirst()) != null) {

                    if (resolvedInterfaces.add(superInterface)) {
                        PainlessClassBuilder functionalInterfacePainlessClassBuilder = classesToPainlessClassBuilders.get(superInterface);

                        if (functionalInterfacePainlessClassBuilder != null) {
                            targetPainlessClassBuilder.functionalInterfaceMethod = functionalInterfacePainlessClassBuilder.methods.get(
                                painlessMethodKey
                            );

                            if (targetPainlessClassBuilder.functionalInterfaceMethod != null) {
                                break;
                            }
                        }

                        superInterfaces.addAll(Arrays.asList(superInterface.getInterfaces()));
                    }
                }
            }
        }
    }

    /**
     * Creates a {@link Map} of PainlessMethodKeys to {@link PainlessMethod}s per {@link PainlessClass} stored as
     * {@link PainlessClass#runtimeMethods} identical to {@link PainlessClass#methods} with the exception of generated
     * bridge methods. A generated bridge method is created for each whitelisted method that has at least one parameter
     * with a boxed type to cast from other numeric primitive/boxed types in a symmetric was not handled by
     * {@link MethodHandle#asType(MethodType)}. As an example {@link MethodHandle#asType(MethodType)} legally casts
     * from {@link Integer} to long but not from int to {@link Long}. Generated bridge methods cover the latter case.
     * A generated bridge method replaces the method its a bridge to in the {@link PainlessClass#runtimeMethods}
     * {@link Map}. The {@link PainlessClass#runtimeMethods} {@link Map} is used exclusively to look up methods at
     * run-time resulting from calls with a def type value target.
     */
    private void generateRuntimeMethods() {
        for (Map.Entry<Class<?>, PainlessClassBuilder> painlessClassBuilderEntry : classesToPainlessClassBuilders.entrySet()) {
            Class<?> targetClass = painlessClassBuilderEntry.getKey();
            PainlessClassBuilder painlessClassBuilder = painlessClassBuilderEntry.getValue();
            painlessClassBuilder.runtimeMethods.putAll(painlessClassBuilder.methods);

            for (PainlessMethod painlessMethod : painlessClassBuilder.runtimeMethods.values()) {
                for (Class<?> typeParameter : painlessMethod.typeParameters()) {
                    if (typeParameter == Byte.class
                        || typeParameter == Short.class
                        || typeParameter == Character.class
                        || typeParameter == Integer.class
                        || typeParameter == Long.class
                        || typeParameter == Float.class
                        || typeParameter == Double.class) {
                        generateFilteredMethod(targetClass, painlessClassBuilder, painlessMethod);
                    }
                }
            }
        }
    }

    private void generateFilteredMethod(Class<?> targetClass, PainlessClassBuilder painlessClassBuilder, PainlessMethod painlessMethod) {
        String painlessMethodKey = buildPainlessMethodKey(painlessMethod.javaMethod().getName(), painlessMethod.typeParameters().size());
        PainlessMethod filteredPainlessMethod = painlessFilteredCache.get(painlessMethod);

        if (filteredPainlessMethod == null) {
            Method javaMethod = painlessMethod.javaMethod();
            boolean isStatic = Modifier.isStatic(painlessMethod.javaMethod().getModifiers());
            int filteredTypeParameterOffset = isStatic ? 0 : 1;
            List<Class<?>> filteredTypeParameters = new ArrayList<>(javaMethod.getParameterCount() + filteredTypeParameterOffset);

            if (isStatic == false) {
                filteredTypeParameters.add(javaMethod.getDeclaringClass());
            }

            for (Class<?> typeParameter : javaMethod.getParameterTypes()) {
                if (typeParameter == Byte.class
                    || typeParameter == Short.class
                    || typeParameter == Character.class
                    || typeParameter == Integer.class
                    || typeParameter == Long.class
                    || typeParameter == Float.class
                    || typeParameter == Double.class) {
                    filteredTypeParameters.add(Object.class);
                } else {
                    filteredTypeParameters.add(typeParameter);
                }
            }

            MethodType filteredMethodType = MethodType.methodType(painlessMethod.returnType(), filteredTypeParameters);
            MethodHandle filteredMethodHandle = painlessMethod.methodHandle();

            try {
                Class<?>[] methodParameters = javaMethod.getParameterTypes();
                for (int typeParameterCount = 0; typeParameterCount < methodParameters.length; ++typeParameterCount) {
                    Class<?> typeParameter = methodParameters[typeParameterCount];
                    MethodHandle castMethodHandle = Def.DEF_TO_BOXED_TYPE_IMPLICIT_CAST.get(typeParameter);

                    if (castMethodHandle != null) {
                        filteredMethodHandle = MethodHandles.filterArguments(
                            filteredMethodHandle,
                            typeParameterCount + filteredTypeParameterOffset,
                            castMethodHandle
                        );
                    }
                }

                filteredPainlessMethod = new PainlessMethod(
                    painlessMethod.javaMethod(),
                    targetClass,
                    painlessMethod.returnType(),
                    filteredTypeParameters,
                    filteredMethodHandle,
                    filteredMethodType,
                    Map.of()
                );
                painlessClassBuilder.runtimeMethods.put(painlessMethodKey.intern(), filteredPainlessMethod);
                painlessFilteredCache.put(painlessMethod, filteredPainlessMethod);
            } catch (Exception exception) {
                throw new IllegalStateException(
                    "internal error occurred attempting to generate a runtime method [" + painlessMethodKey + "]",
                    exception
                );
            }
        } else {
            painlessClassBuilder.runtimeMethods.put(painlessMethodKey.intern(), filteredPainlessMethod);
        }
    }

    private void cacheRuntimeHandles() {
        classesToPainlessClassBuilders.values().forEach(this::cacheRuntimeHandles);
    }

    private void cacheRuntimeHandles(PainlessClassBuilder painlessClassBuilder) {
        for (Map.Entry<String, PainlessMethod> painlessMethodEntry : painlessClassBuilder.methods.entrySet()) {
            String methodKey = painlessMethodEntry.getKey();
            PainlessMethod painlessMethod = painlessMethodEntry.getValue();
            PainlessMethod bridgePainlessMethod = painlessClassBuilder.runtimeMethods.get(methodKey);
            String methodName = painlessMethod.javaMethod().getName();
            int typeParametersSize = painlessMethod.typeParameters().size();

            if (typeParametersSize == 0
                && methodName.startsWith("get")
                && methodName.length() > 3
                && Character.isUpperCase(methodName.charAt(3))) {
                painlessClassBuilder.getterMethodHandles.putIfAbsent(
                    Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4),
                    bridgePainlessMethod.methodHandle()
                );
            } else if (typeParametersSize == 0
                && methodName.startsWith("is")
                && methodName.length() > 2
                && Character.isUpperCase(methodName.charAt(2))) {
                    painlessClassBuilder.getterMethodHandles.putIfAbsent(
                        Character.toLowerCase(methodName.charAt(2)) + methodName.substring(3),
                        bridgePainlessMethod.methodHandle()
                    );
                } else if (typeParametersSize == 1
                    && methodName.startsWith("set")
                    && methodName.length() > 3
                    && Character.isUpperCase(methodName.charAt(3))) {
                        painlessClassBuilder.setterMethodHandles.putIfAbsent(
                            Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4),
                            bridgePainlessMethod.methodHandle()
                        );
                    }
        }

        for (PainlessField painlessField : painlessClassBuilder.fields.values()) {
            painlessClassBuilder.getterMethodHandles.put(painlessField.javaField().getName().intern(), painlessField.getterMethodHandle());
            painlessClassBuilder.setterMethodHandles.put(painlessField.javaField().getName().intern(), painlessField.setterMethodHandle());
        }
    }
}

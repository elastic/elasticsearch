/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import org.elasticsearch.foreign.DefaultMethodHandleResolver;
import org.elasticsearch.foreign.DefaultSymbolResolver;
import org.elasticsearch.foreign.LibrarySpecification;
import org.elasticsearch.foreign.MethodHandleResolver;
import org.elasticsearch.foreign.SymbolResolver;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic.Kind;

/**
 * Models a {@code @LibrarySpecification}-annotated interface or abstract class and the methods
 * that will be bound to native symbols. The supported surface is intentionally narrow: every
 * abstract method must be annotated with {@code @Function} or {@code @StructFactory}; parameter
 * types are limited to primitives and {@code MemorySegment}; return types may also be
 * {@code String}.
 *
 * @param qualifiedName the fully-qualified interface or class name
 * @param simpleName the simple interface or class name
 * @param packageName the package name (may be empty)
 * @param libraryName the native library name from {@code @LibrarySpecification.name()} (may be empty)
 * @param methods all native methods in declaration order
 * @param unavailableOn enum constant names of platforms where this library is unavailable (empty means available everywhere)
 * @param structs all {@code @StructSpecification} types enclosed in this interface, in declaration order
 * @param symbolResolverClassName fully-qualified name of the {@link SymbolResolver} implementation
 *        (defaults to {@code org.elasticsearch.foreign.DefaultSymbolResolver})
 * @param methodHandleResolverClassName fully-qualified name of the {@link MethodHandleResolver} implementation
 *        (defaults to {@code org.elasticsearch.foreign.DefaultMethodHandleResolver})
 * @param isAbstractClass {@code true} when the base type is an abstract class rather than an interface
 */
public record LibraryModel(
    String qualifiedName,
    String simpleName,
    String packageName,
    String libraryName,
    List<MethodModel> methods,
    List<String> unavailableOn,
    List<StructModel> structs,
    String symbolResolverClassName,
    String methodHandleResolverClassName,
    boolean isAbstractClass
) {

    /** All known platform names — used to detect a library that can never be natively loaded. */
    private static final Set<String> ALL_PLATFORM_NAMES = Set.of(
        "LINUX_X64",
        "LINUX_AARCH64",
        "DARWIN_X64",
        "DARWIN_AARCH64",
        "WINDOWS_X64"
    );

    public static final String SYMBOL_RESOLVER_INTERFACE_FQN = SymbolResolver.class.getName();
    public static final String DEFAULT_SYMBOL_RESOLVER_FQN = DefaultSymbolResolver.class.getName();
    public static final String MH_RESOLVER_INTERFACE_FQN = MethodHandleResolver.class.getName();
    public static final String DEFAULT_MH_RESOLVER_FQN = DefaultMethodHandleResolver.class.getName();
    public static final String LIBRARY_SPECIFICATION_FQN = LibrarySpecification.class.getName();
    public static final String STRUCT_SPECIFICATION_FQN = org.elasticsearch.foreign.StructSpecification.class.getName();

    /** Fully-qualified name of the {@code $Impl} class generated for this library. */
    public String implQualifiedName() {
        return packageName.isEmpty() ? simpleName + "$Impl" : packageName + "." + simpleName + "$Impl";
    }

    /** Fully-qualified name of the {@code $Provider} class generated for this library. */
    public String providerQualifiedName() {
        return packageName.isEmpty() ? simpleName + "$Provider" : packageName + "." + simpleName + "$Provider";
    }

    /**
     * Builds a {@code LibraryModel} from a {@code @LibrarySpecification}-annotated interface or
     * abstract class element. Emits {@link Kind#ERROR} diagnostics via the messager for any
     * validation failure.
     *
     * @return the built model, or null if any error was emitted
     */
    public static LibraryModel from(TypeElement element, ProcessingEnvironment env) {
        Messager messager = env.getMessager();

        boolean isAbstractClass;
        if (element.getKind() == ElementKind.INTERFACE) {
            isAbstractClass = false;
        } else if (element.getKind() == ElementKind.CLASS && element.getModifiers().contains(Modifier.ABSTRACT)) {
            isAbstractClass = true;
        } else {
            messager.printMessage(Kind.ERROR, "@LibrarySpecification must be on an interface or abstract class", element);
            return null;
        }

        LibrarySpecification annotation = element.getAnnotation(LibrarySpecification.class);
        String libraryName = annotation != null ? annotation.name() : "";
        String qualifiedName = element.getQualifiedName().toString();
        String simpleName = element.getSimpleName().toString();
        String packageName = env.getElementUtils().getPackageOf(element).getQualifiedName().toString();

        AnnotationMirror specMirror = ModelUtil.findAnnotationMirror(element, LIBRARY_SPECIFICATION_FQN);
        List<String> unavailableOn = extractUnavailableOn(specMirror);

        boolean hasError = false;
        if (unavailableOn.containsAll(ALL_PLATFORM_NAMES)) {
            messager.printMessage(
                Kind.ERROR,
                "@LibrarySpecification.unavailableOn lists all known platforms; the library will never be natively loaded",
                element,
                specMirror
            );
            hasError = true;
        }

        String symbolResolverClassName = resolveAndValidateSymbolResolver(element, messager, env.getTypeUtils());
        if (symbolResolverClassName == null) {
            hasError = true;
        }

        String methodHandleResolverClassName = resolveAndValidateMethodHandleResolver(element, messager, env.getTypeUtils());
        if (methodHandleResolverClassName == null) {
            hasError = true;
        }

        if (isAbstractClass && hasCallableNoArgConstructor(element) == false) {
            messager.printMessage(Kind.ERROR, "@LibrarySpecification abstract class must have a callable no-arg constructor", element);
            hasError = true;
        }

        // First pass: collect struct specifications in declaration order
        List<StructModel> structs = new ArrayList<>();
        List<String> structSimpleNames = new ArrayList<>();
        for (var enclosed : element.getEnclosedElements()) {
            ElementKind kind = enclosed.getKind();
            boolean isType = kind == ElementKind.RECORD
                || kind == ElementKind.INTERFACE
                || kind == ElementKind.CLASS
                || kind == ElementKind.ENUM
                || kind == ElementKind.ANNOTATION_TYPE;
            if (isType == false) {
                continue;
            }
            TypeElement typeElement = (TypeElement) enclosed;
            AnnotationMirror structSpecMirror = ModelUtil.findAnnotationMirror(typeElement, STRUCT_SPECIFICATION_FQN);
            if (structSpecMirror == null) {
                continue;
            }
            if (kind != ElementKind.RECORD && kind != ElementKind.INTERFACE) {
                messager.printMessage(
                    Kind.ERROR,
                    "@StructSpecification is only allowed on a record or interface",
                    enclosed,
                    structSpecMirror
                );
                hasError = true;
                continue;
            }

            StructModel structModel = kind == ElementKind.RECORD
                ? StructSpecParser.fromRecord(typeElement, messager)
                : StructSpecParser.fromInterface(typeElement, structSimpleNames, env, messager);
            if (structModel == null) {
                hasError = true;
            } else {
                structs.add(structModel);
                structSimpleNames.add(structModel.simpleName());
            }
        }

        // Second pass: collect methods (skipping struct declarations)
        List<MethodModel> methods = new ArrayList<>();
        for (var enclosed : element.getEnclosedElements()) {
            if (enclosed.getKind() != ElementKind.METHOD) {
                continue;
            }
            ExecutableElement method = (ExecutableElement) enclosed;
            if (isAbstractClass) {
                // For abstract classes, only process abstract methods; skip concrete, static, etc.
                if (method.getModifiers().contains(Modifier.ABSTRACT) == false) {
                    continue;
                }
            } else {
                // For interfaces, skip default and static methods
                if (method.getModifiers().contains(Modifier.DEFAULT) || method.getModifiers().contains(Modifier.STATIC)) {
                    continue;
                }
            }

            MethodModel methodModel = MethodModel.from(method, env, structSimpleNames);
            if (methodModel == null) {
                hasError = true;
            } else {
                methods.add(methodModel);
            }
        }

        return hasError
            ? null
            : new LibraryModel(
                qualifiedName,
                simpleName,
                packageName,
                libraryName,
                methods,
                unavailableOn,
                structs,
                symbolResolverClassName,
                methodHandleResolverClassName,
                isAbstractClass
            );
    }

    /**
     * Resolves and validates the {@code symbolResolver} attribute from {@link LibrarySpecification}.
     * Returns the default ({@link DefaultSymbolResolver}) when no custom resolver is specified.
     * The resolver class must implement {@link SymbolResolver} and have a public no-arg constructor.
     *
     * @return the resolver's fully-qualified name (never null on success), or {@code null} if validation failed
     *         (error already emitted).
     */
    private static String resolveAndValidateSymbolResolver(TypeElement element, Messager messager, Types types) {
        AnnotationMirror specMirror = ModelUtil.findAnnotationMirror(element, LIBRARY_SPECIFICATION_FQN);
        if (specMirror == null) {
            return DEFAULT_SYMBOL_RESOLVER_FQN;
        }

        TypeMirror resolverTypeMirror = ModelUtil.annotationClassValue(specMirror, "symbolResolver");
        if (resolverTypeMirror == null) {
            return DEFAULT_SYMBOL_RESOLVER_FQN;
        }

        TypeElement resolverElement = types.asElement(resolverTypeMirror) instanceof TypeElement te ? te : null;
        if (resolverElement == null) {
            messager.printMessage(Kind.ERROR, "symbolResolver must reference a class", element, specMirror);
            return null;
        }

        // Use the JVM binary name (e.g. "pkg.Enclosing$Nested" for nested classes), not the
        // dot-separated qualified name, since the generator emits this into bytecode.
        String resolverFqn = binaryName(resolverElement);

        if (resolverFqn.equals(DEFAULT_SYMBOL_RESOLVER_FQN)) {
            return DEFAULT_SYMBOL_RESOLVER_FQN;
        }

        TypeElement resolverInterface = findTypeElement(resolverElement, SYMBOL_RESOLVER_INTERFACE_FQN);
        if (resolverInterface == null) {
            messager.printMessage(
                Kind.ERROR,
                "symbolResolver class [" + resolverFqn + "] must implement [" + SYMBOL_RESOLVER_INTERFACE_FQN + "]",
                element,
                specMirror
            );
            return null;
        }

        if (hasPublicNoArgConstructor(resolverElement) == false) {
            messager.printMessage(
                Kind.ERROR,
                "symbolResolver class [" + resolverFqn + "] must have a public no-arg constructor",
                element,
                specMirror
            );
            return null;
        }

        return resolverFqn;
    }

    /**
     * Resolves and validates the {@code methodHandleResolver} attribute from {@link LibrarySpecification}.
     * Returns the default ({@link DefaultMethodHandleResolver}) when no custom resolver is specified.
     * The resolver class must implement {@link MethodHandleResolver} and have a public no-arg constructor.
     *
     * @return the resolver's fully-qualified name (never null on success), or {@code null} if validation failed
     *         (error already emitted).
     */
    private static String resolveAndValidateMethodHandleResolver(TypeElement element, Messager messager, Types types) {
        AnnotationMirror specMirror = ModelUtil.findAnnotationMirror(element, LIBRARY_SPECIFICATION_FQN);
        TypeMirror resolverTypeMirror = ModelUtil.annotationClassValue(specMirror, "methodHandleResolver");
        if (resolverTypeMirror == null) {
            return DEFAULT_MH_RESOLVER_FQN;
        }

        TypeElement resolverElement = types.asElement(resolverTypeMirror) instanceof TypeElement te ? te : null;
        if (resolverElement == null) {
            messager.printMessage(Kind.ERROR, "methodHandleResolver must reference a class", element, specMirror);
            return null;
        }

        // Use the JVM binary name (e.g. "pkg.Enclosing$Nested" for nested classes), not the
        // dot-separated qualified name, since the generator emits this into bytecode.
        String resolverFqn = binaryName(resolverElement);

        if (resolverFqn.equals(DEFAULT_MH_RESOLVER_FQN)) {
            return DEFAULT_MH_RESOLVER_FQN;
        }

        TypeElement resolverInterface = findTypeElement(resolverElement, MH_RESOLVER_INTERFACE_FQN);
        if (resolverInterface == null) {
            messager.printMessage(
                Kind.ERROR,
                "methodHandleResolver class [" + resolverFqn + "] must implement [" + MH_RESOLVER_INTERFACE_FQN + "]",
                element,
                specMirror
            );
            return null;
        }

        if (hasPublicNoArgConstructor(resolverElement) == false) {
            messager.printMessage(
                Kind.ERROR,
                "methodHandleResolver class [" + resolverFqn + "] must have a public no-arg constructor",
                element,
                specMirror
            );
            return null;
        }

        return resolverFqn;
    }

    /** Checks whether the given type implements (directly or transitively) the interface with the given FQN. */
    private static TypeElement findTypeElement(TypeElement type, String interfaceFqn) {
        for (TypeMirror iface : type.getInterfaces()) {
            if (iface.getKind() != TypeKind.DECLARED) {
                continue;
            }
            TypeElement ifaceElement = (TypeElement) ((DeclaredType) iface).asElement();
            if (ifaceElement.getQualifiedName().contentEquals(interfaceFqn)) {
                return ifaceElement;
            }
            TypeElement found = findTypeElement(ifaceElement, interfaceFqn);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /**
     * Returns the JVM binary name for a type element — e.g. {@code "pkg.Outer$Inner"} for a nested
     * class. {@link TypeElement#getQualifiedName()} uses a dot between the enclosing type and the
     * nested type, which is wrong when the name gets baked into bytecode.
     */
    private static String binaryName(TypeElement type) {
        StringBuilder name = new StringBuilder(type.getSimpleName());
        var enclosing = type.getEnclosingElement();
        while (enclosing instanceof TypeElement enclosingType) {
            name.insert(0, enclosingType.getSimpleName() + "$");
            enclosing = enclosingType.getEnclosingElement();
        }
        if (enclosing instanceof javax.lang.model.element.PackageElement pkg && pkg.isUnnamed() == false) {
            name.insert(0, pkg.getQualifiedName() + ".");
        }
        return name.toString();
    }

    private static boolean hasPublicNoArgConstructor(TypeElement type) {
        for (var enclosed : type.getEnclosedElements()) {
            if (enclosed.getKind() != ElementKind.CONSTRUCTOR) {
                continue;
            }
            ExecutableElement ctor = (ExecutableElement) enclosed;
            if (ctor.getParameters().isEmpty() && ctor.getModifiers().contains(Modifier.PUBLIC)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns {@code true} if the generated {@code $Impl} subclass can call {@code super()} on this
     * type — i.e. the type has a non-{@code private} no-arg constructor (public, protected, or
     * package-private). When no explicit constructors are declared, Java provides an implicit
     * {@code public} no-arg constructor — the annotation processor source model exposes no element
     * for it, so an empty constructor list is treated as having an implicit public no-arg constructor.
     */
    private static boolean hasCallableNoArgConstructor(TypeElement type) {
        boolean foundAnyConstructor = false;
        for (var enclosed : type.getEnclosedElements()) {
            if (enclosed.getKind() != ElementKind.CONSTRUCTOR) {
                continue;
            }
            foundAnyConstructor = true;
            ExecutableElement ctor = (ExecutableElement) enclosed;
            if (ctor.getParameters().isEmpty() && ctor.getModifiers().contains(Modifier.PRIVATE) == false) {
                return true;
            }
        }
        // No explicit constructors → Java provides an implicit public no-arg constructor.
        return foundAnyConstructor == false;
    }

    /**
     * Extracts the {@code unavailableOn} attribute from the {@code @LibrarySpecification} annotation mirror
     * as a list of enum constant names. Uses annotation mirror APIs to avoid loading the {@code Platform}
     * class at processing time. Pure extraction — validation is the caller's responsibility.
     */
    private static List<String> extractUnavailableOn(AnnotationMirror specMirror) {
        if (specMirror == null) {
            return List.of();
        }

        for (var entry : specMirror.getElementValues().entrySet()) {
            if (entry.getKey().getSimpleName().contentEquals("unavailableOn") == false) {
                continue;
            }
            Object rawValue = entry.getValue().getValue();
            if ((rawValue instanceof List<?>) == false) {
                return List.of();
            }
            List<?> valueList = (List<?>) rawValue;
            List<String> platformNames = new ArrayList<>();
            for (Object item : valueList) {
                if (item instanceof AnnotationValue av && av.getValue() instanceof VariableElement ve) {
                    platformNames.add(ve.getSimpleName().toString());
                }
            }
            return List.copyOf(platformNames);
        }
        return List.of();
    }

}

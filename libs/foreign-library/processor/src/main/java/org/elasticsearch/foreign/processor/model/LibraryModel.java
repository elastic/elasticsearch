/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import org.elasticsearch.foreign.LibrarySpecification;

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
import javax.tools.Diagnostic.Kind;

/**
 * Models a {@code @LibrarySpecification}-annotated interface and the methods that will be bound to
 * native symbols. The supported surface is intentionally narrow: every abstract method must be
 * annotated with {@code @Function}; parameter types are limited to primitives and
 * {@code MemorySegment}; return types may also be {@code String}.
 *
 * @param qualifiedName the fully-qualified interface name
 * @param simpleName the simple interface name
 * @param packageName the package name (may be empty)
 * @param libraryName the native library name from {@code @LibrarySpecification.name()} (may be empty)
 * @param methods all native methods in declaration order
 * @param unavailableOn enum constant names of platforms where this library is unavailable (empty means available everywhere)
 */
public record LibraryModel(
    String qualifiedName,
    String simpleName,
    String packageName,
    String libraryName,
    List<MethodModel> methods,
    List<String> unavailableOn
) {

    /** All known platform names — used to detect a library that can never be natively loaded. */
    private static final Set<String> ALL_PLATFORM_NAMES = Set.of(
        "LINUX_X64",
        "LINUX_AARCH64",
        "DARWIN_X64",
        "DARWIN_AARCH64",
        "WINDOWS_X64"
    );

    /** Fully-qualified name of the {@code $Impl} class generated for this library. */
    public String implQualifiedName() {
        return packageName.isEmpty() ? simpleName + "$Impl" : packageName + "." + simpleName + "$Impl";
    }

    /** Fully-qualified name of the {@code $Provider} class generated for this library. */
    public String providerQualifiedName() {
        return packageName.isEmpty() ? simpleName + "$Provider" : packageName + "." + simpleName + "$Provider";
    }

    /**
     * Builds a {@code LibraryModel} from a {@code @LibrarySpecification}-annotated interface element.
     * Emits {@link Kind#ERROR} diagnostics via the messager for any validation failure.
     *
     * @return the built model, or null if any error was emitted
     */
    public static LibraryModel from(TypeElement element, ProcessingEnvironment env) {
        Messager messager = env.getMessager();

        if (element.getKind() != ElementKind.INTERFACE) {
            messager.printMessage(Kind.ERROR, "@LibrarySpecification must be on an interface", element);
            return null;
        }

        LibrarySpecification annotation = element.getAnnotation(LibrarySpecification.class);
        String libraryName = annotation != null ? annotation.name() : "";
        String qualifiedName = element.getQualifiedName().toString();
        String simpleName = element.getSimpleName().toString();
        String packageName = env.getElementUtils().getPackageOf(element).getQualifiedName().toString();

        AnnotationMirror specMirror = findAnnotationMirror(element, "org.elasticsearch.foreign.LibrarySpecification");
        List<String> unavailableOn = extractUnavailableOn(specMirror);

        if (unavailableOn.containsAll(ALL_PLATFORM_NAMES)) {
            messager.printMessage(
                Kind.ERROR,
                "@LibrarySpecification.unavailableOn lists all known platforms; the library will never be natively loaded",
                element,
                specMirror
            );
            return null;
        }

        List<MethodModel> methods = new ArrayList<>();
        boolean hasError = false;
        for (var enclosed : element.getEnclosedElements()) {
            if (enclosed.getKind() != ElementKind.METHOD) {
                continue;
            }
            ExecutableElement method = (ExecutableElement) enclosed;
            if (method.getModifiers().contains(Modifier.DEFAULT) || method.getModifiers().contains(Modifier.STATIC)) {
                continue;
            }

            MethodModel methodModel = MethodModel.from(method, env);
            if (methodModel == null) {
                hasError = true;
            } else {
                methods.add(methodModel);
            }
        }

        return hasError ? null : new LibraryModel(qualifiedName, simpleName, packageName, libraryName, methods, unavailableOn);
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

    private static AnnotationMirror findAnnotationMirror(TypeElement element, String annotationFqn) {
        for (AnnotationMirror mirror : element.getAnnotationMirrors()) {
            TypeElement annotationType = (TypeElement) mirror.getAnnotationType().asElement();
            if (annotationType.getQualifiedName().contentEquals(annotationFqn)) {
                return mirror;
            }
        }
        return null;
    }
}

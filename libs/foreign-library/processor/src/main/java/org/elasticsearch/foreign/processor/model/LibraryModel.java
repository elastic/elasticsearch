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

import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
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
 */
public record LibraryModel(String qualifiedName, String simpleName, String packageName, String libraryName, List<MethodModel> methods) {

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

        return hasError ? null : new LibraryModel(qualifiedName, simpleName, packageName, libraryName, methods);
    }
}

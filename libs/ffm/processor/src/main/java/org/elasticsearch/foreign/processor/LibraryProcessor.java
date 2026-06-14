/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import org.elasticsearch.foreign.LibrarySpecification;
import org.elasticsearch.foreign.processor.model.LibraryModel;
import org.elasticsearch.foreign.processor.model.StructModel;

import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedOptions;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;

@SupportedAnnotationTypes("org.elasticsearch.foreign.LibrarySpecification")
@SupportedOptions(LibraryProcessor.OPTION_JAVA_VERSION)
public class LibraryProcessor extends AbstractProcessor {

    /** Processor option: the minimum Java release the generated classes must run on (e.g. {@code 21}). */
    static final String OPTION_JAVA_VERSION = "javaVersion";

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_21;
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (annotations.isEmpty()) {
            return false;
        }
        Filer filer = processingEnv.getFiler();
        int classFileVersion = resolveClassFileVersion();
        if (classFileVersion < 0) {
            return false;
        }
        ImplClassWriter implGenerator = new ImplClassWriter(filer, classFileVersion);
        ProviderClassWriter providerWriter = new ProviderClassWriter(filer, classFileVersion);
        StructClassWriter structWriter = new StructClassWriter(filer, classFileVersion);
        for (Element element : roundEnv.getElementsAnnotatedWith(LibrarySpecification.class)) {
            if (element.getKind() != ElementKind.INTERFACE) {
                processingEnv.getMessager().printMessage(Kind.ERROR, "@LibrarySpecification must be on an interface", element);
                continue;
            }
            LibraryModel model = LibraryModel.from((TypeElement) element, processingEnv);
            if (model != null) {
                TypeElement typeElement = (TypeElement) element;
                try {
                    implGenerator.generate(model, typeElement);
                    providerWriter.generate(model, typeElement);
                    for (StructModel struct : model.structs()) {
                        structWriter.generate(model, struct, typeElement);
                    }
                } catch (Exception e) {
                    processingEnv.getMessager()
                        .printMessage(Kind.ERROR, "Failed to generate class for " + model.qualifiedName() + ": " + e.getMessage(), element);
                }
            }
        }
        return true;
    }

    private int resolveClassFileVersion() {
        String option = processingEnv.getOptions().getOrDefault(OPTION_JAVA_VERSION, "21");
        try {
            int javaRelease = Integer.parseInt(option);
            if (javaRelease < 21) {
                processingEnv.getMessager().printMessage(Kind.ERROR, OPTION_JAVA_VERSION + " must be >= 21, got: " + option);
                return -1;
            }
            return ClassWriterUtil.classFileVersion(javaRelease);
        } catch (NumberFormatException e) {
            processingEnv.getMessager().printMessage(Kind.ERROR, OPTION_JAVA_VERSION + " must be an integer, got: " + option);
            return -1;
        }
    }
}

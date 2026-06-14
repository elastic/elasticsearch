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
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;

@SupportedAnnotationTypes("org.elasticsearch.foreign.LibrarySpecification")
public class LibraryProcessor extends AbstractProcessor {

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
        ImplClassWriter implGenerator = new ImplClassWriter(filer);
        ProviderClassWriter providerWriter = new ProviderClassWriter(filer);
        StructClassWriter structWriter = new StructClassWriter(filer);
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
}

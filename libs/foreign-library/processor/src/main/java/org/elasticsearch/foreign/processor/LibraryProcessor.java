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

import java.io.IOException;
import java.io.Writer;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedOptions;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;
import javax.tools.StandardLocation;

@SupportedAnnotationTypes("org.elasticsearch.foreign.LibrarySpecification")
@SupportedOptions(LibraryProcessor.OPTION_JAVA_VERSION)
public class LibraryProcessor extends AbstractProcessor {

    /** Processor option: the minimum Java release the generated classes must run on (e.g. {@code 21}). */
    static final String OPTION_JAVA_VERSION = "javaVersion";

    private static final String LIBRARY_PROVIDER_FQN = "org.elasticsearch.foreign.LibraryProvider";
    private static final String LIBRARY_PROVIDER_SERVICE = "META-INF/services/" + LIBRARY_PROVIDER_FQN;

    // LinkedHashSet so the services file output is deterministic across rounds.
    private final Set<String> generatedProviderNames = new LinkedHashSet<>();

    @Override
    public SourceVersion getSupportedSourceVersion() {
        // We only inspect annotations, not language constructs, so we can claim support for whatever
        // source version the toolchain compiles us against. The minimum bytecode target for *generated*
        // classes is controlled separately by the -AjavaVersion processor option.
        return SourceVersion.latestSupported();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (roundEnv.processingOver()) {
            writeServicesFile();
            return false;
        }
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
        for (Element element : roundEnv.getElementsAnnotatedWith(LibrarySpecification.class)) {
            LibraryModel model = LibraryModel.from((TypeElement) element, processingEnv);
            if (model == null) {
                continue;
            }
            try {
                implGenerator.generate(model, (TypeElement) element);
                providerWriter.generate(model, (TypeElement) element);
                generatedProviderNames.add(model.providerQualifiedName());
            } catch (Exception e) {
                processingEnv.getMessager()
                    .printMessage(Kind.ERROR, "Failed to generate class for " + model.qualifiedName() + ": " + e.getMessage(), element);
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

    /**
     * Writes the {@code META-INF/services/org.elasticsearch.foreign.LibraryProvider} file listing every
     * {@code $Provider} generated in this compilation. This file is consumed by ServiceLoader on the
     * classpath (e.g. by tests). For modular consumers, the generated providers are added to the
     * module's {@code Provides} attribute post-compilation by {@link ModuleInfoAugmenter}.
     */
    private void writeServicesFile() {
        if (generatedProviderNames.isEmpty()) {
            return;
        }
        try {
            var fileObject = processingEnv.getFiler().createResource(StandardLocation.CLASS_OUTPUT, "", LIBRARY_PROVIDER_SERVICE);
            try (Writer writer = fileObject.openWriter()) {
                for (String name : generatedProviderNames) {
                    writer.write(name);
                    writer.write('\n');
                }
            }
        } catch (IOException e) {
            processingEnv.getMessager().printMessage(Kind.ERROR, "Failed to write " + LIBRARY_PROVIDER_SERVICE + ": " + e.getMessage());
        }
    }
}

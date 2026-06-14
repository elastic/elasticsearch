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

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedOptions;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ModuleElement;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;
import javax.tools.StandardLocation;

import com.sun.source.tree.ModuleTree;
import com.sun.source.tree.ProvidesTree;
import com.sun.source.util.TreePath;
import com.sun.source.util.Trees;

@SupportedAnnotationTypes("org.elasticsearch.foreign.LibrarySpecification")
@SupportedOptions(LibraryProcessor.OPTION_JAVA_VERSION)
public class LibraryProcessor extends AbstractProcessor {

    /** Processor option: the minimum Java release the generated classes must run on (e.g. {@code 21}). */
    static final String OPTION_JAVA_VERSION = "javaVersion";

    private static final String LIBRARY_PROVIDER_FQN = "org.elasticsearch.foreign.LibraryProvider";
    private static final String LIBRARY_PROVIDER_SERVICE = "META-INF/services/" + LIBRARY_PROVIDER_FQN;

    private final List<String> generatedProviderNames = new ArrayList<>();

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_21;
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
                    String providerFqn = model.qualifiedName() + "$Provider";
                    generatedProviderNames.add(providerFqn);
                    checkModuleProvides(typeElement, providerFqn);
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

    /**
     * Verifies that the module containing the annotated interface declares a
     * {@code provides org.elasticsearch.foreign.LibraryProvider with <providerFqn>} directive in its
     * {@code module-info.java}. The JDK requires this directive for service discovery in named modules
     * (META-INF/services is not consulted there). Skipped for the unnamed module.
     */
    private void checkModuleProvides(TypeElement element, String providerFqn) {
        ModuleElement module = processingEnv.getElementUtils().getModuleOf(element);
        if (module == null || module.isUnnamed()) {
            return;
        }

        // Use the Compiler Tree API: provides directives that reference yet-to-be-generated
        // implementation classes are dropped from ModuleElement.getDirectives() (the compiler can't
        // resolve them during annotation processing), but they remain in the parsed source tree.
        Trees trees = Trees.instance(processingEnv);
        TreePath modulePath = trees.getPath(module);
        if (modulePath == null) {
            return;
        }
        ModuleTree moduleTree = (ModuleTree) modulePath.getLeaf();

        boolean libraryProviderDirectiveExists = false;
        for (var directive : moduleTree.getDirectives()) {
            if (directive instanceof ProvidesTree provides) {
                if (provides.getServiceName().toString().equals(LIBRARY_PROVIDER_FQN) == false) {
                    continue;
                }
                libraryProviderDirectiveExists = true;
                for (var impl : provides.getImplementationNames()) {
                    if (impl.toString().equals(providerFqn)) {
                        return;
                    }
                }
            }
        }

        String message;
        if (libraryProviderDirectiveExists) {
            message = String.format(
                """
                    Generated provider class %s is not listed in the existing `provides %s` directive in module-info.java for module %s.
                    Add %s to that directive's `with` clause:

                        provides %s with ..., %s;""",
                providerFqn,
                LIBRARY_PROVIDER_FQN,
                module.getQualifiedName(),
                providerFqn,
                LIBRARY_PROVIDER_FQN,
                providerFqn
            );
        } else {
            message = String.format(
                """
                    Generated provider class %s is not declared in module-info.java for module %s.
                    Add the following directive to module-info.java:

                        provides %s with %s;""",
                providerFqn,
                module.getQualifiedName(),
                LIBRARY_PROVIDER_FQN,
                providerFqn
            );
        }
        processingEnv.getMessager().printMessage(Kind.ERROR, message, element);
    }

    /**
     * Writes the {@code META-INF/services/org.elasticsearch.foreign.LibraryProvider} file listing every
     * {@code $Provider} generated in this compilation. Required for non-modular consumers (e.g. tests
     * running on the classpath); modular consumers still need a {@code provides} directive in
     * {@code module-info.java} because the JDK requires it for named modules.
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import org.elasticsearch.core.UpdateForV10;

import java.util.Set;

import javax.annotation.processing.Completion;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;

/**
 * A no-op annotation processor that claims marker annotations like {@code @UpdateForV10}.
 * <p>
 * These annotations are documentation-only markers (with {@code @Retention(SOURCE)}) used to
 * track code that needs cleanup in future versions. Since the ESQL module uses annotation
 * processors for code generation, the compiler warns about unclaimed annotations. This
 * processor claims them to suppress those warnings.
 */
public class MarkerAnnotationProcessor implements Processor {

    @Override
    public Set<String> getSupportedOptions() {
        return Set.of();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        // Marker annotations that are documentation-only and don't require processing.
        return Set.of(UpdateForV10.class.getCanonicalName());
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_21;
    }

    @Override
    public void init(ProcessingEnvironment processingEnvironment) {}

    @Override
    public Iterable<? extends Completion> getCompletions(
        Element element,
        AnnotationMirror annotationMirror,
        ExecutableElement executableElement,
        String s
    ) {
        return Set.of();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnvironment) {
        return true;
    }
}

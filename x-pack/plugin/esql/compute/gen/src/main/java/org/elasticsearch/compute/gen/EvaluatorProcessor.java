/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.MvEvaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.processing.Completion;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;

/**
 * Glues the {@link EvaluatorImplementer} into the jdk's annotation
 * processing framework.
 */
public class EvaluatorProcessor implements Processor {
    private ProcessingEnvironment env;

    @Override
    public Set<String> getSupportedOptions() {
        return Set.of();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Set.of(Evaluator.class.getName(), MvEvaluator.class.getName(), ConvertEvaluator.class.getName());
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_17;
    }

    @Override
    public void init(ProcessingEnvironment processingEnvironment) {
        this.env = processingEnvironment;
    }

    @Override
    public Iterable<? extends Completion> getCompletions(
        Element element,
        AnnotationMirror annotationMirror,
        ExecutableElement executableElement,
        String s
    ) {
        return List.of();
    }

    @Override
    public boolean process(Set<? extends TypeElement> set, RoundEnvironment roundEnvironment) {
        for (TypeElement ann : set) {
            for (Element evaluatorMethod : roundEnvironment.getElementsAnnotatedWith(ann)) {
                Evaluator evaluatorAnn = evaluatorMethod.getAnnotation(Evaluator.class);
                if (evaluatorAnn != null) {
                    try {
                        AggregatorProcessor.write(
                            evaluatorMethod,
                            "evaluator",
                            new EvaluatorImplementer(
                                env.getElementUtils(),
                                env.getTypeUtils(),
                                (ExecutableElement) evaluatorMethod,
                                evaluatorAnn.extraName(),
                                warnExceptions(evaluatorMethod)
                            ).sourceFile(),
                            env
                        );
                    } catch (Exception e) {
                        env.getMessager().printMessage(Diagnostic.Kind.ERROR, "failed to build " + evaluatorMethod.getEnclosingElement());
                        throw e;
                    }
                }
                MvEvaluator mvEvaluatorAnn = evaluatorMethod.getAnnotation(MvEvaluator.class);
                if (mvEvaluatorAnn != null) {
                    try {
                        AggregatorProcessor.write(
                            evaluatorMethod,
                            "evaluator",
                            new MvEvaluatorImplementer(
                                env.getElementUtils(),
                                (ExecutableElement) evaluatorMethod,
                                mvEvaluatorAnn.extraName(),
                                mvEvaluatorAnn.finish(),
                                mvEvaluatorAnn.single(),
                                mvEvaluatorAnn.ascending(),
                                warnExceptions(evaluatorMethod)
                            ).sourceFile(),
                            env
                        );
                    } catch (Exception e) {
                        env.getMessager().printMessage(Diagnostic.Kind.ERROR, "failed to build " + evaluatorMethod.getEnclosingElement());
                        throw e;
                    }
                }
                ConvertEvaluator convertEvaluatorAnn = evaluatorMethod.getAnnotation(ConvertEvaluator.class);
                if (convertEvaluatorAnn != null) {
                    try {
                        AggregatorProcessor.write(
                            evaluatorMethod,
                            "evaluator",
                            new ConvertEvaluatorImplementer(
                                env.getElementUtils(),
                                (ExecutableElement) evaluatorMethod,
                                convertEvaluatorAnn.extraName(),
                                warnExceptions(evaluatorMethod)
                            ).sourceFile(),
                            env
                        );
                    } catch (Exception e) {
                        env.getMessager().printMessage(Diagnostic.Kind.ERROR, "failed to build " + evaluatorMethod.getEnclosingElement());
                        throw e;
                    }
                }
            }
        }
        return true;
    }

    private static List<TypeMirror> warnExceptions(Element evaluatorMethod) {
        List<TypeMirror> result = new ArrayList<>();
        for (var mirror : evaluatorMethod.getAnnotationMirrors()) {
            String annotationType = mirror.getAnnotationType().toString();
            if (annotationType.equals(Evaluator.class.getName())
                || annotationType.equals(MvEvaluator.class.getName())
                || annotationType.equals(ConvertEvaluator.class.getName())) {

                for (var e : mirror.getElementValues().entrySet()) {
                    if (false == e.getKey().getSimpleName().toString().equals("warnExceptions")) {
                        continue;
                    }
                    for (var v : (List<?>) e.getValue().getValue()) {
                        result.add((TypeMirror) ((AnnotationValue) v).getValue());
                    }
                }
            }
        }
        return result;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.runtime;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Cache for runtime-generated evaluator classes.
 * <p>
 * Each unique evaluator specification maps to a single generated class.
 * Classes are generated once and cached for the lifetime of the application.
 * </p>
 * <p>
 * Error handling: If bytecode generation or class loading fails, the error is
 * logged and wrapped in a {@link RuntimeEvaluatorException} to prevent node crashes.
 * </p>
 */
public final class EvaluatorCache {

    private static final Logger logger = LogManager.getLogger(EvaluatorCache.class);

    private final ConcurrentHashMap<EvaluatorSpec, Class<? extends ExpressionEvaluator>> cache;
    private final ConcurrentHashMap<EvaluatorSpec, RuntimeEvaluatorException> failedSpecs;

    public EvaluatorCache() {
        this.cache = new ConcurrentHashMap<>();
        this.failedSpecs = new ConcurrentHashMap<>();
    }

    /**
     * Gets a cached evaluator class or generates and caches a new one.
     *
     * @param spec the evaluator specification (used as cache key)
     * @param generator function to generate bytecode if not cached
     * @param loader the class loader to define new classes
     * @return the evaluator class (either cached or newly generated)
     * @throws RuntimeEvaluatorException if bytecode generation or class loading fails
     */
    public Class<? extends ExpressionEvaluator> getOrGenerate(
        EvaluatorSpec spec,
        Function<EvaluatorSpec, byte[]> generator,
        RuntimeClassLoader loader
    ) {
        RuntimeEvaluatorException previousFailure = failedSpecs.get(spec);
        if (previousFailure != null) {
            logger.debug("Returning cached failure for evaluator: {}", spec.evaluatorSimpleName());
            throw previousFailure;
        }

        try {
            return cache.computeIfAbsent(spec, s -> {
                try {
                    logger.debug("Generating evaluator bytecode for: {}", s.evaluatorSimpleName());
                    byte[] bytecode = generator.apply(s);
                    logger.debug("Defining evaluator class: {} ({} bytes)", s.evaluatorClassName(), bytecode.length);
                    return loader.defineEvaluator(s.evaluatorClassName(), bytecode);
                } catch (VerifyError e) {
                    throw new RuntimeEvaluatorException(
                        "Bytecode verification failed for evaluator "
                            + s.evaluatorSimpleName()
                            + ". This is a bug in the runtime generator. Method: "
                            + s.processMethodName()
                            + ", Declaring class: "
                            + s.declaringClass().getName(),
                        e
                    );
                } catch (ClassFormatError e) {
                    throw new RuntimeEvaluatorException(
                        "Invalid bytecode format for evaluator "
                            + s.evaluatorSimpleName()
                            + ". This is a bug in the runtime generator. Method: "
                            + s.processMethodName()
                            + ", Declaring class: "
                            + s.declaringClass().getName(),
                        e
                    );
                } catch (LinkageError e) {
                    throw new RuntimeEvaluatorException(
                        "Class linking failed for evaluator "
                            + s.evaluatorSimpleName()
                            + ". Check that all required classes are available. Method: "
                            + s.processMethodName()
                            + ", Declaring class: "
                            + s.declaringClass().getName(),
                        e
                    );
                } catch (RuntimeEvaluatorException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeEvaluatorException(
                        "Failed to generate evaluator "
                            + s.evaluatorSimpleName()
                            + ". Method: "
                            + s.processMethodName()
                            + ", Declaring class: "
                            + s.declaringClass().getName(),
                        e
                    );
                }
            });
        } catch (RuntimeEvaluatorException e) {
            logger.error("Failed to generate/load evaluator for {}: {}", spec.evaluatorSimpleName(), e.getMessage());
            failedSpecs.put(spec, e);
            throw e;
        }
    }

    /**
     * Returns the number of cached evaluator classes.
     */
    public int size() {
        return cache.size();
    }

    /**
     * Checks if an evaluator is already cached.
     */
    public boolean contains(EvaluatorSpec spec) {
        return cache.containsKey(spec);
    }

    /**
     * Clears the cache. Primarily for testing.
     */
    public void clear() {
        cache.clear();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.runtime;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.core.SuppressForbidden;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Cache for runtime-generated aggregator classes.
 * <p>
 * Each unique aggregator specification maps to a single generated class.
 * Classes are generated once and cached for the lifetime of the application.
 * </p>
 * <p>
 * Error handling: If bytecode generation or class loading fails, the error is
 * logged and wrapped in a {@link RuntimeEvaluatorException} to prevent node crashes.
 * </p>
 * <p>
 * Class loading: Uses {@link RuntimeClassLoader} with the plugin's classloader as parent,
 * which allows the generated classes to access both the user's aggregator class and
 * the compute module's public state classes.
 * </p>
 */
public final class AggregatorCache {

    private static final Logger logger = LogManager.getLogger(AggregatorCache.class);

    private final ConcurrentHashMap<String, Class<? extends AggregatorFunction>> aggregatorCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Class<? extends GroupingAggregatorFunction>> groupingCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, RuntimeEvaluatorException> failedSpecs = new ConcurrentHashMap<>();
    private final ClassLoader parentClassLoader;

    public AggregatorCache(ClassLoader parentClassLoader) {
        this.parentClassLoader = parentClassLoader;
    }

    /**
     * Gets a cached aggregator class or generates and caches a new one.
     *
     * @param spec the aggregator specification (used as cache key)
     * @param generator function to generate bytecode if not cached
     * @return the aggregator class (either cached or newly generated)
     * @throws RuntimeEvaluatorException if bytecode generation or class loading fails
     */
    @SuppressWarnings("unchecked")
    public Class<? extends AggregatorFunction> getOrGenerateAggregator(AggregatorSpec spec, Function<AggregatorSpec, byte[]> generator) {
        String cacheKey = spec.aggregatorClassName();

        RuntimeEvaluatorException previousFailure = failedSpecs.get(cacheKey);
        if (previousFailure != null) {
            logger.debug("Returning cached failure for aggregator: {}", spec.aggregatorSimpleName());
            throw previousFailure;
        }

        try {
            return aggregatorCache.computeIfAbsent(cacheKey, k -> {
                try {
                    logger.debug("Generating aggregator bytecode for: {}", spec.aggregatorSimpleName());
                    byte[] bytecode = generator.apply(spec);
                    logger.debug("Defining aggregator class: {} ({} bytes)", spec.aggregatorClassName(), bytecode.length);
                    RuntimeClassLoader classLoader = new RuntimeClassLoader(parentClassLoader);
                    return (Class<? extends AggregatorFunction>) classLoader.defineClass(spec.aggregatorClassName(), bytecode);
                } catch (VerifyError e) {
                    throw new RuntimeEvaluatorException(
                        "Bytecode verification failed for aggregator "
                            + spec.aggregatorSimpleName()
                            + ". This is a bug in the runtime generator. Declaring class: "
                            + spec.declaringClass().getName(),
                        e
                    );
                } catch (ClassFormatError e) {
                    throw new RuntimeEvaluatorException(
                        "Invalid bytecode format for aggregator "
                            + spec.aggregatorSimpleName()
                            + ". This is a bug in the runtime generator. Declaring class: "
                            + spec.declaringClass().getName(),
                        e
                    );
                } catch (LinkageError e) {
                    throw new RuntimeEvaluatorException(
                        "Class linking failed for aggregator "
                            + spec.aggregatorSimpleName()
                            + ". Check that all required classes are available. Declaring class: "
                            + spec.declaringClass().getName(),
                        e
                    );
                } catch (RuntimeEvaluatorException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeEvaluatorException(
                        "Failed to generate aggregator "
                            + spec.aggregatorSimpleName()
                            + ". Declaring class: "
                            + spec.declaringClass().getName(),
                        e
                    );
                }
            });
        } catch (RuntimeEvaluatorException e) {
            logger.error("Failed to generate/load aggregator for {}: {}", spec.aggregatorSimpleName(), e.getMessage());
            failedSpecs.put(cacheKey, e);
            throw e;
        }
    }

    /**
     * Gets a cached grouping aggregator class or generates and caches a new one.
     * This method also generates and wires up the AddInput inner classes for optimal performance.
     *
     * @param spec the aggregator specification (used as cache key)
     * @param aggregatorGenerator function to generate aggregator bytecode
     * @param blockAddInputGenerator function to generate BlockAddInput bytecode (takes spec and aggregator class)
     * @param vectorAddInputGenerator function to generate VectorAddInput bytecode (takes spec and aggregator class)
     * @return the grouping aggregator class (either cached or newly generated)
     * @throws RuntimeEvaluatorException if bytecode generation or class loading fails
     */
    @SuppressWarnings("unchecked")
    public Class<? extends GroupingAggregatorFunction> getOrGenerateGroupingAggregatorWithAddInput(
        AggregatorSpec spec,
        Function<AggregatorSpec, byte[]> aggregatorGenerator,
        BiFunction<AggregatorSpec, Class<?>, byte[]> blockAddInputGenerator,
        BiFunction<AggregatorSpec, Class<?>, byte[]> vectorAddInputGenerator
    ) {
        String cacheKey = spec.groupingAggregatorClassName();

        RuntimeEvaluatorException previousFailure = failedSpecs.get(cacheKey);
        if (previousFailure != null) {
            logger.debug("Returning cached failure for grouping aggregator: {}", spec.groupingAggregatorSimpleName());
            throw previousFailure;
        }

        try {
            return groupingCache.computeIfAbsent(cacheKey, k -> {
                try {
                    // Create a single classloader for all related classes
                    RuntimeClassLoader classLoader = new RuntimeClassLoader(parentClassLoader);

                    // Step 1: Generate and define the aggregator class
                    logger.debug("Generating grouping aggregator bytecode for: {}", spec.groupingAggregatorSimpleName());
                    byte[] aggregatorBytecode = aggregatorGenerator.apply(spec);
                    logger.debug(
                        "Defining grouping aggregator class: {} ({} bytes)",
                        spec.groupingAggregatorClassName(),
                        aggregatorBytecode.length
                    );
                    Class<? extends GroupingAggregatorFunction> aggregatorClass = (Class<? extends GroupingAggregatorFunction>) classLoader
                        .defineClass(spec.groupingAggregatorClassName(), aggregatorBytecode);

                    // Step 2: Generate and define the BlockAddInput class
                    logger.debug("Generating BlockAddInput for: {}", spec.groupingAggregatorSimpleName());
                    byte[] blockAddInputBytecode = blockAddInputGenerator.apply(spec, aggregatorClass);
                    String blockAddInputClassName = spec.groupingAggregatorClassName() + "$BlockAddInput";
                    Class<?> blockAddInputClass = classLoader.defineClass(blockAddInputClassName, blockAddInputBytecode);

                    // Step 3: Generate and define the VectorAddInput class
                    logger.debug("Generating VectorAddInput for: {}", spec.groupingAggregatorSimpleName());
                    byte[] vectorAddInputBytecode = vectorAddInputGenerator.apply(spec, aggregatorClass);
                    String vectorAddInputClassName = spec.groupingAggregatorClassName() + "$VectorAddInput";
                    Class<?> vectorAddInputClass = classLoader.defineClass(vectorAddInputClassName, vectorAddInputBytecode);

                    // Step 4: Wire up the constructors in the aggregator's static fields
                    wireAddInputConstructors(spec, aggregatorClass, blockAddInputClass, vectorAddInputClass);

                    return aggregatorClass;
                } catch (VerifyError e) {
                    throw new RuntimeEvaluatorException(
                        "Bytecode verification failed for grouping aggregator "
                            + spec.groupingAggregatorSimpleName()
                            + ". This is a bug in the runtime generator. Declaring class: "
                            + spec.declaringClass().getName(),
                        e
                    );
                } catch (ClassFormatError e) {
                    throw new RuntimeEvaluatorException(
                        "Invalid bytecode format for grouping aggregator "
                            + spec.groupingAggregatorSimpleName()
                            + ". This is a bug in the runtime generator. Declaring class: "
                            + spec.declaringClass().getName(),
                        e
                    );
                } catch (LinkageError e) {
                    throw new RuntimeEvaluatorException(
                        "Class linking failed for grouping aggregator "
                            + spec.groupingAggregatorSimpleName()
                            + ". Check that all required classes are available. Declaring class: "
                            + spec.declaringClass().getName(),
                        e
                    );
                } catch (RuntimeEvaluatorException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeEvaluatorException(
                        "Failed to generate grouping aggregator "
                            + spec.groupingAggregatorSimpleName()
                            + ". Declaring class: "
                            + spec.declaringClass().getName(),
                        e
                    );
                }
            });
        } catch (RuntimeEvaluatorException e) {
            logger.error("Failed to generate/load grouping aggregator for {}: {}", spec.groupingAggregatorSimpleName(), e.getMessage());
            failedSpecs.put(cacheKey, e);
            throw e;
        }
    }

    /**
     * Wires up the AddInput constructors in the aggregator's static fields.
     */
    @SuppressForbidden(reason = "Runtime bytecode generation requires reflective access to set static constructor fields")
    private void wireAddInputConstructors(
        AggregatorSpec spec,
        Class<?> aggregatorClass,
        Class<?> blockAddInputClass,
        Class<?> vectorAddInputClass
    ) throws Exception {
        // Get the block type for this aggregator
        Class<?> blockType = getBlockType(spec.valueType());
        Class<?> vectorType = getVectorType(spec.valueType());

        // Get the constructors
        Constructor<?> blockCtor = blockAddInputClass.getConstructor(aggregatorClass, blockType);
        Constructor<?> vectorCtor = vectorAddInputClass.getConstructor(aggregatorClass, vectorType);

        // Set the static fields
        Field blockCtorField = aggregatorClass.getDeclaredField("BLOCK_ADD_INPUT_CTOR");
        blockCtorField.setAccessible(true);
        blockCtorField.set(null, blockCtor);

        Field vectorCtorField = aggregatorClass.getDeclaredField("VECTOR_ADD_INPUT_CTOR");
        vectorCtorField.setAccessible(true);
        vectorCtorField.set(null, vectorCtor);
    }

    private static Class<?> getBlockType(Class<?> valueType) {
        if (valueType == long.class) return org.elasticsearch.compute.data.LongBlock.class;
        if (valueType == int.class) return org.elasticsearch.compute.data.IntBlock.class;
        if (valueType == double.class) return org.elasticsearch.compute.data.DoubleBlock.class;
        if (valueType == boolean.class) return org.elasticsearch.compute.data.BooleanBlock.class;
        if (valueType == org.apache.lucene.util.BytesRef.class) return org.elasticsearch.compute.data.BytesRefBlock.class;
        throw new IllegalArgumentException("Unsupported value type: " + valueType);
    }

    private static Class<?> getVectorType(Class<?> valueType) {
        if (valueType == long.class) return org.elasticsearch.compute.data.LongVector.class;
        if (valueType == int.class) return org.elasticsearch.compute.data.IntVector.class;
        if (valueType == double.class) return org.elasticsearch.compute.data.DoubleVector.class;
        if (valueType == boolean.class) return org.elasticsearch.compute.data.BooleanVector.class;
        if (valueType == org.apache.lucene.util.BytesRef.class) return org.elasticsearch.compute.data.BytesRefVector.class;
        throw new IllegalArgumentException("Unsupported value type: " + valueType);
    }

    /**
     * Returns the number of cached aggregator classes.
     */
    public int aggregatorSize() {
        return aggregatorCache.size();
    }

    /**
     * Returns the number of cached grouping aggregator classes.
     */
    public int groupingAggregatorSize() {
        return groupingCache.size();
    }

    /**
     * Clears the failed specs cache. Primarily for testing.
     */
    public void clearFailedSpecs() {
        failedSpecs.clear();
    }

    /**
     * Clears all caches. Primarily for testing.
     */
    public void clearAll() {
        aggregatorCache.clear();
        groupingCache.clear();
        failedSpecs.clear();
    }
}

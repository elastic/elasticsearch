/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.runtime;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.ClassFileVersion;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.lang.reflect.Method;

/**
 * Generates evaluator bytecode at runtime using ByteBuddy.
 * <p>
 * This class generates evaluator classes equivalent to those produced by the
 * compile-time {@code EvaluatorImplementer}, but creates bytecode directly
 * using ByteBuddy's low-level ASM visitor API instead of JavaPoet source generation.
 * <p>
 * The generated evaluators follow the same pattern as static-generated ones:
 * <ul>
 *   <li>Implement {@code EvalOperator.ExpressionEvaluator}</li>
 *   <li>Have fields for source, child evaluators, and driverContext</li>
 *   <li>Have an {@code eval(Page)} method that dispatches to block/vector paths</li>
 * </ul>
 * <p>
 * <b>Reference Implementation:</b> This class must stay aligned with the compile-time generator.
 * When implementing or modifying features, always reference:
 * <ul>
 *   <li>{@code org.elasticsearch.compute.gen.EvaluatorImplementer} - main compile-time generator</li>
 *   <li>{@code org.elasticsearch.compute.gen.argument.StandardArgument} - argument handling (null/multi-value)</li>
 *   <li>Generated evaluators in {@code x-pack/plugin/esql/src/main/generated/} (e.g., AbsIntEvaluator.java)</li>
 * </ul>
 */
public final class RuntimeEvaluatorGenerator {

    private static final Logger logger = LogManager.getLogger(RuntimeEvaluatorGenerator.class);

    // Type descriptors imported from shared utilities
    private static final String SOURCE_CLASS_NAME = RuntimeBytecodeUtils.SOURCE_CLASS_NAME;
    private static final String SOURCE_INTERNAL = RuntimeBytecodeUtils.SOURCE_INTERNAL;
    private static final Type SOURCE_TYPE = Type.getObjectType(SOURCE_INTERNAL);
    private static final String WARNINGS_MODE_INTERNAL = RuntimeBytecodeUtils.WARNINGS_MODE_INTERNAL;
    private static final String WARNING_SOURCE_LOCATION_INTERNAL = RuntimeBytecodeUtils.WARNING_SOURCE_LOCATION_INTERNAL;

    private final EvaluatorCache cache;
    private final ClassLoader parentClassLoader;
    private final Class<?> sourceClass;

    // Per-classloader instances to handle plugin isolation
    private static final java.util.concurrent.ConcurrentHashMap<ClassLoader, RuntimeEvaluatorGenerator> INSTANCES =
        new java.util.concurrent.ConcurrentHashMap<>();

    public RuntimeEvaluatorGenerator(ClassLoader parentClassLoader) {
        this.cache = new EvaluatorCache();
        this.parentClassLoader = parentClassLoader;
        try {
            this.sourceClass = parentClassLoader.loadClass(SOURCE_CLASS_NAME);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load Source class", e);
        }
    }

    /**
     * Gets the singleton instance of the generator.
     * @deprecated Use {@link #getInstance(ClassLoader)} instead
     */
    @Deprecated
    public static RuntimeEvaluatorGenerator getInstance() {
        return getInstance(RuntimeEvaluatorGenerator.class.getClassLoader());
    }

    /**
     * Gets the generator instance for the given classloader.
     * This ensures that generated classes can access the function classes
     * from the correct plugin classloader.
     *
     * @param classLoader the classloader that can see the function class
     * @return the generator instance for that classloader
     */
    public static RuntimeEvaluatorGenerator getInstance(ClassLoader classLoader) {
        return INSTANCES.computeIfAbsent(classLoader, RuntimeEvaluatorGenerator::new);
    }

    /**
     * Generates or retrieves a cached evaluator class for the given method.
     *
     * @param method the method annotated with @RuntimeEvaluator
     * @return the generated evaluator class
     */
    public Class<?> getOrGenerateEvaluator(java.lang.reflect.Method method) {
        validateMethod(method, false);
        EvaluatorSpec spec = EvaluatorSpec.from(method);
        return cache.getOrGenerate(spec, this::generateBytecode, new RuntimeClassLoader(parentClassLoader));
    }

    /**
     * Generates or retrieves a cached MvEvaluator class for the given method.
     *
     * @param method the method annotated with @RuntimeMvEvaluator
     * @return the generated MvEvaluator class
     */
    public Class<?> getOrGenerateMvEvaluator(java.lang.reflect.Method method) {
        validateMethod(method, true);
        EvaluatorSpec spec = EvaluatorSpec.fromMv(method);
        return cache.getOrGenerate(spec, this::generateMvBytecode, new RuntimeClassLoader(parentClassLoader));
    }

    /**
     * Validates a method before attempting bytecode generation.
     * Catches common issues early with clear error messages.
     */
    private void validateMethod(java.lang.reflect.Method method, boolean isMvEvaluator) {
        if (method == null) {
            throw new RuntimeEvaluatorException("Method cannot be null");
        }

        String annotationName = isMvEvaluator ? "@RuntimeMvEvaluator" : "@RuntimeEvaluator";

        if (java.lang.reflect.Modifier.isStatic(method.getModifiers()) == false) {
            throw new RuntimeEvaluatorException(
                "Method "
                    + method.getName()
                    + " must be static to use "
                    + annotationName
                    + ". Declaring class: "
                    + method.getDeclaringClass().getName()
            );
        }

        if (java.lang.reflect.Modifier.isPublic(method.getModifiers()) == false) {
            throw new RuntimeEvaluatorException(
                "Method "
                    + method.getName()
                    + " must be public to use "
                    + annotationName
                    + ". Declaring class: "
                    + method.getDeclaringClass().getName()
            );
        }

        Class<?> returnType = method.getReturnType();
        if (returnType == void.class) {
            throw new RuntimeEvaluatorException(
                "Method "
                    + method.getName()
                    + " cannot have void return type"
                    + ". Declaring class: "
                    + method.getDeclaringClass().getName()
            );
        }

        if (isSupportedType(returnType) == false) {
            throw new RuntimeEvaluatorException(
                "Method "
                    + method.getName()
                    + " has unsupported return type: "
                    + returnType.getName()
                    + ". Supported types: int, long, double, boolean, BytesRef"
                    + ". Declaring class: "
                    + method.getDeclaringClass().getName()
            );
        }

        for (Class<?> paramType : method.getParameterTypes()) {
            if (isSupportedType(paramType) == false && paramType.isArray() == false) {
                throw new RuntimeEvaluatorException(
                    "Method "
                        + method.getName()
                        + " has unsupported parameter type: "
                        + paramType.getName()
                        + ". Supported types: int, long, double, boolean, BytesRef (and arrays for variadic)"
                        + ". Declaring class: "
                        + method.getDeclaringClass().getName()
                );
            }
        }
    }

    private boolean isSupportedType(Class<?> type) {
        return type == int.class
            || type == long.class
            || type == double.class
            || type == boolean.class
            || type == org.apache.lucene.util.BytesRef.class;
    }

    /**
     * Generates bytecode for an evaluator based on the specification.
     *
     * @param spec the evaluator specification
     * @return the bytecode for the evaluator class
     */
    public byte[] generateBytecode(EvaluatorSpec spec) {
        try {
            logger.debug("Generating evaluator for {} with {} parameters", spec.evaluatorSimpleName(), spec.parameterCount());

            // Variadic functions have a different structure - delegate to specialized method
            if (spec.isVariadic()) {
                return generateVariadicBytecode(spec);
            }

            int paramCount = spec.parameterCount();

            // Use Java 21 class file version for proper stack map frame generation
            DynamicType.Builder<?> builder = new ByteBuddy().with(ClassFileVersion.JAVA_V21)
                .subclass(Object.class)
                .name(spec.evaluatorClassName())
                .implement(EvalOperator.ExpressionEvaluator.class);

            // Add fields
            builder = builder.defineField(
                "BASE_RAM_BYTES_USED",
                long.class,
                Visibility.PRIVATE,
                net.bytebuddy.description.modifier.FieldManifestation.FINAL,
                net.bytebuddy.description.modifier.Ownership.STATIC
            )
                .defineField("source", sourceClass, Visibility.PRIVATE, net.bytebuddy.description.modifier.FieldManifestation.FINAL)
                .defineField(
                    "driverContext",
                    DriverContext.class,
                    Visibility.PRIVATE,
                    net.bytebuddy.description.modifier.FieldManifestation.FINAL
                )
                .defineField("warnings", Warnings.class, Visibility.PRIVATE);

            // Add field for each parameter (field0, field1, ..., fieldN-1)
            // For @RuntimeFixed parameters, use their actual type; for evaluated params, use ExpressionEvaluator
            for (int i = 0; i < paramCount; i++) {
                if (spec.isFixed(i)) {
                    // Fixed parameter: store as actual type (int, long, etc.)
                    builder = builder.defineField(
                        "field" + i,
                        spec.parameterType(i),
                        Visibility.PRIVATE,
                        net.bytebuddy.description.modifier.FieldManifestation.FINAL
                    );
                } else {
                    // Evaluated parameter: store as ExpressionEvaluator
                    builder = builder.defineField(
                        "field" + i,
                        EvalOperator.ExpressionEvaluator.class,
                        Visibility.PRIVATE,
                        net.bytebuddy.description.modifier.FieldManifestation.FINAL
                    );
                }
            }

            // Build constructor parameter types: (Source, [params...], DriverContext)
            // For evaluated params: ExpressionEvaluator
            // For fixed params: their actual type
            Class<?>[] constructorParams = new Class<?>[paramCount + 2];
            constructorParams[0] = sourceClass;
            for (int i = 0; i < paramCount; i++) {
                if (spec.isFixed(i)) {
                    constructorParams[i + 1] = spec.parameterType(i);
                } else {
                    constructorParams[i + 1] = EvalOperator.ExpressionEvaluator.class;
                }
            }
            constructorParams[paramCount + 1] = DriverContext.class;

            // Add constructor
            builder = builder.defineConstructor(Visibility.PUBLIC)
                .withParameters(constructorParams)
                .intercept(new ConstructorImplementation(spec));

            // Add eval(Page) method
            builder = builder.defineMethod("eval", Block.class, Visibility.PUBLIC)
                .withParameters(Page.class)
                .intercept(new EvalMethodImplementation(spec));

            // Add baseRamBytesUsed() method - only sum RAM for evaluated (non-fixed) params
            builder = builder.defineMethod("baseRamBytesUsed", long.class, Visibility.PUBLIC)
                .intercept(new BaseRamBytesUsedImplementation(spec));

            // Add close() method - only close evaluated (non-fixed) params
            builder = builder.defineMethod("close", void.class, Visibility.PUBLIC).intercept(new CloseMethodImplementation(spec));

            // Add toString() method
            builder = builder.defineMethod("toString", String.class, Visibility.PUBLIC).intercept(new ToStringImplementation(spec));

            // Add warnings() method
            builder = builder.defineMethod("warnings", Warnings.class, Visibility.PUBLIC).intercept(new WarningsMethodImplementation());

            // For zero-parameter functions (or all-fixed functions), block and vector eval would have the same signature: eval(int)
            // So we only generate the vector-style eval method (which is simpler and sufficient)
            int evaluatedParamCount = spec.evaluatedParameterCount();

            if (evaluatedParamCount > 0) {
                // Add block-style eval method - parameters are (int positionCount, Block0, Block1, ..., BlockN-1)
                // Only include evaluated (non-fixed) parameters
                Class<?>[] blockEvalParams = new Class<?>[evaluatedParamCount + 1];
                blockEvalParams[0] = int.class;
                int blockParamIdx = 1;
                for (int i = 0; i < paramCount; i++) {
                    if (spec.isFixed(i) == false) {
                        blockEvalParams[blockParamIdx++] = getBlockClass(spec.parameterType(i));
                    }
                }
                builder = builder.defineMethod("eval", getBlockClass(spec.returnType()), Visibility.PRIVATE)
                    .withParameters(blockEvalParams)
                    .intercept(new BlockEvalImplementation(spec));
            }

            // Add vector-style eval method - parameters are (int positionCount, Vector0, Vector1, ..., VectorN-1)
            // For zero-parameter functions (or all-fixed), this is just eval(int positionCount)
            // Only include evaluated (non-fixed) parameters
            Class<?>[] vectorEvalParams = new Class<?>[evaluatedParamCount + 1];
            vectorEvalParams[0] = int.class;
            int vectorParamIdx = 1;
            for (int i = 0; i < paramCount; i++) {
                if (spec.isFixed(i) == false) {
                    vectorEvalParams[vectorParamIdx++] = getVectorClass(spec.parameterType(i));
                }
            }
            builder = builder.defineMethod("eval", getBlockClass(spec.returnType()), Visibility.PRIVATE)
                .withParameters(vectorEvalParams)
                .intercept(new VectorEvalImplementation(spec));

            // Add static initializer for BASE_RAM_BYTES_USED
            builder = builder.invokable(net.bytebuddy.matcher.ElementMatchers.isTypeInitializer())
                .intercept(new StaticInitializerImplementation(spec));

            // Make with COMPUTE_FRAMES flag enabled (ByteBuddy default uses this already)
            DynamicType.Unloaded<?> unloaded = builder.make();

            return unloaded.getBytes();

        } catch (Exception e) {
            logger.error("Failed to generate evaluator for " + spec.evaluatorSimpleName(), e);
            throw new RuntimeException("Failed to generate evaluator: " + e.getMessage(), e);
        }
    }

    /**
     * Generates bytecode for variadic functions (functions that take an array parameter).
     * <p>
     * Variadic evaluators have a different structure:
     * <ul>
     *   <li>Field: {@code EvalOperator.ExpressionEvaluator[] values}</li>
     *   <li>Constructor: {@code (Source, ExpressionEvaluator[], DriverContext)}</li>
     *   <li>Eval methods work with arrays of blocks/vectors</li>
     * </ul>
     * <p>
     * Reference: See compile-time generated GreatestIntEvaluator.java
     */
    private byte[] generateVariadicBytecode(EvaluatorSpec spec) {
        logger.debug("Generating variadic evaluator for {}", spec.evaluatorSimpleName());

        Class<?> componentType = spec.variadicComponentType();

        // Use Java 21 class file version for proper stack map frame generation
        DynamicType.Builder<?> builder = new ByteBuddy().with(ClassFileVersion.JAVA_V21)
            .subclass(Object.class)
            .name(spec.evaluatorClassName())
            .implement(EvalOperator.ExpressionEvaluator.class);

        // Add fields: source, values (array), driverContext, warnings
        builder = builder.defineField(
            "BASE_RAM_BYTES_USED",
            long.class,
            Visibility.PRIVATE,
            net.bytebuddy.description.modifier.FieldManifestation.FINAL,
            net.bytebuddy.description.modifier.Ownership.STATIC
        )
            .defineField("source", sourceClass, Visibility.PRIVATE, net.bytebuddy.description.modifier.FieldManifestation.FINAL)
            .defineField(
                "values",
                EvalOperator.ExpressionEvaluator[].class,
                Visibility.PRIVATE,
                net.bytebuddy.description.modifier.FieldManifestation.FINAL
            )
            .defineField(
                "driverContext",
                DriverContext.class,
                Visibility.PRIVATE,
                net.bytebuddy.description.modifier.FieldManifestation.FINAL
            )
            .defineField("warnings", Warnings.class, Visibility.PRIVATE);

        // Constructor: (Source, ExpressionEvaluator[], DriverContext)
        builder = builder.defineConstructor(Visibility.PUBLIC)
            .withParameters(sourceClass, EvalOperator.ExpressionEvaluator[].class, DriverContext.class)
            .intercept(new VariadicConstructorImplementation(spec));

        // Add eval(Page) method
        builder = builder.defineMethod("eval", Block.class, Visibility.PUBLIC)
            .withParameters(Page.class)
            .intercept(new VariadicEvalMethodImplementation(spec));

        // Add baseRamBytesUsed() method
        builder = builder.defineMethod("baseRamBytesUsed", long.class, Visibility.PUBLIC)
            .intercept(new VariadicBaseRamBytesUsedImplementation(spec));

        // Add close() method
        builder = builder.defineMethod("close", void.class, Visibility.PUBLIC).intercept(new VariadicCloseMethodImplementation(spec));

        // Add toString() method
        builder = builder.defineMethod("toString", String.class, Visibility.PUBLIC).intercept(new VariadicToStringImplementation(spec));

        // Add warnings() method
        builder = builder.defineMethod("warnings", Warnings.class, Visibility.PUBLIC).intercept(new WarningsMethodImplementation());

        // Add block-style eval method: eval(int positionCount, Block[] valuesBlocks)
        Class<?> blockArrayClass = java.lang.reflect.Array.newInstance(getBlockClass(componentType), 0).getClass();
        builder = builder.defineMethod("eval", getBlockClass(spec.returnType()), Visibility.PUBLIC)
            .withParameters(int.class, blockArrayClass)
            .intercept(new VariadicBlockEvalImplementation(spec));

        // Add vector-style eval method: eval(int positionCount, Vector[] valuesVectors)
        Class<?> vectorArrayClass = java.lang.reflect.Array.newInstance(getVectorClass(componentType), 0).getClass();
        builder = builder.defineMethod("eval", getVectorClass(spec.returnType()), Visibility.PUBLIC)
            .withParameters(int.class, vectorArrayClass)
            .intercept(new VariadicVectorEvalImplementation(spec));

        // Add static initializer for BASE_RAM_BYTES_USED
        builder = builder.invokable(net.bytebuddy.matcher.ElementMatchers.isTypeInitializer())
            .intercept(new StaticInitializerImplementation(spec));

        DynamicType.Unloaded<?> unloaded = builder.make();
        return unloaded.getBytes();
    }

    /**
     * Generates bytecode for MvEvaluator functions (multi-value reducers).
     * <p>
     * MvEvaluators reduce multivalued fields to a single value using pairwise processing.
     * The generated class extends AbstractMultivalueFunction.AbstractNullableEvaluator.
     * <p>
     * Structure:
     * <ul>
     *   <li>Extends AbstractMultivalueFunction.AbstractNullableEvaluator</li>
     *   <li>Field: source (if warnExceptions), warnings</li>
     *   <li>Constructor: (Source, ExpressionEvaluator, DriverContext) - calls super(driverContext, field)</li>
     *   <li>name() - returns function name</li>
     *   <li>evalNullable(Block) - pairwise processing loop</li>
     *   <li>warnings() - if warnExceptions</li>
     *   <li>baseRamBytesUsed() - returns BASE_RAM_BYTES_USED + field.baseRamBytesUsed()</li>
     * </ul>
     * <p>
     * Reference: See compile-time generated MvSumIntEvaluator.java
     */
    private byte[] generateMvBytecode(EvaluatorSpec spec) {
        logger.debug("Generating MvEvaluator for {}", spec.evaluatorSimpleName());

        // The field type is the first parameter type (pairwise: process(T current, T next))
        Class<?> fieldType = spec.parameterType(0);

        // Base class selection: MvEvaluatorImplementer.type() lines 133-139
        // - When warnExceptions.isEmpty() → AbstractEvaluator
        // - When !warnExceptions.isEmpty() → AbstractNullableEvaluator
        String baseClassName = spec.hasWarnExceptions()
            ? "org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.AbstractMultivalueFunction$AbstractNullableEvaluator"
            : "org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.AbstractMultivalueFunction$AbstractEvaluator";
        Class<?> baseEvaluatorClass;
        try {
            baseEvaluatorClass = parentClassLoader.loadClass(baseClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load base evaluator class: " + baseClassName, e);
        }

        // Use Java 21 class file version for proper stack map frame generation
        DynamicType.Builder<?> builder = new ByteBuddy().with(ClassFileVersion.JAVA_V21)
            .subclass(baseEvaluatorClass)
            .name(spec.evaluatorClassName());

        // Add fields: BASE_RAM_BYTES_USED (static), source (if warnExceptions), warnings
        builder = builder.defineField(
            "BASE_RAM_BYTES_USED",
            long.class,
            Visibility.PRIVATE,
            net.bytebuddy.description.modifier.FieldManifestation.FINAL,
            net.bytebuddy.description.modifier.Ownership.STATIC
        );

        if (spec.hasWarnExceptions()) {
            builder = builder.defineField(
                "source",
                sourceClass,
                Visibility.PRIVATE,
                net.bytebuddy.description.modifier.FieldManifestation.FINAL
            ).defineField("warnings", Warnings.class, Visibility.PRIVATE);
        }

        // Constructor: (Source, ExpressionEvaluator, DriverContext) or (ExpressionEvaluator, DriverContext)
        if (spec.hasWarnExceptions()) {
            builder = builder.defineConstructor(Visibility.PUBLIC)
                .withParameters(sourceClass, EvalOperator.ExpressionEvaluator.class, DriverContext.class)
                .intercept(new MvConstructorImplementation(spec, true));
        } else {
            builder = builder.defineConstructor(Visibility.PUBLIC)
                .withParameters(EvalOperator.ExpressionEvaluator.class, DriverContext.class)
                .intercept(new MvConstructorImplementation(spec, false));
        }

        // Add name() method
        builder = builder.defineMethod("name", String.class, Visibility.PUBLIC).intercept(new MvNameMethodImplementation(spec));

        // Add evalNullable(Block) method - always generated
        builder = builder.defineMethod("evalNullable", Block.class, Visibility.PUBLIC)
            .withParameters(Block.class)
            .intercept(new MvEvalNullableImplementation(spec));

        // Add evalNotNullable(Block) method - only when no warnExceptions
        // See MvEvaluatorImplementer.type() lines 145-147
        if (spec.hasWarnExceptions() == false) {
            builder = builder.defineMethod("evalNotNullable", Block.class, Visibility.PUBLIC)
                .withParameters(Block.class)
                .intercept(new MvEvalNotNullableImplementation(spec));
        }

        // Add evalSingleValuedNullable/NotNullable methods if single value function is specified
        // See MvEvaluatorImplementer.type() lines 148-153
        if (spec.hasSingleValueMethod()) {
            builder = builder.defineMethod("evalSingleValuedNullable", Block.class, Visibility.PUBLIC)
                .withParameters(Block.class)
                .intercept(new MvEvalSingleValuedNullableImplementation(spec));
            if (spec.hasWarnExceptions() == false) {
                builder = builder.defineMethod("evalSingleValuedNotNullable", Block.class, Visibility.PUBLIC)
                    .withParameters(Block.class)
                    .intercept(new MvEvalSingleValuedNotNullableImplementation(spec));
            }
        }

        // Add evalAscendingNullable/NotNullable methods if ascending function is specified
        // See MvEvaluatorImplementer.type() lines 154-157
        if (spec.hasAscendingMethod()) {
            builder = builder.defineMethod("evalAscendingNullable", Block.class, Visibility.PRIVATE)
                .withParameters(Block.class)
                .intercept(new MvEvalAscendingNullableImplementation(spec));
            builder = builder.defineMethod("evalAscendingNotNullable", Block.class, Visibility.PRIVATE)
                .withParameters(Block.class)
                .intercept(new MvEvalAscendingNotNullableImplementation(spec));
        }

        // Add warnings() method if needed
        if (spec.hasWarnExceptions()) {
            builder = builder.defineMethod("warnings", Warnings.class, Visibility.PRIVATE)
                .intercept(new MvWarningsMethodImplementation(spec));
        }

        // Add baseRamBytesUsed() method
        builder = builder.defineMethod("baseRamBytesUsed", long.class, Visibility.PUBLIC)
            .intercept(new MvBaseRamBytesUsedImplementation(spec));

        // Add static initializer for BASE_RAM_BYTES_USED
        builder = builder.invokable(net.bytebuddy.matcher.ElementMatchers.isTypeInitializer())
            .intercept(new StaticInitializerImplementation(spec));

        DynamicType.Unloaded<?> unloaded = builder.make();
        return unloaded.getBytes();
    }

    // Helper methods to get Block and Vector classes for different types
    private Class<?> getBlockClass(Class<?> fieldType) {
        if (fieldType == double.class) {
            return org.elasticsearch.compute.data.DoubleBlock.class;
        } else if (fieldType == long.class) {
            return org.elasticsearch.compute.data.LongBlock.class;
        } else if (fieldType == int.class) {
            return org.elasticsearch.compute.data.IntBlock.class;
        } else if (fieldType == boolean.class) {
            return org.elasticsearch.compute.data.BooleanBlock.class;
        } else if (fieldType == BytesRef.class) {
            return org.elasticsearch.compute.data.BytesRefBlock.class;
        }
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }

    private Class<?> getVectorClass(Class<?> fieldType) {
        if (fieldType == double.class) {
            return org.elasticsearch.compute.data.DoubleVector.class;
        } else if (fieldType == long.class) {
            return org.elasticsearch.compute.data.LongVector.class;
        } else if (fieldType == int.class) {
            return org.elasticsearch.compute.data.IntVector.class;
        } else if (fieldType == boolean.class) {
            return org.elasticsearch.compute.data.BooleanVector.class;
        } else if (fieldType == BytesRef.class) {
            return org.elasticsearch.compute.data.BytesRefVector.class;
        }
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }

    private String getBlockType(Class<?> fieldType) {
        if (fieldType == double.class) return "DoubleBlock";
        if (fieldType == long.class) return "LongBlock";
        if (fieldType == int.class) return "IntBlock";
        if (fieldType == boolean.class) return "BooleanBlock";
        if (fieldType == BytesRef.class) return "BytesRefBlock";
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }

    private String getVectorType(Class<?> fieldType) {
        if (fieldType == double.class) return "DoubleVector";
        if (fieldType == long.class) return "LongVector";
        if (fieldType == int.class) return "IntVector";
        if (fieldType == boolean.class) return "BooleanVector";
        if (fieldType == BytesRef.class) return "BytesRefVector";
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }

    // ============================================================================================
    // SCALAR EVALUATOR IMPLEMENTATIONS
    // ============================================================================================
    // These implementations handle standard scalar functions (e.g., ABS, ROUND).
    // Reference: See compile-time EvaluatorImplementer and generated evaluators like AbsIntEvaluator.java
    // ============================================================================================

    /**
     * Implements the constructor: (Source, ExpressionEvaluator..., DriverContext)
     * For N-ary functions, the constructor has N ExpressionEvaluator parameters.
     * <p>
     * Generates equivalent bytecode to:
     * <pre>{@code
     * public Evaluator(Source source, ExpressionEvaluator field0, ..., DriverContext driverContext) {
     *     this.source = source;
     *     this.field0 = field0;
     *     // ... for each field
     *     this.driverContext = driverContext;
     * }
     * }</pre>
     * <p>
     * Reference: {@code EvaluatorImplementer#ctor()} and {@code StandardArgument#implementCtor()}
     * Reference: {@code FixedArgument#implementCtor()} for fixed parameter handling
     */
    private static class ConstructorImplementation implements Implementation {
        private final EvaluatorSpec spec;

        ConstructorImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                int paramCount = spec.parameterCount();

                // super()
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, Type.getInternalName(Object.class), "<init>", "()V", false);

                // this.source = source (param 1)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1);
                methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "source", SOURCE_TYPE.getDescriptor());

                // this.field0 = field0 (param 2), this.field1 = field1 (param 3), etc.
                // Track current local variable slot - primitives use 1 slot, longs/doubles use 2
                int currentSlot = 2; // Start after 'this' and 'source'
                for (int i = 0; i < paramCount; i++) {
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);

                    Class<?> paramType = spec.parameterType(i);
                    if (spec.isFixed(i)) {
                        // Fixed parameter: use appropriate load instruction based on type
                        int loadOpcode = getLoadOpcode(paramType);
                        methodVisitor.visitVarInsn(loadOpcode, currentSlot);
                        methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "field" + i, Type.getDescriptor(paramType));
                        // long and double take 2 slots
                        currentSlot += (paramType == long.class || paramType == double.class) ? 2 : 1;
                    } else {
                        // Evaluated parameter: always ALOAD for ExpressionEvaluator
                        methodVisitor.visitVarInsn(Opcodes.ALOAD, currentSlot);
                        methodVisitor.visitFieldInsn(
                            Opcodes.PUTFIELD,
                            className,
                            "field" + i,
                            Type.getDescriptor(EvalOperator.ExpressionEvaluator.class)
                        );
                        currentSlot += 1; // Object references take 1 slot
                    }
                }

                // this.driverContext = driverContext (last param)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, currentSlot);
                methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "driverContext", Type.getDescriptor(DriverContext.class));

                methodVisitor.visitInsn(Opcodes.RETURN);

                return new ByteCodeAppender.Size(4, currentSlot + 1);
            };
        }

        private int getLoadOpcode(Class<?> type) {
            if (type == int.class || type == short.class || type == byte.class || type == char.class || type == boolean.class) {
                return Opcodes.ILOAD;
            } else if (type == long.class) {
                return Opcodes.LLOAD;
            } else if (type == float.class) {
                return Opcodes.FLOAD;
            } else if (type == double.class) {
                return Opcodes.DLOAD;
            } else {
                return Opcodes.ALOAD; // Object types
            }
        }
    }

    /**
     * Implements the eval(Page) method - the main entry point for evaluation.
     * <p>
     * This method evaluates all child expressions to get blocks, attempts to get vectors
     * from those blocks, and dispatches to either the vector path (optimized) or block path
     * (handles nulls/multi-values).
     * <p>
     * Generates equivalent bytecode to:
     * <pre>{@code
     * public Block eval(Page page) {
     *     try (IntBlock field0Block = (IntBlock) field0.eval(page)) {
     *         IntVector field0Vector = field0Block.asVector();
     *         if (field0Vector == null) {
     *             return eval(page.getPositionCount(), field0Block);
     *         }
     *         return eval(page.getPositionCount(), field0Vector).asBlock();
     *     }
     * }
     * }</pre>
     * <p>
     * Reference: {@code EvaluatorImplementer#eval()}, {@code StandardArgument#evalToBlock()},
     * {@code StandardArgument#resolveVectors()}
     */
    private static class EvalMethodImplementation implements Implementation {
        private final EvaluatorSpec spec;

        EvalMethodImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                int paramCount = spec.parameterCount();
                String resultBlockType = getBlockTypeInternal(spec.returnType());

                // Count evaluated (non-fixed) parameters - only these need blocks/vectors
                int evaluatedParamCount = spec.evaluatedParameterCount();

                // Special case: zero evaluated parameters (all fixed, or no params like PI(), E(), NOW())
                // No child evaluators, go directly to vector path
                if (evaluatedParamCount == 0) {
                    // result = eval(page.getPositionCount())
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // page
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        Type.getInternalName(Page.class),
                        "getPositionCount",
                        "()I",
                        false
                    );
                    // Call vector eval with just positionCount: eval(int) -> Block
                    String vectorEvalDesc = "(I)L" + resultBlockType + ";";
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, className, "eval", vectorEvalDesc, false);
                    methodVisitor.visitInsn(Opcodes.ARETURN);
                    return new ByteCodeAppender.Size(3, 2);
                }

                // Local variables layout (for evaluatedParamCount >= 1):
                // 0: this
                // 1: page (parameter)
                // 2 to 2+N-1: blocks[0..N-1] for evaluated params only
                // 2+N to 2+2N-1: vectors[0..N-1] for evaluated params only
                // 2+2N: allVectors (boolean as int)
                // 2+2N+1: result

                int blocksStartSlot = 2;
                int vectorsStartSlot = blocksStartSlot + evaluatedParamCount;
                int allVectorsSlot = vectorsStartSlot + evaluatedParamCount;
                int resultSlot = allVectorsSlot + 1;

                // Evaluate only non-fixed child evaluators and store blocks
                // Track which local slot corresponds to which original param index
                int evalIdx = 0;
                for (int i = 0; i < paramCount; i++) {
                    if (spec.isFixed(i)) {
                        continue; // Skip fixed parameters - they don't need evaluation
                    }

                    String blockType = getBlockTypeInternal(spec.parameterType(i));

                    // Block block_evalIdx = (BlockType) field_i.eval(page)
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    methodVisitor.visitFieldInsn(
                        Opcodes.GETFIELD,
                        className,
                        "field" + i,
                        Type.getDescriptor(EvalOperator.ExpressionEvaluator.class)
                    );
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // page
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEINTERFACE,
                        Type.getInternalName(EvalOperator.ExpressionEvaluator.class),
                        "eval",
                        Type.getMethodDescriptor(Type.getType(Block.class), Type.getType(Page.class)),
                        true
                    );
                    methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, blockType);
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, blocksStartSlot + evalIdx);
                    evalIdx++;
                }

                // Get vectors from all evaluated blocks
                evalIdx = 0;
                for (int i = 0; i < paramCount; i++) {
                    if (spec.isFixed(i)) {
                        continue; // Skip fixed parameters
                    }

                    String blockType = getBlockTypeInternal(spec.parameterType(i));
                    String vectorType = getVectorTypeInternal(spec.parameterType(i));

                    // Vector vector_evalIdx = block_evalIdx.asVector()
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, blocksStartSlot + evalIdx);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "asVector", "()L" + vectorType + ";", true);
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, vectorsStartSlot + evalIdx);
                    evalIdx++;
                }

                // Check if all vectors are non-null: allVectors = (vector0 != null && vector1 != null && ...)
                // Use a simple approach without jumps inside the loop:
                // allVectors = (vector0 != null) ? 1 : 0
                // for each subsequent vector: allVectors = allVectors & ((vectorN != null) ? 1 : 0)

                // First vector check (for first evaluated parameter)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, vectorsStartSlot);
                Label firstNotNull = new Label();
                Label afterFirstCheck = new Label();
                methodVisitor.visitJumpInsn(Opcodes.IFNONNULL, firstNotNull);
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, afterFirstCheck);
                methodVisitor.visitLabel(firstNotNull);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    2 + 2 * evaluatedParamCount,
                    buildFullFrameLocalsWithoutAllVectorsForEvaluated(className, evaluatedParamCount, spec),
                    0,
                    null
                );
                methodVisitor.visitInsn(Opcodes.ICONST_1);
                methodVisitor.visitLabel(afterFirstCheck);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    2 + 2 * evaluatedParamCount,
                    buildFullFrameLocalsWithoutAllVectorsForEvaluated(className, evaluatedParamCount, spec),
                    1,
                    new Object[] { Opcodes.INTEGER }
                );
                methodVisitor.visitVarInsn(Opcodes.ISTORE, allVectorsSlot);

                // Subsequent vector checks (if any) - for remaining evaluated parameters
                for (int i = 1; i < evaluatedParamCount; i++) {
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, allVectorsSlot);
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, vectorsStartSlot + i);
                    Label notNull = new Label();
                    Label afterCheck = new Label();
                    methodVisitor.visitJumpInsn(Opcodes.IFNONNULL, notNull);
                    methodVisitor.visitInsn(Opcodes.ICONST_0);
                    methodVisitor.visitJumpInsn(Opcodes.GOTO, afterCheck);
                    methodVisitor.visitLabel(notNull);
                    methodVisitor.visitFrame(
                        Opcodes.F_FULL,
                        2 + 2 * evaluatedParamCount + 1,
                        buildFullFrameLocalsForEvaluated(className, evaluatedParamCount, spec),
                        1,
                        new Object[] { Opcodes.INTEGER }
                    );
                    methodVisitor.visitInsn(Opcodes.ICONST_1);
                    methodVisitor.visitLabel(afterCheck);
                    methodVisitor.visitFrame(
                        Opcodes.F_FULL,
                        2 + 2 * evaluatedParamCount + 1,
                        buildFullFrameLocalsForEvaluated(className, evaluatedParamCount, spec),
                        2,
                        new Object[] { Opcodes.INTEGER, Opcodes.INTEGER }
                    );
                    methodVisitor.visitInsn(Opcodes.IAND);
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, allVectorsSlot);
                }

                // if (!allVectors) goto blockPath
                methodVisitor.visitVarInsn(Opcodes.ILOAD, allVectorsSlot);
                Label vectorPath = new Label();
                methodVisitor.visitJumpInsn(Opcodes.IFNE, vectorPath);

                // Block path: result = eval(page.getPositionCount(), block0, block1, ...)
                // Only pass evaluated (non-fixed) blocks
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // page
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(Page.class), "getPositionCount", "()I", false);
                for (int i = 0; i < evaluatedParamCount; i++) {
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, blocksStartSlot + i);
                }
                String blockEvalDesc = buildBlockEvalDescriptor(spec);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, className, "eval", blockEvalDesc, false);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, resultSlot);

                // Close all evaluated blocks
                for (int i = 0; i < evaluatedParamCount; i++) {
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, blocksStartSlot + i);
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKESTATIC,
                        Type.getInternalName(Releasables.class),
                        "closeExpectNoException",
                        Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(Releasable.class)),
                        false
                    );
                }

                // return result
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                methodVisitor.visitInsn(Opcodes.ARETURN);

                // Vector path
                methodVisitor.visitLabel(vectorPath);
                // Same frame as checkDone
                methodVisitor.visitFrame(Opcodes.F_SAME, 0, null, 0, null);

                // result = eval(page.getPositionCount(), vector0, vector1, ...)
                // Only pass evaluated (non-fixed) vectors
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // page
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, Type.getInternalName(Page.class), "getPositionCount", "()I", false);
                for (int i = 0; i < evaluatedParamCount; i++) {
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, vectorsStartSlot + i);
                }
                String vectorEvalDesc = buildVectorEvalDescriptor(spec);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, className, "eval", vectorEvalDesc, false);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, resultSlot);

                // Close all evaluated blocks
                for (int i = 0; i < evaluatedParamCount; i++) {
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, blocksStartSlot + i);
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKESTATIC,
                        Type.getInternalName(Releasables.class),
                        "closeExpectNoException",
                        Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(Releasable.class)),
                        false
                    );
                }

                // return result
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                methodVisitor.visitInsn(Opcodes.ARETURN);

                return new ByteCodeAppender.Size(evaluatedParamCount + 5, resultSlot + 2);
            };
        }

        // Helper methods for building frame locals - these work with EVALUATED parameters only
        private Object[] buildFullFrameLocalsForEvaluated(String className, int evaluatedParamCount, EvaluatorSpec spec) {
            // this, page, blocks..., vectors..., allVectors (for evaluated params only)
            Object[] locals = new Object[2 + 2 * evaluatedParamCount + 1];
            locals[0] = className;
            locals[1] = Type.getInternalName(Page.class);
            int evalIdx = 0;
            for (int i = 0; i < spec.parameterCount(); i++) {
                if (spec.isFixed(i) == false) {
                    locals[2 + evalIdx] = getBlockTypeInternal(spec.parameterType(i));
                    evalIdx++;
                }
            }
            evalIdx = 0;
            for (int i = 0; i < spec.parameterCount(); i++) {
                if (spec.isFixed(i) == false) {
                    locals[2 + evaluatedParamCount + evalIdx] = getVectorTypeInternal(spec.parameterType(i));
                    evalIdx++;
                }
            }
            locals[2 + 2 * evaluatedParamCount] = Opcodes.INTEGER;
            return locals;
        }

        private Object[] buildFullFrameLocalsWithoutAllVectorsForEvaluated(String className, int evaluatedParamCount, EvaluatorSpec spec) {
            // this, page, blocks..., vectors... (no allVectors yet, for evaluated params only)
            Object[] locals = new Object[2 + 2 * evaluatedParamCount];
            locals[0] = className;
            locals[1] = Type.getInternalName(Page.class);
            int evalIdx = 0;
            for (int i = 0; i < spec.parameterCount(); i++) {
                if (spec.isFixed(i) == false) {
                    locals[2 + evalIdx] = getBlockTypeInternal(spec.parameterType(i));
                    evalIdx++;
                }
            }
            evalIdx = 0;
            for (int i = 0; i < spec.parameterCount(); i++) {
                if (spec.isFixed(i) == false) {
                    locals[2 + evaluatedParamCount + evalIdx] = getVectorTypeInternal(spec.parameterType(i));
                    evalIdx++;
                }
            }
            return locals;
        }

        // Legacy methods kept for backward compatibility (used by other parts of the code)
        private Object[] buildFullFrameLocals(String className, int paramCount, EvaluatorSpec spec) {
            // this, page, blocks..., vectors..., allVectors
            Object[] locals = new Object[2 + 2 * paramCount + 1];
            locals[0] = className;
            locals[1] = Type.getInternalName(Page.class);
            for (int i = 0; i < paramCount; i++) {
                locals[2 + i] = getBlockTypeInternal(spec.parameterType(i));
            }
            for (int i = 0; i < paramCount; i++) {
                locals[2 + paramCount + i] = getVectorTypeInternal(spec.parameterType(i));
            }
            locals[2 + 2 * paramCount] = Opcodes.INTEGER;
            return locals;
        }

        private Object[] buildFullFrameLocalsWithoutAllVectors(String className, int paramCount, EvaluatorSpec spec) {
            // this, page, blocks..., vectors... (no allVectors yet)
            Object[] locals = new Object[2 + 2 * paramCount];
            locals[0] = className;
            locals[1] = Type.getInternalName(Page.class);
            for (int i = 0; i < paramCount; i++) {
                locals[2 + i] = getBlockTypeInternal(spec.parameterType(i));
            }
            for (int i = 0; i < paramCount; i++) {
                locals[2 + paramCount + i] = getVectorTypeInternal(spec.parameterType(i));
            }
            return locals;
        }

        // Build method descriptors for block/vector eval - only include EVALUATED parameters
        private String buildBlockEvalDescriptor(EvaluatorSpec spec) {
            StringBuilder desc = new StringBuilder("(I");
            for (int i = 0; i < spec.parameterCount(); i++) {
                if (spec.isFixed(i) == false) {
                    desc.append("L").append(getBlockTypeInternal(spec.parameterType(i))).append(";");
                }
            }
            desc.append(")L").append(getBlockTypeInternal(spec.returnType())).append(";");
            return desc.toString();
        }

        private String buildVectorEvalDescriptor(EvaluatorSpec spec) {
            StringBuilder desc = new StringBuilder("(I");
            for (int i = 0; i < spec.parameterCount(); i++) {
                if (spec.isFixed(i) == false) {
                    desc.append("L").append(getVectorTypeInternal(spec.parameterType(i))).append(";");
                }
            }
            desc.append(")L").append(getBlockTypeInternal(spec.returnType())).append(";");
            return desc.toString();
        }

        private String getBlockTypeInternal(Class<?> fieldType) {
            if (fieldType == double.class) return "org/elasticsearch/compute/data/DoubleBlock";
            if (fieldType == long.class) return "org/elasticsearch/compute/data/LongBlock";
            if (fieldType == int.class) return "org/elasticsearch/compute/data/IntBlock";
            if (fieldType == boolean.class) return "org/elasticsearch/compute/data/BooleanBlock";
            if (fieldType == BytesRef.class) return "org/elasticsearch/compute/data/BytesRefBlock";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getVectorTypeInternal(Class<?> fieldType) {
            if (fieldType == double.class) return "org/elasticsearch/compute/data/DoubleVector";
            if (fieldType == long.class) return "org/elasticsearch/compute/data/LongVector";
            if (fieldType == int.class) return "org/elasticsearch/compute/data/IntVector";
            if (fieldType == boolean.class) return "org/elasticsearch/compute/data/BooleanVector";
            if (fieldType == BytesRef.class) return "org/elasticsearch/compute/data/BytesRefVector";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }
    }

    /**
     * Implements baseRamBytesUsed() - returns the base RAM usage plus child evaluator RAM.
     * <p>
     * Generates equivalent bytecode to:
     * <pre>{@code
     * public long baseRamBytesUsed() {
     *     long baseRamBytesUsed = BASE_RAM_BYTES_USED;
     *     baseRamBytesUsed += field0.baseRamBytesUsed();
     *     baseRamBytesUsed += field1.baseRamBytesUsed();
     *     // ... for each field
     *     return baseRamBytesUsed;
     * }
     * }</pre>
     * <p>
     * Reference: {@code ProcessFunction#baseRamBytesUsed()}, {@code StandardArgument#sumBaseRamBytesUsed()}
     * Reference: {@code FixedArgument#sumBaseRamBytesUsed()} - fixed params do NOT contribute to RAM
     */
    private static class BaseRamBytesUsedImplementation implements Implementation {
        private final EvaluatorSpec spec;

        BaseRamBytesUsedImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                int paramCount = spec.parameterCount();

                // long baseRamBytesUsed = BASE_RAM_BYTES_USED;
                methodVisitor.visitFieldInsn(Opcodes.GETSTATIC, className, "BASE_RAM_BYTES_USED", "J");

                // baseRamBytesUsed += field0.baseRamBytesUsed();
                // baseRamBytesUsed += field1.baseRamBytesUsed();
                // ... for each evaluated (non-fixed) field
                // Fixed parameters do NOT contribute to RAM usage (per FixedArgument#sumBaseRamBytesUsed)
                for (int i = 0; i < paramCount; i++) {
                    if (spec.isFixed(i) == false) {
                        methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                        methodVisitor.visitFieldInsn(
                            Opcodes.GETFIELD,
                            className,
                            "field" + i,
                            Type.getDescriptor(EvalOperator.ExpressionEvaluator.class)
                        );
                        methodVisitor.visitMethodInsn(
                            Opcodes.INVOKEINTERFACE,
                            Type.getInternalName(EvalOperator.ExpressionEvaluator.class),
                            "baseRamBytesUsed",
                            "()J",
                            true
                        );
                        methodVisitor.visitInsn(Opcodes.LADD);
                    }
                }

                // return baseRamBytesUsed;
                methodVisitor.visitInsn(Opcodes.LRETURN);
                return new ByteCodeAppender.Size(4, 1);
            };
        }
    }

    /**
     * Implements close() - closes all field evaluators using Releasables.closeExpectNoException.
     * <p>
     * Generates equivalent bytecode to:
     * <pre>{@code
     * public void close() {
     *     Releasables.closeExpectNoException(field0, field1, ...);
     * }
     * }</pre>
     * <p>
     * Reference: {@code ProcessFunction#close()}, {@code StandardArgument#closeInvocation()}
     * Reference: {@code FixedArgument#closeInvocation()} - fixed params are NOT closed (unless releasable)
     */
    private static class CloseMethodImplementation implements Implementation {
        private final EvaluatorSpec spec;

        CloseMethodImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                int paramCount = spec.parameterCount();

                // Only close evaluated (non-fixed) field evaluators
                // Fixed parameters are not Releasable (unless marked as such, which we don't support yet)
                for (int i = 0; i < paramCount; i++) {
                    if (spec.isFixed(i) == false) {
                        methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                        methodVisitor.visitFieldInsn(
                            Opcodes.GETFIELD,
                            className,
                            "field" + i,
                            Type.getDescriptor(EvalOperator.ExpressionEvaluator.class)
                        );
                        methodVisitor.visitMethodInsn(
                            Opcodes.INVOKESTATIC,
                            Type.getInternalName(Releasables.class),
                            "closeExpectNoException",
                            Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(Releasable.class)),
                            false
                        );
                    }
                }

                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(2, 1);
            };
        }
    }

    /**
     * Implements toString() - returns evaluator name with actual field evaluator values.
     * <p>
     * Generates equivalent bytecode to:
     * <pre>{@code
     * public String toString() {
     *     return "EvaluatorName[field0=" + field0 + ", field1=" + field1 + "]";
     * }
     * }</pre>
     * <p>
     * Reference: {@code ProcessFunction#toStringMethod()}, {@code StandardArgument#buildToStringInvocation()}
     */
    private static class ToStringImplementation implements Implementation {
        private final EvaluatorSpec spec;

        ToStringImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                int paramCount = spec.parameterCount();

                // Generate: return "EvaluatorName[" + "field0=" + field0 + ", field1=" + field1 + "]"
                // Using string concatenation which Java compiles to StringBuilder

                // Start with the evaluator name and opening bracket
                methodVisitor.visitLdcInsn(spec.evaluatorSimpleName() + "[");

                // For each field, concatenate "fieldN=" + fieldN
                for (int i = 0; i < paramCount; i++) {
                    // Add separator for fields after the first
                    String prefix = (i == 0) ? "field" + i + "=" : ", field" + i + "=";

                    // Concatenate the prefix string
                    methodVisitor.visitLdcInsn(prefix);
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        "java/lang/String",
                        "concat",
                        "(Ljava/lang/String;)Ljava/lang/String;",
                        false
                    );

                    // Load the field value and convert to string via String.valueOf
                    // Fixed parameters have their actual type, evaluated parameters are ExpressionEvaluator
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    Class<?> fieldType = spec.isFixed(i) ? spec.parameterType(i) : EvalOperator.ExpressionEvaluator.class;
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "field" + i, Type.getDescriptor(fieldType));

                    // Convert to string - use appropriate String.valueOf overload
                    String valueOfDescriptor;
                    if (fieldType == int.class) {
                        valueOfDescriptor = "(I)Ljava/lang/String;";
                    } else if (fieldType == long.class) {
                        valueOfDescriptor = "(J)Ljava/lang/String;";
                    } else if (fieldType == double.class) {
                        valueOfDescriptor = "(D)Ljava/lang/String;";
                    } else if (fieldType == boolean.class) {
                        valueOfDescriptor = "(Z)Ljava/lang/String;";
                    } else {
                        valueOfDescriptor = "(Ljava/lang/Object;)Ljava/lang/String;";
                    }
                    methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/String", "valueOf", valueOfDescriptor, false);

                    // Concatenate the field value
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        "java/lang/String",
                        "concat",
                        "(Ljava/lang/String;)Ljava/lang/String;",
                        false
                    );
                }

                // Add closing bracket
                methodVisitor.visitLdcInsn("]");
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "java/lang/String",
                    "concat",
                    "(Ljava/lang/String;)Ljava/lang/String;",
                    false
                );

                methodVisitor.visitInsn(Opcodes.ARETURN);
                return new ByteCodeAppender.Size(3, 1);
            };
        }
    }

    /**
     * Implements warnings() - lazy initialization of warnings field.
     * <p>
     * Generates equivalent bytecode to:
     * <pre>{@code
     * private Warnings warnings() {
     *     if (warnings == null) {
     *         this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
     *     }
     *     return warnings;
     * }
     * }</pre>
     * <p>
     * Reference: {@code EvaluatorImplementer#warnings()}
     */
    private static class WarningsMethodImplementation implements Implementation {
        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();

                // if (warnings == null)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "warnings", Type.getDescriptor(Warnings.class));
                Label notNull = new Label();
                methodVisitor.visitJumpInsn(Opcodes.IFNONNULL, notNull);

                // warnings = Warnings.createWarnings(...)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "driverContext", Type.getDescriptor(DriverContext.class));
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    Type.getInternalName(DriverContext.class),
                    "warningsMode",
                    "()L" + WARNINGS_MODE_INTERNAL + ";",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "source", SOURCE_TYPE.getDescriptor());
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    Type.getInternalName(Warnings.class),
                    "createWarnings",
                    Type.getMethodDescriptor(
                        Type.getType(Warnings.class),
                        Type.getObjectType(WARNINGS_MODE_INTERNAL),
                        Type.getObjectType(WARNING_SOURCE_LOCATION_INTERNAL)
                    ),
                    false
                );
                methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "warnings", Type.getDescriptor(Warnings.class));

                // return warnings
                methodVisitor.visitLabel(notNull);
                // Frame at notNull - same locals as method entry (just 'this')
                methodVisitor.visitFrame(Opcodes.F_SAME, 0, null, 0, null);

                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "warnings", Type.getDescriptor(Warnings.class));
                methodVisitor.visitInsn(Opcodes.ARETURN);

                return new ByteCodeAppender.Size(3, 1);
            };
        }
    }

    /**
     * Implements eval(int positionCount, Block...) - block-style evaluation for N-ary functions.
     * <p>
     * This method handles positions that may have nulls (valueCount==0) or multi-values (valueCount&gt;1).
     * For each position, it checks all input blocks and:
     * <ul>
     *   <li>If any block has valueCount==0: appends null to result</li>
     *   <li>If any block has valueCount&gt;1: registers a warning and appends null</li>
     *   <li>If all blocks have valueCount==1: processes the values normally</li>
     * </ul>
     * <p>
     * Generates equivalent bytecode to:
     * <pre>{@code
     * public IntBlock eval(int positionCount, IntBlock fieldBlock) {
     *     try (IntBlock.Builder result = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
     *         position: for (int p = 0; p < positionCount; p++) {
     *             switch (fieldBlock.getValueCount(p)) {
     *                 case 0 -> { result.appendNull(); continue position; }
     *                 case 1 -> { break; }
     *                 default -> {
     *                     warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"));
     *                     result.appendNull();
     *                     continue position;
     *                 }
     *             }
     *             int field = fieldBlock.getInt(fieldBlock.getFirstValueIndex(p));
     *             result.appendInt(Function.process(field));
     *         }
     *         return result.build();
     *     }
     * }
     * }</pre>
     * <p>
     * Reference: {@code EvaluatorImplementer#realEval(true)}, {@code StandardArgument#skipNull()}
     */
    private static class BlockEvalImplementation implements Implementation {
        private final EvaluatorSpec spec;

        BlockEvalImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                int paramCount = spec.parameterCount();
                Class<?> returnType = spec.returnType();
                String resultBlockType = getBlockTypeInternal(returnType);
                String builderType = getBuilderTypeInternal(returnType);

                // Count evaluated (non-fixed) parameters - only these are method parameters
                int evaluatedParamCount = spec.evaluatedParameterCount();

                // Special case: zero evaluated parameters (all fixed, or no params like PI(), E(), NOW())
                // No input blocks to check for nulls/multi-values, just loop and call process()
                if (evaluatedParamCount == 0) {
                    return generateZeroParamBlockEval(methodVisitor, className, returnType, resultBlockType, builderType);
                }

                // Local variables layout (for evaluatedParamCount >= 1):
                // 0: this
                // 1: positionCount
                // 2 to 2+N-1: blocks[0..N-1] (method parameters, only evaluated params)
                // 2+N: result (builder)
                // 2+N+1: p (loop counter)
                // 2+N+2: valueCount (temp)
                // 2+N+3 to 2+N+3+scratchCount-1: scratch objects for BytesRef evaluated parameters
                // 2+N+3+scratchCount: builtBlock

                int blocksStartSlot = 2;
                int resultSlot = blocksStartSlot + evaluatedParamCount;
                int pSlot = resultSlot + 1;
                int valueCountSlot = pSlot + 1;

                // Count how many scratch objects we need (one per BytesRef evaluated parameter)
                int scratchCount = 0;
                int[] scratchSlots = new int[evaluatedParamCount]; // Only for evaluated params
                int evalIdx = 0;
                for (int i = 0; i < paramCount; i++) {
                    if (spec.isFixed(i) == false) {
                        if (requiresScratch(spec.parameterType(i))) {
                            scratchSlots[evalIdx] = valueCountSlot + 1 + scratchCount;
                            scratchCount++;
                        } else {
                            scratchSlots[evalIdx] = -1;
                        }
                        evalIdx++;
                    }
                }
                int builtBlockSlot = valueCountSlot + 1 + scratchCount;

                // Create result builder
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "driverContext", Type.getDescriptor(DriverContext.class));
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    Type.getInternalName(DriverContext.class),
                    "blockFactory",
                    "()Lorg/elasticsearch/compute/data/BlockFactory;",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 1);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/data/BlockFactory",
                    getBlockBuilderFactoryMethod(returnType),
                    getBlockBuilderFactoryDescriptor(returnType),
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, resultSlot);

                // p = 0
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, pSlot);

                // valueCount = 0 (initialize)
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, valueCountSlot);

                // Initialize scratch objects for BytesRef evaluated parameters
                for (int i = 0; i < evaluatedParamCount; i++) {
                    if (scratchSlots[i] >= 0) {
                        // new BytesRef()
                        methodVisitor.visitTypeInsn(Opcodes.NEW, "org/apache/lucene/util/BytesRef");
                        methodVisitor.visitInsn(Opcodes.DUP);
                        methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/lucene/util/BytesRef", "<init>", "()V", false);
                        methodVisitor.visitVarInsn(Opcodes.ASTORE, scratchSlots[i]);
                    }
                }

                // Set up try-finally for proper resource cleanup (after all locals are initialized)
                Label tryStart = new Label();
                Label tryEnd = new Label();
                Label finallyHandler = new Label();
                Label afterFinally = new Label();
                int exceptionSlot = builtBlockSlot + 1;

                methodVisitor.visitLabel(tryStart);

                Label loopStart = new Label();
                Label loopEnd = new Label();

                // Build full frame locals array for F_FULL
                // Full frame includes all locals: this, positionCount, blocks[0..N-1], result, p, valueCount, scratches...
                // Only evaluated params are in blocks
                int fullLocalsCount = 2 + evaluatedParamCount + 3 + scratchCount;
                Object[] fullFrameLocals = new Object[fullLocalsCount];
                fullFrameLocals[0] = className;
                fullFrameLocals[1] = Opcodes.INTEGER; // positionCount
                evalIdx = 0;
                for (int i = 0; i < paramCount; i++) {
                    if (spec.isFixed(i) == false) {
                        fullFrameLocals[2 + evalIdx] = getBlockTypeInternal(spec.parameterType(i));
                        evalIdx++;
                    }
                }
                fullFrameLocals[2 + evaluatedParamCount] = builderType;
                fullFrameLocals[2 + evaluatedParamCount + 1] = Opcodes.INTEGER; // p
                fullFrameLocals[2 + evaluatedParamCount + 2] = Opcodes.INTEGER; // valueCount
                for (int i = 0; i < scratchCount; i++) {
                    fullFrameLocals[2 + evaluatedParamCount + 3 + i] = "org/apache/lucene/util/BytesRef";
                }

                methodVisitor.visitLabel(loopStart);
                methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 0, new Object[] {});

                // if (p >= positionCount) goto loopEnd
                methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 1);
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, loopEnd);

                // For each block, implement the switch-case pattern from StandardArgument.skipNull():
                // switch (block.getValueCount(p)) {
                // case 0 -> { result.appendNull(); continue position; }
                // case 1 -> { break; } // process normally
                // default -> { warnings().registerException(...); result.appendNull(); continue position; }
                // }

                Label processValue = new Label();
                Label appendNullForNull = new Label();  // valueCount == 0
                Label appendNullForMultiValue = new Label();  // valueCount > 1
                Label continueLoop = new Label();

                // Check each evaluated block's value count using switch-case pattern
                // Only check evaluated (non-fixed) parameters - fixed params don't have blocks
                evalIdx = 0;
                for (int i = 0; i < paramCount; i++) {
                    if (spec.isFixed(i)) {
                        continue; // Skip fixed parameters - they don't have blocks
                    }

                    String blockType = getBlockTypeInternal(spec.parameterType(i));

                    // Get valueCount for this block
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, blocksStartSlot + evalIdx);
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "getValueCount", "(I)I", true);
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, valueCountSlot);

                    // switch (valueCount)
                    // case 0: goto appendNullForNull
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, valueCountSlot);
                    methodVisitor.visitJumpInsn(Opcodes.IFEQ, appendNullForNull);

                    // case 1: continue to next block check (or processValue if last)
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, valueCountSlot);
                    methodVisitor.visitInsn(Opcodes.ICONST_1);
                    methodVisitor.visitJumpInsn(Opcodes.IF_ICMPNE, appendNullForMultiValue);

                    // valueCount == 1, continue to next block or processValue
                    evalIdx++;
                }
                // All evaluated blocks have valueCount == 1, go to processValue
                methodVisitor.visitJumpInsn(Opcodes.GOTO, processValue);

                // Handle null (valueCount == 0) - just append null, no warning
                methodVisitor.visitLabel(appendNullForNull);
                methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 0, new Object[] {});
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderType, "appendNull", "()L" + builderType + ";", true);
                methodVisitor.visitInsn(Opcodes.POP);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, continueLoop);

                // Handle multi-value (valueCount > 1) - register warning then append null
                methodVisitor.visitLabel(appendNullForMultiValue);
                methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 0, new Object[] {});

                // warnings().registerException(new IllegalArgumentException("single-value function encountered multi-value"))
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    className,
                    "warnings",
                    "()" + Type.getDescriptor(Warnings.class),
                    false
                );
                methodVisitor.visitTypeInsn(Opcodes.NEW, Type.getInternalName(IllegalArgumentException.class));
                methodVisitor.visitInsn(Opcodes.DUP);
                methodVisitor.visitLdcInsn("single-value function encountered multi-value");
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKESPECIAL,
                    Type.getInternalName(IllegalArgumentException.class),
                    "<init>",
                    "(Ljava/lang/String;)V",
                    false
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    Type.getInternalName(Warnings.class),
                    "registerException",
                    "(Ljava/lang/Exception;)V",
                    false
                );

                // Append null
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderType, "appendNull", "()L" + builderType + ";", true);
                methodVisitor.visitInsn(Opcodes.POP);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, continueLoop);

                // Process value - all blocks have exactly one value
                methodVisitor.visitLabel(processValue);
                methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 0, new Object[] {});

                // Load result builder
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);

                // Load values for process method in the correct order
                // For evaluated params: extract from block
                // For fixed params: load from this.fieldN
                evalIdx = 0;
                for (int i = 0; i < paramCount; i++) {
                    Class<?> paramType = spec.parameterType(i);

                    if (spec.isFixed(i)) {
                        // Fixed parameter: load from this.fieldN
                        methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                        methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "field" + i, Type.getDescriptor(paramType));
                    } else {
                        // Evaluated parameter: extract from block
                        String blockType = getBlockTypeInternal(paramType);

                        methodVisitor.visitVarInsn(Opcodes.ALOAD, blocksStartSlot + evalIdx);
                        methodVisitor.visitVarInsn(Opcodes.ALOAD, blocksStartSlot + evalIdx);
                        methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
                        methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "getFirstValueIndex", "(I)I", true);

                        // For BytesRef, we need to pass the scratch object
                        if (requiresScratch(paramType)) {
                            methodVisitor.visitVarInsn(Opcodes.ALOAD, scratchSlots[evalIdx]);
                        }

                        methodVisitor.visitMethodInsn(
                            Opcodes.INVOKEINTERFACE,
                            blockType,
                            getBlockGetterName(paramType),
                            getBlockGetterDescriptor(paramType),
                            true
                        );
                        evalIdx++;
                    }
                }

                // Call process method (with optional try-catch for warnExceptions)
                Method processMethod;
                try {
                    processMethod = spec.processMethod();
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException("Failed to find process method", e);
                }

                Label warnTryStart = null;
                Label warnTryEnd = null;
                Label warnCatchStart = null;

                if (spec.hasWarnExceptions()) {
                    warnTryStart = new Label();
                    warnTryEnd = new Label();
                    warnCatchStart = new Label();
                    methodVisitor.visitLabel(warnTryStart);
                }

                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    Type.getInternalName(processMethod.getDeclaringClass()),
                    processMethod.getName(),
                    Type.getMethodDescriptor(processMethod),
                    false
                );

                // Append result
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    builderType,
                    getAppendMethod(returnType),
                    getAppendDescriptor(returnType),
                    true
                );
                methodVisitor.visitInsn(Opcodes.POP);

                if (spec.hasWarnExceptions()) {
                    methodVisitor.visitLabel(warnTryEnd);
                    methodVisitor.visitJumpInsn(Opcodes.GOTO, continueLoop);

                    // Catch block
                    methodVisitor.visitLabel(warnCatchStart);
                    methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 1, new Object[] { "java/lang/Exception" });

                    // Store exception in a local (reuse builtBlockSlot temporarily)
                    int warnExceptionSlot = builtBlockSlot;
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, warnExceptionSlot);

                    // warnings().registerException(e)
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        className,
                        "warnings",
                        "()L" + Type.getInternalName(Warnings.class) + ";",
                        false
                    );
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, warnExceptionSlot);
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        Type.getInternalName(Warnings.class),
                        "registerException",
                        "(Ljava/lang/Exception;)V",
                        false
                    );

                    // result.appendNull()
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderType, "appendNull", "()L" + builderType + ";", true);
                    methodVisitor.visitInsn(Opcodes.POP);

                    // Register exception handlers for each warnException type
                    for (Class<? extends Exception> exceptionType : spec.warnExceptions()) {
                        methodVisitor.visitTryCatchBlock(warnTryStart, warnTryEnd, warnCatchStart, Type.getInternalName(exceptionType));
                    }
                }

                // Continue loop
                methodVisitor.visitLabel(continueLoop);
                methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 0, new Object[] {});
                methodVisitor.visitIincInsn(pSlot, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, loopStart);

                // Loop end
                methodVisitor.visitLabel(loopEnd);
                methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 0, new Object[] {});

                // Build result
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderType, "build", "()L" + resultBlockType + ";", true);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, builtBlockSlot);

                methodVisitor.visitLabel(tryEnd);

                // Close builder (no-op after build, but required for try-finally pattern)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Releasable.class), "close", "()V", true);

                // Return
                methodVisitor.visitVarInsn(Opcodes.ALOAD, builtBlockSlot);
                methodVisitor.visitInsn(Opcodes.ARETURN);

                // Finally handler - catches any throwable, closes builder, re-throws
                methodVisitor.visitLabel(finallyHandler);
                methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 1, new Object[] { "java/lang/Throwable" });
                methodVisitor.visitVarInsn(Opcodes.ASTORE, exceptionSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Releasable.class), "close", "()V", true);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, exceptionSlot);
                methodVisitor.visitInsn(Opcodes.ATHROW);

                // Register the try-finally handler (null type catches all throwables)
                methodVisitor.visitTryCatchBlock(tryStart, tryEnd, finallyHandler, null);

                return new ByteCodeAppender.Size(paramCount + 6, exceptionSlot + 1);
            };
        }

        /**
         * Generates block evaluation for zero-parameter functions.
         * Simply loops from 0 to positionCount and calls process() for each position.
         */
        private ByteCodeAppender.Size generateZeroParamBlockEval(
            MethodVisitor methodVisitor,
            String className,
            Class<?> returnType,
            String resultBlockType,
            String builderType
        ) {
            // Local variables layout:
            // 0: this
            // 1: positionCount
            // 2: result (builder)
            // 3: p (loop counter)
            // 4: builtBlock
            // 5: exception (for finally handler)

            int resultSlot = 2;
            int pSlot = 3;
            int builtBlockSlot = 4;
            int exceptionSlot = 5;

            // Create result builder
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
            methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "driverContext", Type.getDescriptor(DriverContext.class));
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(DriverContext.class),
                "blockFactory",
                "()Lorg/elasticsearch/compute/data/BlockFactory;",
                false
            );
            methodVisitor.visitVarInsn(Opcodes.ILOAD, 1);
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                "org/elasticsearch/compute/data/BlockFactory",
                getBlockBuilderFactoryMethod(returnType),
                getBlockBuilderFactoryDescriptor(returnType),
                false
            );
            methodVisitor.visitVarInsn(Opcodes.ASTORE, resultSlot);

            // p = 0
            methodVisitor.visitInsn(Opcodes.ICONST_0);
            methodVisitor.visitVarInsn(Opcodes.ISTORE, pSlot);

            // Set up try-finally for proper resource cleanup (after all locals are initialized)
            Label tryStart = new Label();
            Label tryEnd = new Label();
            Label finallyHandler = new Label();

            methodVisitor.visitLabel(tryStart);

            Label loopStart = new Label();
            Label loopEnd = new Label();

            // Frame locals: this, positionCount, result, p
            Object[] fullFrameLocals = new Object[] { className, Opcodes.INTEGER, builderType, Opcodes.INTEGER };

            methodVisitor.visitLabel(loopStart);
            methodVisitor.visitFrame(Opcodes.F_FULL, 4, fullFrameLocals, 0, new Object[] {});

            // if (p >= positionCount) goto loopEnd
            methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, 1);
            methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, loopEnd);

            // result.appendXxx(process())
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);

            // Call process method with no arguments
            Method processMethod;
            try {
                processMethod = spec.processMethod();
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Failed to find process method", e);
            }
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKESTATIC,
                Type.getInternalName(processMethod.getDeclaringClass()),
                processMethod.getName(),
                Type.getMethodDescriptor(processMethod),
                false
            );

            // Append result
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEINTERFACE,
                builderType,
                getAppendMethod(returnType),
                getAppendDescriptor(returnType),
                true
            );
            methodVisitor.visitInsn(Opcodes.POP);

            // p++
            methodVisitor.visitIincInsn(pSlot, 1);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, loopStart);

            // Loop end
            methodVisitor.visitLabel(loopEnd);
            methodVisitor.visitFrame(Opcodes.F_FULL, 4, fullFrameLocals, 0, new Object[] {});

            // Build result
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderType, "build", "()L" + resultBlockType + ";", true);
            methodVisitor.visitVarInsn(Opcodes.ASTORE, builtBlockSlot);

            methodVisitor.visitLabel(tryEnd);

            // Close builder (no-op after build, but required for try-finally pattern)
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Releasable.class), "close", "()V", true);

            // Return
            methodVisitor.visitVarInsn(Opcodes.ALOAD, builtBlockSlot);
            methodVisitor.visitInsn(Opcodes.ARETURN);

            // Finally handler - catches any throwable, closes builder, re-throws
            methodVisitor.visitLabel(finallyHandler);
            methodVisitor.visitFrame(Opcodes.F_FULL, 4, fullFrameLocals, 1, new Object[] { "java/lang/Throwable" });
            methodVisitor.visitVarInsn(Opcodes.ASTORE, exceptionSlot);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Releasable.class), "close", "()V", true);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, exceptionSlot);
            methodVisitor.visitInsn(Opcodes.ATHROW);

            // Register the try-finally handler (null type catches all throwables)
            methodVisitor.visitTryCatchBlock(tryStart, tryEnd, finallyHandler, null);

            return new ByteCodeAppender.Size(4, exceptionSlot + 1);
        }

        private String getBlockTypeInternal(Class<?> fieldType) {
            if (fieldType == double.class) return "org/elasticsearch/compute/data/DoubleBlock";
            if (fieldType == long.class) return "org/elasticsearch/compute/data/LongBlock";
            if (fieldType == int.class) return "org/elasticsearch/compute/data/IntBlock";
            if (fieldType == boolean.class) return "org/elasticsearch/compute/data/BooleanBlock";
            if (fieldType == BytesRef.class) return "org/elasticsearch/compute/data/BytesRefBlock";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getBuilderTypeInternal(Class<?> fieldType) {
            if (fieldType == double.class) return "org/elasticsearch/compute/data/DoubleBlock$Builder";
            if (fieldType == long.class) return "org/elasticsearch/compute/data/LongBlock$Builder";
            if (fieldType == int.class) return "org/elasticsearch/compute/data/IntBlock$Builder";
            if (fieldType == boolean.class) return "org/elasticsearch/compute/data/BooleanBlock$Builder";
            if (fieldType == BytesRef.class) return "org/elasticsearch/compute/data/BytesRefBlock$Builder";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getBlockBuilderFactoryMethod(Class<?> fieldType) {
            if (fieldType == double.class) return "newDoubleBlockBuilder";
            if (fieldType == long.class) return "newLongBlockBuilder";
            if (fieldType == int.class) return "newIntBlockBuilder";
            if (fieldType == boolean.class) return "newBooleanBlockBuilder";
            if (fieldType == BytesRef.class) return "newBytesRefBlockBuilder";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getBlockBuilderFactoryDescriptor(Class<?> fieldType) {
            String builderType = getBuilderTypeInternal(fieldType);
            return "(I)L" + builderType + ";";
        }

        private String getBlockGetterName(Class<?> fieldType) {
            if (fieldType == double.class) return "getDouble";
            if (fieldType == long.class) return "getLong";
            if (fieldType == int.class) return "getInt";
            if (fieldType == boolean.class) return "getBoolean";
            if (fieldType == BytesRef.class) return "getBytesRef";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getBlockGetterDescriptor(Class<?> fieldType) {
            if (fieldType == double.class) return "(I)D";
            if (fieldType == long.class) return "(I)J";
            if (fieldType == int.class) return "(I)I";
            if (fieldType == boolean.class) return "(I)Z";
            if (fieldType == BytesRef.class) return "(ILorg/apache/lucene/util/BytesRef;)Lorg/apache/lucene/util/BytesRef;";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getAppendMethod(Class<?> fieldType) {
            if (fieldType == double.class) return "appendDouble";
            if (fieldType == long.class) return "appendLong";
            if (fieldType == int.class) return "appendInt";
            if (fieldType == boolean.class) return "appendBoolean";
            if (fieldType == BytesRef.class) return "appendBytesRef";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getAppendDescriptor(Class<?> fieldType) {
            String builderType = getBuilderTypeInternal(fieldType);
            if (fieldType == double.class) return "(D)L" + builderType + ";";
            if (fieldType == long.class) return "(J)L" + builderType + ";";
            if (fieldType == int.class) return "(I)L" + builderType + ";";
            if (fieldType == boolean.class) return "(Z)L" + builderType + ";";
            if (fieldType == BytesRef.class) return "(Lorg/apache/lucene/util/BytesRef;)L" + builderType + ";";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        /**
         * Returns true if the field type requires a scratch object for reading (e.g., BytesRef)
         */
        private boolean requiresScratch(Class<?> fieldType) {
            return fieldType == BytesRef.class;
        }
    }

    /**
     * Implements eval(int positionCount, Vector...) - vector-style evaluation (optimized path) for N-ary functions.
     * <p>
     * This is the fast path when all input blocks can be converted to vectors (no nulls or multi-values).
     * Uses FixedBuilder for primitive types for better performance.
     * <p>
     * Generates equivalent bytecode to:
     * <pre>{@code
     * public IntVector eval(int positionCount, IntVector fieldVector) {
     *     try (IntVector.FixedBuilder result = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
     *         for (int p = 0; p < positionCount; p++) {
     *             int field = fieldVector.getInt(p);
     *             result.appendInt(p, Function.process(field));
     *         }
     *         return result.build();
     *     }
     * }
     * }</pre>
     * <p>
     * Reference: {@code EvaluatorImplementer#realEval(false)}, {@code StandardArgument#read()}
     */
    private static class VectorEvalImplementation implements Implementation {
        private final EvaluatorSpec spec;

        VectorEvalImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                int paramCount = spec.parameterCount();
                Class<?> returnType = spec.returnType();
                String resultBlockType = getBlockTypeInternal(returnType);
                String resultVectorType = getVectorTypeInternal(returnType);

                // When warnExceptions is non-empty, we need to use Block.Builder instead of Vector.FixedBuilder
                // because we might need to append nulls when exceptions occur
                if (spec.hasWarnExceptions()) {
                    return generateVectorEvalWithWarnExceptions(methodVisitor, className, paramCount, returnType, resultBlockType);
                }

                // BytesRef requires a different approach - use builder instead of array
                if (returnType == BytesRef.class) {
                    return generateBytesRefVectorEval(methodVisitor, className, paramCount, resultBlockType, resultVectorType);
                }

                // Primitive types use array-based approach
                return generatePrimitiveVectorEval(methodVisitor, className, paramCount, returnType, resultBlockType, resultVectorType);
            };
        }

        private ByteCodeAppender.Size generatePrimitiveVectorEval(
            MethodVisitor methodVisitor,
            String className,
            int paramCount,
            Class<?> returnType,
            String resultBlockType,
            String resultVectorType
        ) {
            // Uses FixedBuilder for better performance (matches compile-time generated code)
            // Count evaluated (non-fixed) parameters - only these are method parameters
            int evaluatedParamCount = spec.evaluatedParameterCount();

            // Local variables layout:
            // 0: this
            // 1: positionCount
            // 2 to 2+N-1: vectors[0..N-1] (method parameters, only evaluated params)
            // 2+N: result (FixedBuilder)
            // 2+N+1: p (loop counter)
            // 2+N+2 to 2+N+2+scratchCount-1: scratch objects for BytesRef evaluated parameters
            // 2+N+2+scratchCount: builtVector

            int vectorsStartSlot = 2;
            int resultSlot = vectorsStartSlot + evaluatedParamCount;
            int pSlot = resultSlot + 1;

            // Count scratch objects needed for BytesRef evaluated parameters
            int scratchCount = 0;
            int[] scratchSlots = new int[evaluatedParamCount];
            int evalIdx = 0;
            for (int i = 0; i < paramCount; i++) {
                if (spec.isFixed(i) == false) {
                    if (requiresScratch(spec.parameterType(i))) {
                        scratchSlots[evalIdx] = pSlot + 1 + scratchCount;
                        scratchCount++;
                    } else {
                        scratchSlots[evalIdx] = -1;
                    }
                    evalIdx++;
                }
            }
            int builtVectorSlot = pSlot + 1 + scratchCount;
            int exceptionSlot = builtVectorSlot + 1;
            int maxLocals = exceptionSlot + 1;

            String fixedBuilderType = getFixedBuilderTypeInternal(returnType);

            // Create FixedBuilder: result = driverContext.blockFactory().newXxxVectorFixedBuilder(positionCount)
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
            methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "driverContext", Type.getDescriptor(DriverContext.class));
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(DriverContext.class),
                "blockFactory",
                "()Lorg/elasticsearch/compute/data/BlockFactory;",
                false
            );
            methodVisitor.visitVarInsn(Opcodes.ILOAD, 1); // positionCount
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                "org/elasticsearch/compute/data/BlockFactory",
                getFixedBuilderFactoryMethod(returnType),
                getFixedBuilderFactoryDescriptor(returnType),
                false
            );
            methodVisitor.visitVarInsn(Opcodes.ASTORE, resultSlot);

            // p = 0
            methodVisitor.visitInsn(Opcodes.ICONST_0);
            methodVisitor.visitVarInsn(Opcodes.ISTORE, pSlot);

            // Initialize scratch objects for BytesRef evaluated parameters
            for (int i = 0; i < evaluatedParamCount; i++) {
                if (scratchSlots[i] >= 0) {
                    methodVisitor.visitTypeInsn(Opcodes.NEW, "org/apache/lucene/util/BytesRef");
                    methodVisitor.visitInsn(Opcodes.DUP);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/lucene/util/BytesRef", "<init>", "()V", false);
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, scratchSlots[i]);
                }
            }

            // Set up try-finally for proper resource cleanup (after all locals are initialized)
            Label tryStart = new Label();
            Label tryEnd = new Label();
            Label finallyHandler = new Label();

            methodVisitor.visitLabel(tryStart);

            Label loopStart = new Label();
            Label loopEnd = new Label();

            // Build full frame locals array for F_FULL
            int fullLocalsCount = 2 + evaluatedParamCount + 2 + scratchCount;
            Object[] fullFrameLocals = new Object[fullLocalsCount];
            fullFrameLocals[0] = className;
            fullFrameLocals[1] = Opcodes.INTEGER; // positionCount
            evalIdx = 0;
            for (int i = 0; i < paramCount; i++) {
                if (spec.isFixed(i) == false) {
                    fullFrameLocals[2 + evalIdx] = getVectorTypeInternal(spec.parameterType(i));
                    evalIdx++;
                }
            }
            fullFrameLocals[2 + evaluatedParamCount] = fixedBuilderType;
            fullFrameLocals[2 + evaluatedParamCount + 1] = Opcodes.INTEGER; // p
            for (int i = 0; i < scratchCount; i++) {
                fullFrameLocals[2 + evaluatedParamCount + 2 + i] = "org/apache/lucene/util/BytesRef";
            }

            methodVisitor.visitLabel(loopStart);
            methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 0, new Object[] {});

            // if (p >= positionCount) goto loopEnd
            methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, 1);
            methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, loopEnd);

            // result.appendXxx(p, process(vector0.getXxx(p), vector1.getXxx(p), ...))
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot); // position argument for appendXxx(p, value)

            // Load values for process method in the correct order
            // For evaluated params: get from vector
            // For fixed params: load from this.fieldN
            evalIdx = 0;
            for (int j = 0; j < paramCount; j++) {
                Class<?> paramType = spec.parameterType(j);

                if (spec.isFixed(j)) {
                    // Fixed parameter: load from this.fieldN
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "field" + j, Type.getDescriptor(paramType));
                } else {
                    // Evaluated parameter: get from vector
                    String vectorType = getVectorTypeInternal(paramType);

                    methodVisitor.visitVarInsn(Opcodes.ALOAD, vectorsStartSlot + evalIdx);
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);

                    // For BytesRef parameters, pass the scratch object
                    if (requiresScratch(paramType)) {
                        methodVisitor.visitVarInsn(Opcodes.ALOAD, scratchSlots[evalIdx]);
                    }

                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEINTERFACE,
                        vectorType,
                        getVectorGetterName(paramType),
                        getVectorGetterDescriptor(paramType),
                        true
                    );
                    evalIdx++;
                }
            }

            // Call process method (this is now outside the loop that loads values)
            Method processMethod;
            try {
                processMethod = spec.processMethod();
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Failed to find process method", e);
            }
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKESTATIC,
                Type.getInternalName(processMethod.getDeclaringClass()),
                processMethod.getName(),
                Type.getMethodDescriptor(processMethod),
                false
            );

            // Append result using FixedBuilder's appendXxx(position, value)
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEINTERFACE,
                fixedBuilderType,
                getFixedBuilderAppendMethod(returnType),
                getFixedBuilderAppendDescriptor(returnType),
                true
            );
            // Pop the return value (builder for chaining) - we don't need it
            methodVisitor.visitInsn(Opcodes.POP);

            // p++
            methodVisitor.visitIincInsn(pSlot, 1);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, loopStart);

            // Loop end
            methodVisitor.visitLabel(loopEnd);
            methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 0, new Object[] {});

            // Build result
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, fixedBuilderType, "build", "()L" + resultVectorType + ";", true);

            // Convert to block: .asBlock()
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, resultVectorType, "asBlock", "()L" + resultBlockType + ";", true);
            methodVisitor.visitVarInsn(Opcodes.ASTORE, builtVectorSlot);

            methodVisitor.visitLabel(tryEnd);

            // Close builder (no-op after build, but required for try-finally pattern)
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Releasable.class), "close", "()V", true);

            // Return
            methodVisitor.visitVarInsn(Opcodes.ALOAD, builtVectorSlot);
            methodVisitor.visitInsn(Opcodes.ARETURN);

            // Finally handler - catches any throwable, closes builder, re-throws
            methodVisitor.visitLabel(finallyHandler);
            methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 1, new Object[] { "java/lang/Throwable" });
            methodVisitor.visitVarInsn(Opcodes.ASTORE, exceptionSlot);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Releasable.class), "close", "()V", true);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, exceptionSlot);
            methodVisitor.visitInsn(Opcodes.ATHROW);

            // Register the try-finally handler (null type catches all throwables)
            methodVisitor.visitTryCatchBlock(tryStart, tryEnd, finallyHandler, null);

            return new ByteCodeAppender.Size(evaluatedParamCount + 5, maxLocals);
        }

        /**
         * Generates vector evaluation with warnExceptions support.
         * Uses Block.Builder instead of Vector.FixedBuilder because we might need to append nulls.
         * Wraps the process method call in try-catch for each exception type.
         *
         * Reference: EvaluatorImplementer.realEval() when warnExceptions is non-empty
         */
        private ByteCodeAppender.Size generateVectorEvalWithWarnExceptions(
            MethodVisitor methodVisitor,
            String className,
            int paramCount,
            Class<?> returnType,
            String resultBlockType
        ) {
            int evaluatedParamCount = spec.evaluatedParameterCount();
            String builderType = getBuilderTypeInternal(returnType);

            // Local variables layout:
            // 0: this
            // 1: positionCount
            // 2 to 2+N-1: vectors[0..N-1] (method parameters, only evaluated params)
            // 2+N: result (Block.Builder)
            // 2+N+1: p (loop counter)
            // 2+N+2 to 2+N+2+scratchCount-1: scratch objects for BytesRef evaluated parameters
            // 2+N+2+scratchCount: builtBlock

            int vectorsStartSlot = 2;
            int resultSlot = vectorsStartSlot + evaluatedParamCount;
            int pSlot = resultSlot + 1;

            // Count scratch objects needed for BytesRef evaluated parameters
            int scratchCount = 0;
            int[] scratchSlots = new int[evaluatedParamCount];
            int evalIdx = 0;
            for (int i = 0; i < paramCount; i++) {
                if (spec.isFixed(i) == false) {
                    if (requiresScratch(spec.parameterType(i))) {
                        scratchSlots[evalIdx] = pSlot + 1 + scratchCount;
                        scratchCount++;
                    } else {
                        scratchSlots[evalIdx] = -1;
                    }
                    evalIdx++;
                }
            }
            int builtBlockSlot = pSlot + 1 + scratchCount;
            int finallyExceptionSlot = builtBlockSlot + 1;
            int maxLocals = finallyExceptionSlot + 1;

            // Create Block.Builder: result = driverContext.blockFactory().newXxxBlockBuilder(positionCount)
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
            methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "driverContext", Type.getDescriptor(DriverContext.class));
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(DriverContext.class),
                "blockFactory",
                "()Lorg/elasticsearch/compute/data/BlockFactory;",
                false
            );
            methodVisitor.visitVarInsn(Opcodes.ILOAD, 1); // positionCount
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                "org/elasticsearch/compute/data/BlockFactory",
                getBlockBuilderFactoryMethod(returnType),
                getBlockBuilderFactoryDescriptor(returnType),
                false
            );
            methodVisitor.visitVarInsn(Opcodes.ASTORE, resultSlot);

            // p = 0
            methodVisitor.visitInsn(Opcodes.ICONST_0);
            methodVisitor.visitVarInsn(Opcodes.ISTORE, pSlot);

            // Initialize scratch objects for BytesRef evaluated parameters
            for (int i = 0; i < evaluatedParamCount; i++) {
                if (scratchSlots[i] >= 0) {
                    methodVisitor.visitTypeInsn(Opcodes.NEW, "org/apache/lucene/util/BytesRef");
                    methodVisitor.visitInsn(Opcodes.DUP);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/lucene/util/BytesRef", "<init>", "()V", false);
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, scratchSlots[i]);
                }
            }

            // Set up try-finally for proper resource cleanup (after all locals are initialized)
            Label outerTryStart = new Label();
            Label outerTryEnd = new Label();
            Label finallyHandler = new Label();

            methodVisitor.visitLabel(outerTryStart);

            Label loopStart = new Label();
            Label loopEnd = new Label();
            Label continueLoop = new Label();

            // Build full frame locals array for F_FULL
            int fullLocalsCount = 2 + evaluatedParamCount + 2 + scratchCount;
            Object[] fullFrameLocals = new Object[fullLocalsCount];
            fullFrameLocals[0] = className;
            fullFrameLocals[1] = Opcodes.INTEGER; // positionCount
            evalIdx = 0;
            for (int i = 0; i < paramCount; i++) {
                if (spec.isFixed(i) == false) {
                    fullFrameLocals[2 + evalIdx] = getVectorTypeInternal(spec.parameterType(i));
                    evalIdx++;
                }
            }
            fullFrameLocals[2 + evaluatedParamCount] = builderType;
            fullFrameLocals[2 + evaluatedParamCount + 1] = Opcodes.INTEGER; // p
            for (int i = 0; i < scratchCount; i++) {
                fullFrameLocals[2 + evaluatedParamCount + 2 + i] = "org/apache/lucene/util/BytesRef";
            }

            methodVisitor.visitLabel(loopStart);
            methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 0, new Object[] {});

            // if (p >= positionCount) goto loopEnd
            methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, 1);
            methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, loopEnd);

            // Try block start
            Label tryStart = new Label();
            Label tryEnd = new Label();
            Label catchStart = new Label();

            methodVisitor.visitLabel(tryStart);

            // result.appendXxx(process(vector0.getXxx(p), vector1.getXxx(p), ...))
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);

            // Load values for process method in the correct order
            evalIdx = 0;
            for (int j = 0; j < paramCount; j++) {
                Class<?> paramType = spec.parameterType(j);

                if (spec.isFixed(j)) {
                    // Fixed parameter: load from this.fieldN
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "field" + j, Type.getDescriptor(paramType));
                } else {
                    // Evaluated parameter: get from vector
                    String vectorType = getVectorTypeInternal(paramType);

                    methodVisitor.visitVarInsn(Opcodes.ALOAD, vectorsStartSlot + evalIdx);
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);

                    // For BytesRef parameters, pass the scratch object
                    if (requiresScratch(paramType)) {
                        methodVisitor.visitVarInsn(Opcodes.ALOAD, scratchSlots[evalIdx]);
                    }

                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEINTERFACE,
                        vectorType,
                        getVectorGetterName(paramType),
                        getVectorGetterDescriptor(paramType),
                        true
                    );
                    evalIdx++;
                }
            }

            // Call process method
            Method processMethod;
            try {
                processMethod = spec.processMethod();
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Failed to find process method", e);
            }
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKESTATIC,
                Type.getInternalName(processMethod.getDeclaringClass()),
                processMethod.getName(),
                Type.getMethodDescriptor(processMethod),
                false
            );

            // Append result using Block.Builder's appendXxx(value)
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEINTERFACE,
                builderType,
                getAppendMethod(returnType),
                getAppendDescriptor(returnType),
                true
            );
            methodVisitor.visitInsn(Opcodes.POP); // Pop the builder return value

            methodVisitor.visitLabel(tryEnd);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, continueLoop);

            // Catch block - catch each exception type
            methodVisitor.visitLabel(catchStart);
            methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 1, new Object[] { "java/lang/Exception" });

            // Store exception in a local (reuse builtBlockSlot temporarily)
            int exceptionSlot = builtBlockSlot;
            methodVisitor.visitVarInsn(Opcodes.ASTORE, exceptionSlot);

            // warnings().registerException(e)
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                className,
                "warnings",
                "()L" + Type.getInternalName(Warnings.class) + ";",
                false
            );
            methodVisitor.visitVarInsn(Opcodes.ALOAD, exceptionSlot);
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(Warnings.class),
                "registerException",
                "(Ljava/lang/Exception;)V",
                false
            );

            // result.appendNull()
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderType, "appendNull", "()L" + builderType + ";", true);
            methodVisitor.visitInsn(Opcodes.POP);

            // Continue loop
            methodVisitor.visitLabel(continueLoop);
            methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 0, new Object[] {});
            methodVisitor.visitIincInsn(pSlot, 1);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, loopStart);

            // Register exception handlers for each warnException type
            for (Class<? extends Exception> exceptionType : spec.warnExceptions()) {
                methodVisitor.visitTryCatchBlock(tryStart, tryEnd, catchStart, Type.getInternalName(exceptionType));
            }

            // Loop end
            methodVisitor.visitLabel(loopEnd);
            methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 0, new Object[] {});

            // Build result: result.build()
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderType, "build", "()L" + resultBlockType + ";", true);
            methodVisitor.visitVarInsn(Opcodes.ASTORE, builtBlockSlot);

            methodVisitor.visitLabel(outerTryEnd);

            // Close builder (no-op after build, but required for try-finally pattern)
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Releasable.class), "close", "()V", true);

            // Return
            methodVisitor.visitVarInsn(Opcodes.ALOAD, builtBlockSlot);
            methodVisitor.visitInsn(Opcodes.ARETURN);

            // Finally handler - catches any throwable, closes builder, re-throws
            methodVisitor.visitLabel(finallyHandler);
            methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 1, new Object[] { "java/lang/Throwable" });
            methodVisitor.visitVarInsn(Opcodes.ASTORE, finallyExceptionSlot);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Releasable.class), "close", "()V", true);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, finallyExceptionSlot);
            methodVisitor.visitInsn(Opcodes.ATHROW);

            // Register the try-finally handler (null type catches all throwables)
            methodVisitor.visitTryCatchBlock(outerTryStart, outerTryEnd, finallyHandler, null);

            return new ByteCodeAppender.Size(evaluatedParamCount + 6, maxLocals);
        }

        // Helper methods for Block.Builder (used by warnExceptions path)
        private String getBuilderTypeInternal(Class<?> fieldType) {
            if (fieldType == double.class) return "org/elasticsearch/compute/data/DoubleBlock$Builder";
            if (fieldType == long.class) return "org/elasticsearch/compute/data/LongBlock$Builder";
            if (fieldType == int.class) return "org/elasticsearch/compute/data/IntBlock$Builder";
            if (fieldType == boolean.class) return "org/elasticsearch/compute/data/BooleanBlock$Builder";
            if (fieldType == BytesRef.class) return "org/elasticsearch/compute/data/BytesRefBlock$Builder";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getBlockBuilderFactoryMethod(Class<?> fieldType) {
            if (fieldType == double.class) return "newDoubleBlockBuilder";
            if (fieldType == long.class) return "newLongBlockBuilder";
            if (fieldType == int.class) return "newIntBlockBuilder";
            if (fieldType == boolean.class) return "newBooleanBlockBuilder";
            if (fieldType == BytesRef.class) return "newBytesRefBlockBuilder";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getBlockBuilderFactoryDescriptor(Class<?> fieldType) {
            String builderType = getBuilderTypeInternal(fieldType);
            return "(I)L" + builderType + ";";
        }

        private String getAppendMethod(Class<?> fieldType) {
            if (fieldType == double.class) return "appendDouble";
            if (fieldType == long.class) return "appendLong";
            if (fieldType == int.class) return "appendInt";
            if (fieldType == boolean.class) return "appendBoolean";
            if (fieldType == BytesRef.class) return "appendBytesRef";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getAppendDescriptor(Class<?> fieldType) {
            String builderType = getBuilderTypeInternal(fieldType);
            if (fieldType == double.class) return "(D)L" + builderType + ";";
            if (fieldType == long.class) return "(J)L" + builderType + ";";
            if (fieldType == int.class) return "(I)L" + builderType + ";";
            if (fieldType == boolean.class) return "(Z)L" + builderType + ";";
            if (fieldType == BytesRef.class) return "(Lorg/apache/lucene/util/BytesRef;)L" + builderType + ";";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        // Helper methods for FixedBuilder
        private String getFixedBuilderTypeInternal(Class<?> fieldType) {
            if (fieldType == double.class) return "org/elasticsearch/compute/data/DoubleVector$FixedBuilder";
            if (fieldType == long.class) return "org/elasticsearch/compute/data/LongVector$FixedBuilder";
            if (fieldType == int.class) return "org/elasticsearch/compute/data/IntVector$FixedBuilder";
            if (fieldType == boolean.class) return "org/elasticsearch/compute/data/BooleanVector$FixedBuilder";
            throw new IllegalArgumentException("Unsupported field type for FixedBuilder: " + fieldType);
        }

        private String getFixedBuilderFactoryMethod(Class<?> fieldType) {
            if (fieldType == double.class) return "newDoubleVectorFixedBuilder";
            if (fieldType == long.class) return "newLongVectorFixedBuilder";
            if (fieldType == int.class) return "newIntVectorFixedBuilder";
            if (fieldType == boolean.class) return "newBooleanVectorFixedBuilder";
            throw new IllegalArgumentException("Unsupported field type for FixedBuilder: " + fieldType);
        }

        private String getFixedBuilderFactoryDescriptor(Class<?> fieldType) {
            String builderType = getFixedBuilderTypeInternal(fieldType);
            return "(I)L" + builderType + ";";
        }

        private String getFixedBuilderAppendMethod(Class<?> fieldType) {
            if (fieldType == double.class) return "appendDouble";
            if (fieldType == long.class) return "appendLong";
            if (fieldType == int.class) return "appendInt";
            if (fieldType == boolean.class) return "appendBoolean";
            throw new IllegalArgumentException("Unsupported field type for FixedBuilder: " + fieldType);
        }

        private String getFixedBuilderAppendDescriptor(Class<?> fieldType) {
            String builderType = getFixedBuilderTypeInternal(fieldType);
            if (fieldType == double.class) return "(ID)L" + builderType + ";";
            if (fieldType == long.class) return "(IJ)L" + builderType + ";";
            if (fieldType == int.class) return "(II)L" + builderType + ";";
            if (fieldType == boolean.class) return "(IZ)L" + builderType + ";";
            throw new IllegalArgumentException("Unsupported field type for FixedBuilder: " + fieldType);
        }

        private ByteCodeAppender.Size generateBytesRefVectorEval(
            MethodVisitor methodVisitor,
            String className,
            int paramCount,
            String resultBlockType,
            String resultVectorType
        ) {
            // For BytesRef return type, use builder pattern:
            // try (BytesRefVector.Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
            // BytesRef scratch = new BytesRef();
            // for (int p = 0; p < positionCount; p++) {
            // BytesRef val = vector.getBytesRef(p, scratch);
            // result.appendBytesRef(process(val));
            // }
            // return result.build();
            // }

            // Count evaluated (non-fixed) parameters - only these are method parameters
            int evaluatedParamCount = spec.evaluatedParameterCount();

            // Local variables layout:
            // 0: this
            // 1: positionCount
            // 2 to 2+N-1: vectors[0..N-1] (method parameters, only evaluated params)
            // 2+N: result (builder)
            // 2+N+1: p (loop counter)
            // 2+N+2 to 2+N+2+scratchCount-1: scratch objects for BytesRef evaluated parameters
            // 2+N+2+scratchCount: builtVector

            int vectorsStartSlot = 2;
            int resultSlot = vectorsStartSlot + evaluatedParamCount;
            int pSlot = resultSlot + 1;

            // Count scratch objects needed for BytesRef evaluated parameters
            int scratchCount = 0;
            int[] scratchSlots = new int[evaluatedParamCount];
            int evalIdx = 0;
            for (int i = 0; i < paramCount; i++) {
                if (spec.isFixed(i) == false) {
                    if (requiresScratch(spec.parameterType(i))) {
                        scratchSlots[evalIdx] = pSlot + 1 + scratchCount;
                        scratchCount++;
                    } else {
                        scratchSlots[evalIdx] = -1;
                    }
                    evalIdx++;
                }
            }
            int builtVectorSlot = pSlot + 1 + scratchCount;
            int exceptionSlot = builtVectorSlot + 1;
            int maxLocals = exceptionSlot + 1;

            String builderType = "org/elasticsearch/compute/data/BytesRefVector$Builder";

            // Create builder: result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
            methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "driverContext", Type.getDescriptor(DriverContext.class));
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(DriverContext.class),
                "blockFactory",
                "()Lorg/elasticsearch/compute/data/BlockFactory;",
                false
            );
            methodVisitor.visitVarInsn(Opcodes.ILOAD, 1);
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                "org/elasticsearch/compute/data/BlockFactory",
                "newBytesRefVectorBuilder",
                "(I)Lorg/elasticsearch/compute/data/BytesRefVector$Builder;",
                false
            );
            methodVisitor.visitVarInsn(Opcodes.ASTORE, resultSlot);

            // p = 0
            methodVisitor.visitInsn(Opcodes.ICONST_0);
            methodVisitor.visitVarInsn(Opcodes.ISTORE, pSlot);

            // Initialize scratch objects for BytesRef evaluated parameters
            for (int i = 0; i < evaluatedParamCount; i++) {
                if (scratchSlots[i] >= 0) {
                    methodVisitor.visitTypeInsn(Opcodes.NEW, "org/apache/lucene/util/BytesRef");
                    methodVisitor.visitInsn(Opcodes.DUP);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/lucene/util/BytesRef", "<init>", "()V", false);
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, scratchSlots[i]);
                }
            }

            // Set up try-finally for proper resource cleanup (after all locals are initialized)
            Label tryStart = new Label();
            Label tryEnd = new Label();
            Label finallyHandler = new Label();

            methodVisitor.visitLabel(tryStart);

            Label loopStart = new Label();
            Label loopEnd = new Label();

            // Build full frame locals array for F_FULL
            // Full frame includes all locals: this, positionCount, vectors[0..N-1], result, p, scratches...
            // Only evaluated (non-fixed) parameters are method parameters
            int fullLocalsCount = 2 + evaluatedParamCount + 2 + scratchCount;
            Object[] fullFrameLocals = new Object[fullLocalsCount];
            fullFrameLocals[0] = className;
            fullFrameLocals[1] = Opcodes.INTEGER; // positionCount
            evalIdx = 0;
            for (int i = 0; i < paramCount; i++) {
                if (spec.isFixed(i) == false) {
                    fullFrameLocals[2 + evalIdx] = getVectorTypeInternal(spec.parameterType(i));
                    evalIdx++;
                }
            }
            fullFrameLocals[2 + evaluatedParamCount] = builderType;
            fullFrameLocals[2 + evaluatedParamCount + 1] = Opcodes.INTEGER; // p
            for (int i = 0; i < scratchCount; i++) {
                fullFrameLocals[2 + evaluatedParamCount + 2 + i] = "org/apache/lucene/util/BytesRef";
            }

            methodVisitor.visitLabel(loopStart);
            methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 0, new Object[] {});

            // if (p >= positionCount) goto loopEnd
            methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, 1);
            methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, loopEnd);

            // result.appendBytesRef(process(vector.getBytesRef(p, scratch)))
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);

            // Load values for process method in the correct order
            // For evaluated params: get from vector
            // For fixed params: load from this.fieldN
            evalIdx = 0;
            for (int j = 0; j < paramCount; j++) {
                Class<?> paramType = spec.parameterType(j);

                if (spec.isFixed(j)) {
                    // Fixed parameter: load from this.fieldN
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "field" + j, Type.getDescriptor(paramType));
                } else {
                    // Evaluated parameter: get from vector
                    String vectorType = getVectorTypeInternal(paramType);

                    methodVisitor.visitVarInsn(Opcodes.ALOAD, vectorsStartSlot + evalIdx);
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);

                    // For BytesRef parameters, pass the scratch object
                    if (requiresScratch(paramType)) {
                        methodVisitor.visitVarInsn(Opcodes.ALOAD, scratchSlots[evalIdx]);
                    }

                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEINTERFACE,
                        vectorType,
                        getVectorGetterName(paramType),
                        getVectorGetterDescriptor(paramType),
                        true
                    );
                    evalIdx++;
                }
            }

            // Call static process method
            Method processMethod;
            try {
                processMethod = spec.processMethod();
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Failed to find process method", e);
            }
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKESTATIC,
                Type.getInternalName(processMethod.getDeclaringClass()),
                processMethod.getName(),
                Type.getMethodDescriptor(processMethod),
                false
            );

            // appendBytesRef(result)
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEINTERFACE,
                builderType,
                "appendBytesRef",
                "(Lorg/apache/lucene/util/BytesRef;)L" + builderType + ";",
                true
            );
            methodVisitor.visitInsn(Opcodes.POP);

            // p++
            methodVisitor.visitIincInsn(pSlot, 1);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, loopStart);

            methodVisitor.visitLabel(loopEnd);
            methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 0, new Object[] {});

            // Build result: result.build()
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEINTERFACE,
                builderType,
                "build",
                "()Lorg/elasticsearch/compute/data/BytesRefVector;",
                true
            );
            methodVisitor.visitVarInsn(Opcodes.ASTORE, builtVectorSlot);

            methodVisitor.visitLabel(tryEnd);

            // Close builder (no-op after build, but required for try-finally pattern)
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Releasable.class), "close", "()V", true);

            // Return builtVector.asBlock()
            methodVisitor.visitVarInsn(Opcodes.ALOAD, builtVectorSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, resultVectorType, "asBlock", "()L" + resultBlockType + ";", true);

            methodVisitor.visitInsn(Opcodes.ARETURN);

            // Finally handler - catches any throwable, closes builder, re-throws
            methodVisitor.visitLabel(finallyHandler);
            methodVisitor.visitFrame(Opcodes.F_FULL, fullLocalsCount, fullFrameLocals, 1, new Object[] { "java/lang/Throwable" });
            methodVisitor.visitVarInsn(Opcodes.ASTORE, exceptionSlot);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Releasable.class), "close", "()V", true);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, exceptionSlot);
            methodVisitor.visitInsn(Opcodes.ATHROW);

            // Register the try-finally handler (null type catches all throwables)
            methodVisitor.visitTryCatchBlock(tryStart, tryEnd, finallyHandler, null);

            return new ByteCodeAppender.Size(paramCount + 6, maxLocals);
        }

        private String getVectorTypeInternal(Class<?> fieldType) {
            if (fieldType == double.class) return "org/elasticsearch/compute/data/DoubleVector";
            if (fieldType == long.class) return "org/elasticsearch/compute/data/LongVector";
            if (fieldType == int.class) return "org/elasticsearch/compute/data/IntVector";
            if (fieldType == boolean.class) return "org/elasticsearch/compute/data/BooleanVector";
            if (fieldType == BytesRef.class) return "org/elasticsearch/compute/data/BytesRefVector";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getBlockTypeInternal(Class<?> fieldType) {
            if (fieldType == double.class) return "org/elasticsearch/compute/data/DoubleBlock";
            if (fieldType == long.class) return "org/elasticsearch/compute/data/LongBlock";
            if (fieldType == int.class) return "org/elasticsearch/compute/data/IntBlock";
            if (fieldType == boolean.class) return "org/elasticsearch/compute/data/BooleanBlock";
            if (fieldType == BytesRef.class) return "org/elasticsearch/compute/data/BytesRefBlock";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getArrayTypeDescriptor(Class<?> fieldType) {
            if (fieldType == double.class) return "[D";
            if (fieldType == long.class) return "[J";
            if (fieldType == int.class) return "[I";
            if (fieldType == boolean.class) return "[Z";
            if (fieldType == BytesRef.class) return "[Lorg/apache/lucene/util/BytesRef;";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        /**
         * Returns the NEWARRAY type code for primitive arrays, or -1 for reference types
         */
        private int getArrayElementType(Class<?> fieldType) {
            if (fieldType == double.class) return Opcodes.T_DOUBLE;
            if (fieldType == long.class) return Opcodes.T_LONG;
            if (fieldType == int.class) return Opcodes.T_INT;
            if (fieldType == boolean.class) return Opcodes.T_BOOLEAN;
            if (fieldType == BytesRef.class) return -1; // Reference type, use ANEWARRAY
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private int getArrayStoreOpcode(Class<?> fieldType) {
            if (fieldType == double.class) return Opcodes.DASTORE;
            if (fieldType == long.class) return Opcodes.LASTORE;
            if (fieldType == int.class) return Opcodes.IASTORE;
            if (fieldType == boolean.class) return Opcodes.BASTORE;
            if (fieldType == BytesRef.class) return Opcodes.AASTORE;
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getVectorGetterName(Class<?> fieldType) {
            if (fieldType == double.class) return "getDouble";
            if (fieldType == long.class) return "getLong";
            if (fieldType == int.class) return "getInt";
            if (fieldType == boolean.class) return "getBoolean";
            if (fieldType == BytesRef.class) return "getBytesRef";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getVectorGetterDescriptor(Class<?> fieldType) {
            if (fieldType == double.class) return "(I)D";
            if (fieldType == long.class) return "(I)J";
            if (fieldType == int.class) return "(I)I";
            if (fieldType == boolean.class) return "(I)Z";
            if (fieldType == BytesRef.class) return "(ILorg/apache/lucene/util/BytesRef;)Lorg/apache/lucene/util/BytesRef;";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getBlockFactoryMethod(Class<?> fieldType) {
            if (fieldType == double.class) return "newDoubleArrayVector";
            if (fieldType == long.class) return "newLongArrayVector";
            if (fieldType == int.class) return "newIntArrayVector";
            if (fieldType == boolean.class) return "newBooleanArrayVector";
            // BytesRef doesn't use array-based factory - handled separately
            throw new IllegalArgumentException("Unsupported field type for array vector: " + fieldType);
        }

        private String getBlockFactoryDescriptor(Class<?> fieldType) {
            if (fieldType == double.class) return "([DI)Lorg/elasticsearch/compute/data/DoubleVector;";
            if (fieldType == long.class) return "([JI)Lorg/elasticsearch/compute/data/LongVector;";
            if (fieldType == int.class) return "([II)Lorg/elasticsearch/compute/data/IntVector;";
            if (fieldType == boolean.class) return "([ZI)Lorg/elasticsearch/compute/data/BooleanVector;";
            // BytesRef doesn't use array-based factory - handled separately
            throw new IllegalArgumentException("Unsupported field type for array vector: " + fieldType);
        }

        /**
         * Returns true if the field type requires a scratch object for reading (e.g., BytesRef)
         */
        private boolean requiresScratch(Class<?> fieldType) {
            return fieldType == BytesRef.class;
        }
    }

    // ============================================================================================
    // VARIADIC EVALUATOR IMPLEMENTATIONS
    // ============================================================================================
    // These implementations handle functions that take an array parameter (e.g., GREATEST, LEAST, COALESCE).
    // Variadic evaluators have a different structure:
    // - Field: ExpressionEvaluator[] values
    // - Constructor: (Source, ExpressionEvaluator[], DriverContext)
    // - Eval methods work with arrays of blocks/vectors
    // Reference: See compile-time generated GreatestIntEvaluator.java
    // ============================================================================================

    /**
     * Implements constructor for variadic evaluators.
     * <p>
     * Generates equivalent bytecode to:
     * <pre>{@code
     * public Evaluator(Source source, ExpressionEvaluator[] values, DriverContext driverContext) {
     *     this.source = source;
     *     this.values = values;
     *     this.driverContext = driverContext;
     * }
     * }</pre>
     */
    private class VariadicConstructorImplementation implements Implementation {
        private final EvaluatorSpec spec;

        VariadicConstructorImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = spec.evaluatorInternalName();

                // Call super()
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);

                // this.source = source
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1);
                methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "source", "L" + SOURCE_INTERNAL + ";");

                // this.values = values
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2);
                methodVisitor.visitFieldInsn(
                    Opcodes.PUTFIELD,
                    className,
                    "values",
                    "[Lorg/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator;"
                );

                // this.driverContext = driverContext
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3);
                methodVisitor.visitFieldInsn(
                    Opcodes.PUTFIELD,
                    className,
                    "driverContext",
                    "Lorg/elasticsearch/compute/operator/DriverContext;"
                );

                methodVisitor.visitInsn(Opcodes.RETURN);

                return new ByteCodeAppender.Size(2, 4);
            };
        }
    }

    /**
     * Implements eval(Page) method for variadic evaluators.
     * <p>
     * Reference: GreatestIntEvaluator.eval(Page)
     */
    private class VariadicEvalMethodImplementation implements Implementation {
        private final EvaluatorSpec spec;

        VariadicEvalMethodImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = spec.evaluatorInternalName();
                Class<?> componentType = spec.variadicComponentType();
                String blockType = getBlockTypeInternal(componentType);
                String vectorType = getVectorTypeInternal(componentType);
                String blockArrayType = "[L" + blockType + ";";
                String vectorArrayType = "[L" + vectorType + ";";
                String pageType = "org/elasticsearch/compute/data/Page";
                String releasableType = "org/elasticsearch/core/Releasable";

                // Local variables:
                // 0: this
                // 1: page
                // 2: valuesBlocks (Block[])
                // 3: valuesRelease (Releasable)
                // 4: i (loop counter)
                // 5: valuesVectors (Vector[])
                // 6: i (second loop counter)

                int valuesBlocksSlot = 2;
                int valuesReleaseSlot = 3;
                int iSlot = 4;
                int valuesVectorsSlot = 5;
                int i2Slot = 6;
                int exceptionSlot = 7;

                // Block[] valuesBlocks = new Block[values.length];
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    className,
                    "values",
                    "[Lorg/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator;"
                );
                methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
                methodVisitor.visitTypeInsn(Opcodes.ANEWARRAY, blockType);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, valuesBlocksSlot);

                // Releasable valuesRelease = Releasables.wrap(valuesBlocks);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesBlocksSlot);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    "org/elasticsearch/core/Releasables",
                    "wrap",
                    "([Lorg/elasticsearch/core/Releasable;)Lorg/elasticsearch/core/Releasable;",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, valuesReleaseSlot);

                Label tryStart = new Label();
                Label tryEnd = new Label();
                Label finallyHandler = new Label();

                methodVisitor.visitTryCatchBlock(tryStart, tryEnd, finallyHandler, null);

                methodVisitor.visitLabel(tryStart);

                // for (int i = 0; i < valuesBlocks.length; i++)
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, iSlot);

                Label loop1Start = new Label();
                Label loop1End = new Label();

                // Frame at loop1Start: locals = [this, page, valuesBlocks, valuesRelease, i]
                methodVisitor.visitLabel(loop1Start);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    5,
                    new Object[] { className, pageType, blockArrayType, releasableType, Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesBlocksSlot);
                methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, loop1End);

                // valuesBlocks[i] = (Block) values[i].eval(page);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesBlocksSlot);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    className,
                    "values",
                    "[Lorg/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator;"
                );
                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
                methodVisitor.visitInsn(Opcodes.AALOAD);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // page
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    "org/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator",
                    "eval",
                    "(Lorg/elasticsearch/compute/data/Page;)Lorg/elasticsearch/compute/data/Block;",
                    true
                );
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, blockType);
                methodVisitor.visitInsn(Opcodes.AASTORE);

                methodVisitor.visitIincInsn(iSlot, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, loop1Start);

                // Frame at loop1End
                methodVisitor.visitLabel(loop1End);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    5,
                    new Object[] { className, pageType, blockArrayType, releasableType, Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                // Vector[] valuesVectors = new Vector[values.length];
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    className,
                    "values",
                    "[Lorg/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator;"
                );
                methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
                methodVisitor.visitTypeInsn(Opcodes.ANEWARRAY, vectorType);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, valuesVectorsSlot);

                // for (int i = 0; i < valuesBlocks.length; i++)
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, i2Slot);

                Label loop2Start = new Label();
                Label loop2End = new Label();
                Label callBlockEval = new Label();

                // Frame at loop2Start: locals = [this, page, valuesBlocks, valuesRelease, i, valuesVectors, i2]
                methodVisitor.visitLabel(loop2Start);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    7,
                    new Object[] { className, pageType, blockArrayType, releasableType, Opcodes.INTEGER, vectorArrayType, Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                methodVisitor.visitVarInsn(Opcodes.ILOAD, i2Slot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesBlocksSlot);
                methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, loop2End);

                // valuesVectors[i] = valuesBlocks[i].asVector();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesVectorsSlot);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, i2Slot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesBlocksSlot);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, i2Slot);
                methodVisitor.visitInsn(Opcodes.AALOAD);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "asVector", "()L" + vectorType + ";", true);
                methodVisitor.visitInsn(Opcodes.AASTORE);

                // if (valuesVectors[i] == null) return eval(positionCount, valuesBlocks);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesVectorsSlot);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, i2Slot);
                methodVisitor.visitInsn(Opcodes.AALOAD);
                methodVisitor.visitJumpInsn(Opcodes.IFNULL, callBlockEval);

                methodVisitor.visitIincInsn(i2Slot, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, loop2Start);

                // Frame at callBlockEval
                methodVisitor.visitLabel(callBlockEval);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    7,
                    new Object[] { className, pageType, blockArrayType, releasableType, Opcodes.INTEGER, vectorArrayType, Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                // return eval(page.getPositionCount(), valuesBlocks);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // page
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/data/Page",
                    "getPositionCount",
                    "()I",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesBlocksSlot);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    className,
                    "eval",
                    "(I[L" + blockType + ";)L" + blockType + ";",
                    false
                );
                // Close valuesRelease before returning
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesReleaseSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);
                methodVisitor.visitInsn(Opcodes.ARETURN);

                // Frame at loop2End
                methodVisitor.visitLabel(loop2End);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    7,
                    new Object[] { className, pageType, blockArrayType, releasableType, Opcodes.INTEGER, vectorArrayType, Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                // return eval(page.getPositionCount(), valuesVectors).asBlock();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // page
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/data/Page",
                    "getPositionCount",
                    "()I",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesVectorsSlot);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    className,
                    "eval",
                    "(I[L" + vectorType + ";)L" + vectorType + ";",
                    false
                );
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, vectorType, "asBlock", "()L" + blockType + ";", true);

                methodVisitor.visitLabel(tryEnd);
                // Close valuesRelease
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesReleaseSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);
                methodVisitor.visitInsn(Opcodes.ARETURN);

                // Frame at finally handler
                methodVisitor.visitLabel(finallyHandler);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    4,
                    new Object[] { className, pageType, blockArrayType, releasableType },
                    1,
                    new Object[] { "java/lang/Throwable" }
                );

                methodVisitor.visitVarInsn(Opcodes.ASTORE, exceptionSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesReleaseSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, exceptionSlot);
                methodVisitor.visitInsn(Opcodes.ATHROW);

                return new ByteCodeAppender.Size(6, 8);
            };
        }

        private String getBlockTypeInternal(Class<?> fieldType) {
            if (fieldType == double.class) return "org/elasticsearch/compute/data/DoubleBlock";
            if (fieldType == long.class) return "org/elasticsearch/compute/data/LongBlock";
            if (fieldType == int.class) return "org/elasticsearch/compute/data/IntBlock";
            if (fieldType == boolean.class) return "org/elasticsearch/compute/data/BooleanBlock";
            if (fieldType == BytesRef.class) return "org/elasticsearch/compute/data/BytesRefBlock";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getVectorTypeInternal(Class<?> fieldType) {
            if (fieldType == double.class) return "org/elasticsearch/compute/data/DoubleVector";
            if (fieldType == long.class) return "org/elasticsearch/compute/data/LongVector";
            if (fieldType == int.class) return "org/elasticsearch/compute/data/IntVector";
            if (fieldType == boolean.class) return "org/elasticsearch/compute/data/BooleanVector";
            if (fieldType == BytesRef.class) return "org/elasticsearch/compute/data/BytesRefVector";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }
    }

    /**
     * Implements block-style eval for variadic evaluators.
     * <p>
     * Reference: GreatestIntEvaluator.eval(int positionCount, IntBlock[] valuesBlocks)
     */
    private class VariadicBlockEvalImplementation implements Implementation {
        private final EvaluatorSpec spec;

        VariadicBlockEvalImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = spec.evaluatorInternalName();
                Class<?> componentType = spec.variadicComponentType();
                Class<?> returnType = spec.returnType();

                // BytesRef needs special handling because getBytesRef requires a scratch parameter
                if (componentType == BytesRef.class) {
                    return generateBytesRefVariadicBlockEval(methodVisitor, className, componentType, returnType);
                }

                String blockType = getBlockTypeInternal(componentType);
                String blockArrayType = "[L" + blockType + ";";
                String resultBlockType = getBlockTypeInternal(returnType);
                String builderType = resultBlockType + "$Builder";
                String primitiveArrayType = getPrimitiveArrayType(componentType);

                // Local variables:
                // 0: this
                // 1: positionCount
                // 2: valuesBlocks (Block[])
                // 3: result (Builder)
                // 4: valuesValues (primitive array)
                // 5: p (position loop counter)
                // 6: i (inner loop counter)
                // 7: builtBlock (for storing result before close)
                // 8: exception (for finally handler)

                int positionCountSlot = 1;
                int valuesBlocksSlot = 2;
                int resultSlot = 3;
                int valuesValuesSlot = 4;
                int pSlot = 5;
                int iSlot = 6;
                int builtBlockSlot = 7;
                int exceptionSlot = 8;

                // try (Builder result = driverContext.blockFactory().newXxxBlockBuilder(positionCount)) {
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    className,
                    "driverContext",
                    "Lorg/elasticsearch/compute/operator/DriverContext;"
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/operator/DriverContext",
                    "blockFactory",
                    "()Lorg/elasticsearch/compute/data/BlockFactory;",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ILOAD, positionCountSlot);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/data/BlockFactory",
                    getBlockBuilderFactoryMethod(returnType),
                    getBlockBuilderFactoryDescriptor(returnType),
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, resultSlot);

                // primitive[] valuesValues = new primitive[values.length];
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    className,
                    "values",
                    "[Lorg/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator;"
                );
                methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
                int arrayType = getArrayElementType(componentType);
                if (arrayType >= 0) {
                    methodVisitor.visitIntInsn(Opcodes.NEWARRAY, arrayType);
                } else {
                    methodVisitor.visitTypeInsn(Opcodes.ANEWARRAY, "org/apache/lucene/util/BytesRef");
                }
                methodVisitor.visitVarInsn(Opcodes.ASTORE, valuesValuesSlot);

                // position: for (int p = 0; p < positionCount; p++) {
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, pSlot);

                // Set up try-finally for proper resource cleanup (after all locals are initialized)
                Label tryStart = new Label();
                Label tryEnd = new Label();
                Label finallyHandler = new Label();

                methodVisitor.visitLabel(tryStart);

                Label positionLoopStart = new Label();
                Label positionLoopEnd = new Label();
                Label continuePosition = new Label();

                // Frame at positionLoopStart
                methodVisitor.visitLabel(positionLoopStart);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] { className, Opcodes.INTEGER, blockArrayType, builderType, primitiveArrayType, Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, positionCountSlot);
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, positionLoopEnd);

                // for (int i = 0; i < valuesBlocks.length; i++) {
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, iSlot);

                Label checkLoopStart = new Label();
                Label checkLoopEnd = new Label();
                Label appendNullForNull = new Label();
                Label appendNullForMultiValue = new Label();

                // Frame at checkLoopStart
                methodVisitor.visitLabel(checkLoopStart);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    7,
                    new Object[] {
                        className,
                        Opcodes.INTEGER,
                        blockArrayType,
                        builderType,
                        primitiveArrayType,
                        Opcodes.INTEGER,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesBlocksSlot);
                methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, checkLoopEnd);

                // switch (valuesBlocks[i].getValueCount(p)) {
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesBlocksSlot);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
                methodVisitor.visitInsn(Opcodes.AALOAD);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "getValueCount", "(I)I", true);

                // tableswitch for 0, 1, default
                Label case0 = new Label();
                Label case1 = new Label();
                Label caseDefault = new Label();

                methodVisitor.visitTableSwitchInsn(0, 1, caseDefault, case0, case1);

                // case 0: result.appendNull(); continue position;
                methodVisitor.visitLabel(case0);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    7,
                    new Object[] {
                        className,
                        Opcodes.INTEGER,
                        blockArrayType,
                        builderType,
                        primitiveArrayType,
                        Opcodes.INTEGER,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );
                methodVisitor.visitJumpInsn(Opcodes.GOTO, appendNullForNull);

                // case 1: break;
                methodVisitor.visitLabel(case1);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    7,
                    new Object[] {
                        className,
                        Opcodes.INTEGER,
                        blockArrayType,
                        builderType,
                        primitiveArrayType,
                        Opcodes.INTEGER,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );
                methodVisitor.visitIincInsn(iSlot, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, checkLoopStart);

                // default: warn and appendNull
                methodVisitor.visitLabel(caseDefault);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    7,
                    new Object[] {
                        className,
                        Opcodes.INTEGER,
                        blockArrayType,
                        builderType,
                        primitiveArrayType,
                        Opcodes.INTEGER,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );
                methodVisitor.visitJumpInsn(Opcodes.GOTO, appendNullForMultiValue);

                // Frame at checkLoopEnd
                methodVisitor.visitLabel(checkLoopEnd);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    7,
                    new Object[] {
                        className,
                        Opcodes.INTEGER,
                        blockArrayType,
                        builderType,
                        primitiveArrayType,
                        Opcodes.INTEGER,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                // unpack valuesBlocks into valuesValues
                // for (int i = 0; i < valuesBlocks.length; i++) {
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, iSlot);

                Label unpackLoopStart = new Label();
                Label unpackLoopEnd = new Label();

                // Frame at unpackLoopStart
                methodVisitor.visitLabel(unpackLoopStart);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    7,
                    new Object[] {
                        className,
                        Opcodes.INTEGER,
                        blockArrayType,
                        builderType,
                        primitiveArrayType,
                        Opcodes.INTEGER,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesBlocksSlot);
                methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, unpackLoopEnd);

                // int o = valuesBlocks[i].getFirstValueIndex(p);
                // valuesValues[i] = valuesBlocks[i].getXxx(o);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesValuesSlot);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesBlocksSlot);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
                methodVisitor.visitInsn(Opcodes.AALOAD);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesBlocksSlot);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
                methodVisitor.visitInsn(Opcodes.AALOAD);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "getFirstValueIndex", "(I)I", true);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    blockType,
                    getBlockGetterName(componentType),
                    getBlockGetterDescriptor(componentType),
                    true
                );
                methodVisitor.visitInsn(getArrayStoreOpcode(componentType));

                methodVisitor.visitIincInsn(iSlot, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, unpackLoopStart);

                // Frame at unpackLoopEnd
                methodVisitor.visitLabel(unpackLoopEnd);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    7,
                    new Object[] {
                        className,
                        Opcodes.INTEGER,
                        blockArrayType,
                        builderType,
                        primitiveArrayType,
                        Opcodes.INTEGER,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                // result.appendXxx(FunctionClass.process(valuesValues));
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesValuesSlot);

                // Call static process method
                Method processMethod;
                try {
                    processMethod = spec.processMethod();
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException("Failed to find process method", e);
                }
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    Type.getInternalName(processMethod.getDeclaringClass()),
                    processMethod.getName(),
                    Type.getMethodDescriptor(processMethod),
                    false
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    builderType,
                    getAppendMethod(returnType),
                    getAppendDescriptor(returnType),
                    true
                );
                methodVisitor.visitInsn(Opcodes.POP);

                // Frame at continuePosition
                methodVisitor.visitLabel(continuePosition);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] { className, Opcodes.INTEGER, blockArrayType, builderType, primitiveArrayType, Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );
                methodVisitor.visitIincInsn(pSlot, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, positionLoopStart);

                // appendNullForNull: result.appendNull(); continue position;
                methodVisitor.visitLabel(appendNullForNull);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    7,
                    new Object[] {
                        className,
                        Opcodes.INTEGER,
                        blockArrayType,
                        builderType,
                        primitiveArrayType,
                        Opcodes.INTEGER,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderType, "appendNull", "()L" + builderType + ";", true);
                methodVisitor.visitInsn(Opcodes.POP);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, continuePosition);

                // appendNullForMultiValue: warnings().registerException(...); result.appendNull(); continue position;
                methodVisitor.visitLabel(appendNullForMultiValue);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    7,
                    new Object[] {
                        className,
                        Opcodes.INTEGER,
                        blockArrayType,
                        builderType,
                        primitiveArrayType,
                        Opcodes.INTEGER,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, className, "warnings", "()L" + WARNINGS_INTERNAL + ";", false);
                methodVisitor.visitTypeInsn(Opcodes.NEW, "java/lang/IllegalArgumentException");
                methodVisitor.visitInsn(Opcodes.DUP);
                methodVisitor.visitLdcInsn("single-value function encountered multi-value");
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKESPECIAL,
                    "java/lang/IllegalArgumentException",
                    "<init>",
                    "(Ljava/lang/String;)V",
                    false
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    WARNINGS_INTERNAL,
                    "registerException",
                    "(Ljava/lang/Exception;)V",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderType, "appendNull", "()L" + builderType + ";", true);
                methodVisitor.visitInsn(Opcodes.POP);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, continuePosition);

                // Frame at positionLoopEnd
                methodVisitor.visitLabel(positionLoopEnd);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] { className, Opcodes.INTEGER, blockArrayType, builderType, primitiveArrayType, Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                // Build result
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderType, "build", "()L" + resultBlockType + ";", true);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, builtBlockSlot);

                methodVisitor.visitLabel(tryEnd);

                // Close builder (no-op after build, but required for try-finally pattern)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);

                // Return
                methodVisitor.visitVarInsn(Opcodes.ALOAD, builtBlockSlot);
                methodVisitor.visitInsn(Opcodes.ARETURN);

                // Finally handler - catches any throwable, closes builder, re-throws
                Object[] frameLocals6 = new Object[] {
                    className,
                    Opcodes.INTEGER,
                    blockArrayType,
                    builderType,
                    primitiveArrayType,
                    Opcodes.INTEGER };
                methodVisitor.visitLabel(finallyHandler);
                methodVisitor.visitFrame(Opcodes.F_FULL, 6, frameLocals6, 1, new Object[] { "java/lang/Throwable" });
                methodVisitor.visitVarInsn(Opcodes.ASTORE, exceptionSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, exceptionSlot);
                methodVisitor.visitInsn(Opcodes.ATHROW);

                // Register the try-finally handler (null type catches all throwables)
                methodVisitor.visitTryCatchBlock(tryStart, tryEnd, finallyHandler, null);

                return new ByteCodeAppender.Size(8, exceptionSlot + 1);
            };
        }

        private String getPrimitiveArrayType(Class<?> fieldType) {
            if (fieldType == double.class) return "[D";
            if (fieldType == long.class) return "[J";
            if (fieldType == int.class) return "[I";
            if (fieldType == boolean.class) return "[Z";
            if (fieldType == BytesRef.class) return "[Lorg/apache/lucene/util/BytesRef;";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getBlockTypeInternal(Class<?> fieldType) {
            if (fieldType == double.class) return "org/elasticsearch/compute/data/DoubleBlock";
            if (fieldType == long.class) return "org/elasticsearch/compute/data/LongBlock";
            if (fieldType == int.class) return "org/elasticsearch/compute/data/IntBlock";
            if (fieldType == boolean.class) return "org/elasticsearch/compute/data/BooleanBlock";
            if (fieldType == BytesRef.class) return "org/elasticsearch/compute/data/BytesRefBlock";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getBlockBuilderFactoryMethod(Class<?> fieldType) {
            if (fieldType == double.class) return "newDoubleBlockBuilder";
            if (fieldType == long.class) return "newLongBlockBuilder";
            if (fieldType == int.class) return "newIntBlockBuilder";
            if (fieldType == boolean.class) return "newBooleanBlockBuilder";
            if (fieldType == BytesRef.class) return "newBytesRefBlockBuilder";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getBlockBuilderFactoryDescriptor(Class<?> fieldType) {
            if (fieldType == double.class) return "(I)Lorg/elasticsearch/compute/data/DoubleBlock$Builder;";
            if (fieldType == long.class) return "(I)Lorg/elasticsearch/compute/data/LongBlock$Builder;";
            if (fieldType == int.class) return "(I)Lorg/elasticsearch/compute/data/IntBlock$Builder;";
            if (fieldType == boolean.class) return "(I)Lorg/elasticsearch/compute/data/BooleanBlock$Builder;";
            if (fieldType == BytesRef.class) return "(I)Lorg/elasticsearch/compute/data/BytesRefBlock$Builder;";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private int getArrayElementType(Class<?> fieldType) {
            if (fieldType == double.class) return Opcodes.T_DOUBLE;
            if (fieldType == long.class) return Opcodes.T_LONG;
            if (fieldType == int.class) return Opcodes.T_INT;
            if (fieldType == boolean.class) return Opcodes.T_BOOLEAN;
            if (fieldType == BytesRef.class) return -1;
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private int getArrayStoreOpcode(Class<?> fieldType) {
            if (fieldType == double.class) return Opcodes.DASTORE;
            if (fieldType == long.class) return Opcodes.LASTORE;
            if (fieldType == int.class) return Opcodes.IASTORE;
            if (fieldType == boolean.class) return Opcodes.BASTORE;
            if (fieldType == BytesRef.class) return Opcodes.AASTORE;
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getBlockGetterName(Class<?> fieldType) {
            if (fieldType == double.class) return "getDouble";
            if (fieldType == long.class) return "getLong";
            if (fieldType == int.class) return "getInt";
            if (fieldType == boolean.class) return "getBoolean";
            if (fieldType == BytesRef.class) return "getBytesRef";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getBlockGetterDescriptor(Class<?> fieldType) {
            if (fieldType == double.class) return "(I)D";
            if (fieldType == long.class) return "(I)J";
            if (fieldType == int.class) return "(I)I";
            if (fieldType == boolean.class) return "(I)Z";
            if (fieldType == BytesRef.class) return "(ILorg/apache/lucene/util/BytesRef;)Lorg/apache/lucene/util/BytesRef;";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getAppendMethod(Class<?> fieldType) {
            if (fieldType == double.class) return "appendDouble";
            if (fieldType == long.class) return "appendLong";
            if (fieldType == int.class) return "appendInt";
            if (fieldType == boolean.class) return "appendBoolean";
            if (fieldType == BytesRef.class) return "appendBytesRef";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getAppendDescriptor(Class<?> fieldType) {
            String blockType = getBlockTypeInternal(fieldType);
            String builderType = blockType + "$Builder";
            if (fieldType == double.class) return "(D)L" + builderType + ";";
            if (fieldType == long.class) return "(J)L" + builderType + ";";
            if (fieldType == int.class) return "(I)L" + builderType + ";";
            if (fieldType == boolean.class) return "(Z)L" + builderType + ";";
            if (fieldType == BytesRef.class) return "(Lorg/apache/lucene/util/BytesRef;)L" + builderType + ";";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        /**
         * Generates specialized bytecode for variadic BytesRef block evaluation.
         * BytesRef requires special handling because getBytesRef(int, BytesRef) needs a scratch parameter.
         */
        private ByteCodeAppender.Size generateBytesRefVariadicBlockEval(
            MethodVisitor methodVisitor,
            String className,
            Class<?> componentType,
            Class<?> returnType
        ) {
            String blockType = getBlockTypeInternal(componentType);
            String blockArrayType = "[L" + blockType + ";";
            String resultBlockType = getBlockTypeInternal(returnType);
            String builderType = resultBlockType + "$Builder";
            String primitiveArrayType = "[Lorg/apache/lucene/util/BytesRef;";

            // Local variables:
            // 0: this
            // 1: positionCount
            // 2: valuesBlocks (Block[])
            // 3: result (Builder)
            // 4: valuesValues (BytesRef[])
            // 5: valuesScratch (BytesRef[] - one scratch per parameter, like compile-time generator)
            // 6: p (position loop counter)
            // 7: i (inner loop counter)
            // 8: builtBlock (for storing result before close)
            // 9: exception (for finally handler)

            int positionCountSlot = 1;
            int valuesBlocksSlot = 2;
            int resultSlot = 3;
            int valuesValuesSlot = 4;
            int valuesScratchSlot = 5;
            int pSlot = 6;
            int iSlot = 7;
            int builtBlockSlot = 8;
            int exceptionSlot = 9;

            // try (Builder result = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
            methodVisitor.visitFieldInsn(
                Opcodes.GETFIELD,
                className,
                "driverContext",
                "Lorg/elasticsearch/compute/operator/DriverContext;"
            );
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                "org/elasticsearch/compute/operator/DriverContext",
                "blockFactory",
                "()Lorg/elasticsearch/compute/data/BlockFactory;",
                false
            );
            methodVisitor.visitVarInsn(Opcodes.ILOAD, positionCountSlot);
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                "org/elasticsearch/compute/data/BlockFactory",
                "newBytesRefBlockBuilder",
                "(I)L" + builderType + ";",
                false
            );
            methodVisitor.visitVarInsn(Opcodes.ASTORE, resultSlot);

            // BytesRef[] valuesValues = new BytesRef[values.length];
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
            methodVisitor.visitFieldInsn(
                Opcodes.GETFIELD,
                className,
                "values",
                "[Lorg/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator;"
            );
            methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
            methodVisitor.visitTypeInsn(Opcodes.ANEWARRAY, "org/apache/lucene/util/BytesRef");
            methodVisitor.visitVarInsn(Opcodes.ASTORE, valuesValuesSlot);

            // BytesRef[] valuesScratch = new BytesRef[values.length];
            // (Like compile-time generator - one scratch per parameter)
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
            methodVisitor.visitFieldInsn(
                Opcodes.GETFIELD,
                className,
                "values",
                "[Lorg/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator;"
            );
            methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
            methodVisitor.visitTypeInsn(Opcodes.ANEWARRAY, "org/apache/lucene/util/BytesRef");
            methodVisitor.visitVarInsn(Opcodes.ASTORE, valuesScratchSlot);

            // for (int i = 0; i < values.length; i++) { valuesScratch[i] = new BytesRef(); }
            Label scratchInitLoopStart = new Label();
            Label scratchInitLoopEnd = new Label();

            methodVisitor.visitInsn(Opcodes.ICONST_0);
            methodVisitor.visitVarInsn(Opcodes.ISTORE, pSlot); // reuse pSlot temporarily as loop counter

            methodVisitor.visitLabel(scratchInitLoopStart);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                7,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    blockArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );

            methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesScratchSlot);
            methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
            methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, scratchInitLoopEnd);

            // valuesScratch[i] = new BytesRef();
            methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesScratchSlot);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
            methodVisitor.visitTypeInsn(Opcodes.NEW, "org/apache/lucene/util/BytesRef");
            methodVisitor.visitInsn(Opcodes.DUP);
            methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/lucene/util/BytesRef", "<init>", "()V", false);
            methodVisitor.visitInsn(Opcodes.AASTORE);

            methodVisitor.visitIincInsn(pSlot, 1);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, scratchInitLoopStart);

            methodVisitor.visitLabel(scratchInitLoopEnd);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                7,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    blockArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );

            // p = 0 (now for the actual position loop)
            methodVisitor.visitInsn(Opcodes.ICONST_0);
            methodVisitor.visitVarInsn(Opcodes.ISTORE, pSlot);

            // Set up try-finally for proper resource cleanup
            Label tryStart = new Label();
            Label tryEnd = new Label();
            Label finallyHandler = new Label();

            methodVisitor.visitLabel(tryStart);

            Label positionLoopStart = new Label();
            Label positionLoopEnd = new Label();
            Label continuePosition = new Label();
            Label appendNullForNull = new Label();
            Label appendNullForMultiValue = new Label();

            // Frame at positionLoopStart - 7 locals (with valuesScratch)
            methodVisitor.visitLabel(positionLoopStart);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                7,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    blockArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );

            methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, positionCountSlot);
            methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, positionLoopEnd);

            // for (int i = 0; i < valuesBlocks.length; i++) { check for null/multivalue }
            methodVisitor.visitInsn(Opcodes.ICONST_0);
            methodVisitor.visitVarInsn(Opcodes.ISTORE, iSlot);

            Label checkLoopStart = new Label();
            Label checkLoopEnd = new Label();

            // Frame at checkLoopStart - 8 locals (with valuesScratch)
            methodVisitor.visitLabel(checkLoopStart);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                8,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    blockArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );

            methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesBlocksSlot);
            methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
            methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, checkLoopEnd);

            // switch (valuesBlocks[i].getValueCount(p)) {
            methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesBlocksSlot);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
            methodVisitor.visitInsn(Opcodes.AALOAD);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "getValueCount", "(I)I", true);

            Label case0 = new Label();
            Label case1 = new Label();
            Label caseDefault = new Label();

            methodVisitor.visitTableSwitchInsn(0, 1, caseDefault, case0, case1);

            // case 0: goto appendNullForNull
            methodVisitor.visitLabel(case0);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                8,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    blockArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );
            methodVisitor.visitJumpInsn(Opcodes.GOTO, appendNullForNull);

            // case 1: i++; continue
            methodVisitor.visitLabel(case1);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                8,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    blockArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );
            methodVisitor.visitIincInsn(iSlot, 1);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, checkLoopStart);

            // default: goto appendNullForMultiValue
            methodVisitor.visitLabel(caseDefault);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                8,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    blockArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );
            methodVisitor.visitJumpInsn(Opcodes.GOTO, appendNullForMultiValue);

            // Frame at checkLoopEnd - 8 locals (with valuesScratch)
            methodVisitor.visitLabel(checkLoopEnd);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                8,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    blockArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );

            // unpack valuesBlocks into valuesValues
            // for (int i = 0; i < valuesBlocks.length; i++) {
            methodVisitor.visitInsn(Opcodes.ICONST_0);
            methodVisitor.visitVarInsn(Opcodes.ISTORE, iSlot);

            Label unpackLoopStart = new Label();
            Label unpackLoopEnd = new Label();

            // Frame at unpackLoopStart - 8 locals (with valuesScratch array, not single scratch)
            methodVisitor.visitLabel(unpackLoopStart);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                8,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    blockArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );

            methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesBlocksSlot);
            methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
            methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, unpackLoopEnd);

            // int o = valuesBlocks[i].getFirstValueIndex(p);
            // valuesValues[i] = valuesBlocks[i].getBytesRef(o, valuesScratch[i]);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesValuesSlot);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesBlocksSlot);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
            methodVisitor.visitInsn(Opcodes.AALOAD);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesBlocksSlot);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
            methodVisitor.visitInsn(Opcodes.AALOAD);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "getFirstValueIndex", "(I)I", true);
            // Use valuesScratch[i] instead of single scratch
            methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesScratchSlot);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
            methodVisitor.visitInsn(Opcodes.AALOAD);
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEINTERFACE,
                blockType,
                "getBytesRef",
                "(ILorg/apache/lucene/util/BytesRef;)Lorg/apache/lucene/util/BytesRef;",
                true
            );
            methodVisitor.visitInsn(Opcodes.AASTORE);

            methodVisitor.visitIincInsn(iSlot, 1);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, unpackLoopStart);

            // Frame at unpackLoopEnd - 8 locals
            methodVisitor.visitLabel(unpackLoopEnd);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                8,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    blockArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );

            // result.appendBytesRef(FunctionClass.process(valuesValues));
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesValuesSlot);

            // Call static process method
            Method processMethod;
            try {
                processMethod = spec.processMethod();
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Failed to find process method", e);
            }
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKESTATIC,
                Type.getInternalName(processMethod.getDeclaringClass()),
                processMethod.getName(),
                Type.getMethodDescriptor(processMethod),
                false
            );
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEINTERFACE,
                builderType,
                "appendBytesRef",
                "(Lorg/apache/lucene/util/BytesRef;)L" + builderType + ";",
                true
            );
            methodVisitor.visitInsn(Opcodes.POP);

            // Frame at continuePosition - 7 locals (with valuesScratch)
            methodVisitor.visitLabel(continuePosition);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                7,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    blockArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );
            methodVisitor.visitIincInsn(pSlot, 1);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, positionLoopStart);

            // appendNullForNull: result.appendNull(); continue position;
            methodVisitor.visitLabel(appendNullForNull);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                8,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    blockArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderType, "appendNull", "()L" + builderType + ";", true);
            methodVisitor.visitInsn(Opcodes.POP);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, continuePosition);

            // appendNullForMultiValue: warnings.registerException(...); result.appendNull(); continue position;
            methodVisitor.visitLabel(appendNullForMultiValue);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                8,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    blockArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );
            // warnings.registerException(new IllegalArgumentException("single-value function encountered multi-value"));
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
            methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "warnings", "Lorg/elasticsearch/compute/operator/Warnings;");
            methodVisitor.visitTypeInsn(Opcodes.NEW, "java/lang/IllegalArgumentException");
            methodVisitor.visitInsn(Opcodes.DUP);
            methodVisitor.visitLdcInsn("single-value function encountered multi-value");
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKESPECIAL,
                "java/lang/IllegalArgumentException",
                "<init>",
                "(Ljava/lang/String;)V",
                false
            );
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                "org/elasticsearch/compute/operator/Warnings",
                "registerException",
                "(Ljava/lang/Exception;)V",
                false
            );
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderType, "appendNull", "()L" + builderType + ";", true);
            methodVisitor.visitInsn(Opcodes.POP);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, continuePosition);

            // Frame at positionLoopEnd - 7 locals (with valuesScratch)
            methodVisitor.visitLabel(positionLoopEnd);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                7,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    blockArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );

            // Build result
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderType, "build", "()L" + resultBlockType + ";", true);
            methodVisitor.visitVarInsn(Opcodes.ASTORE, builtBlockSlot);

            methodVisitor.visitLabel(tryEnd);

            // Close builder
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);

            // Return
            methodVisitor.visitVarInsn(Opcodes.ALOAD, builtBlockSlot);
            methodVisitor.visitInsn(Opcodes.ARETURN);

            // Finally handler - 7 locals (with valuesScratch)
            methodVisitor.visitLabel(finallyHandler);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                7,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    blockArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER },
                1,
                new Object[] { "java/lang/Throwable" }
            );
            methodVisitor.visitVarInsn(Opcodes.ASTORE, exceptionSlot);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, exceptionSlot);
            methodVisitor.visitInsn(Opcodes.ATHROW);

            // Register the try-finally handler
            methodVisitor.visitTryCatchBlock(tryStart, tryEnd, finallyHandler, null);

            return new ByteCodeAppender.Size(7, exceptionSlot + 1);
        }
    }

    /**
     * Implements vector-style eval for variadic evaluators.
     * <p>
     * Reference: GreatestIntEvaluator.eval(int positionCount, IntVector[] valuesVectors)
     */
    private class VariadicVectorEvalImplementation implements Implementation {
        private final EvaluatorSpec spec;

        VariadicVectorEvalImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = spec.evaluatorInternalName();
                Class<?> componentType = spec.variadicComponentType();
                Class<?> returnType = spec.returnType();

                // BytesRef needs special handling - use Builder instead of FixedBuilder
                if (returnType == BytesRef.class) {
                    return generateBytesRefVariadicVectorEval(methodVisitor, className, componentType, returnType);
                }

                String vectorType = getVectorTypeInternal(componentType);
                String vectorArrayType = "[L" + vectorType + ";";
                String resultVectorType = getVectorTypeInternal(returnType);
                String fixedBuilderType = resultVectorType + "$FixedBuilder";
                String primitiveArrayType = getPrimitiveArrayType(componentType);

                // Local variables:
                // 0: this
                // 1: positionCount
                // 2: valuesVectors (Vector[])
                // 3: result (FixedBuilder)
                // 4: valuesValues (primitive array)
                // 5: p (position loop counter)
                // 6: i (inner loop counter)
                // 7: builtVector (for storing result before close)
                // 8: exception (for finally handler)

                int positionCountSlot = 1;
                int valuesVectorsSlot = 2;
                int resultSlot = 3;
                int valuesValuesSlot = 4;
                int pSlot = 5;
                int iSlot = 6;
                int builtVectorSlot = 7;
                int exceptionSlot = 8;

                // try (FixedBuilder result = driverContext.blockFactory().newXxxVectorFixedBuilder(positionCount)) {
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    className,
                    "driverContext",
                    "Lorg/elasticsearch/compute/operator/DriverContext;"
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/operator/DriverContext",
                    "blockFactory",
                    "()Lorg/elasticsearch/compute/data/BlockFactory;",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ILOAD, positionCountSlot);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/data/BlockFactory",
                    getFixedBuilderFactoryMethod(returnType),
                    getFixedBuilderFactoryDescriptor(returnType),
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, resultSlot);

                // primitive[] valuesValues = new primitive[values.length];
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    className,
                    "values",
                    "[Lorg/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator;"
                );
                methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
                int arrayType = getArrayElementType(componentType);
                if (arrayType >= 0) {
                    methodVisitor.visitIntInsn(Opcodes.NEWARRAY, arrayType);
                } else {
                    methodVisitor.visitTypeInsn(Opcodes.ANEWARRAY, "org/apache/lucene/util/BytesRef");
                }
                methodVisitor.visitVarInsn(Opcodes.ASTORE, valuesValuesSlot);

                // position: for (int p = 0; p < positionCount; p++) {
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, pSlot);

                // Set up try-finally for proper resource cleanup (after all locals are initialized)
                Label tryStart = new Label();
                Label tryEnd = new Label();
                Label finallyHandler = new Label();

                methodVisitor.visitLabel(tryStart);

                Label positionLoopStart = new Label();
                Label positionLoopEnd = new Label();

                // Frame at positionLoopStart
                methodVisitor.visitLabel(positionLoopStart);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] { className, Opcodes.INTEGER, vectorArrayType, fixedBuilderType, primitiveArrayType, Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, positionCountSlot);
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, positionLoopEnd);

                // unpack valuesVectors into valuesValues
                // for (int i = 0; i < valuesVectors.length; i++) {
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, iSlot);

                Label unpackLoopStart = new Label();
                Label unpackLoopEnd = new Label();

                // Frame at unpackLoopStart
                methodVisitor.visitLabel(unpackLoopStart);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    7,
                    new Object[] {
                        className,
                        Opcodes.INTEGER,
                        vectorArrayType,
                        fixedBuilderType,
                        primitiveArrayType,
                        Opcodes.INTEGER,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesVectorsSlot);
                methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, unpackLoopEnd);

                // valuesValues[i] = valuesVectors[i].getXxx(p);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesValuesSlot);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesVectorsSlot);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
                methodVisitor.visitInsn(Opcodes.AALOAD);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    vectorType,
                    getVectorGetterName(componentType),
                    getVectorGetterDescriptor(componentType),
                    true
                );
                methodVisitor.visitInsn(getArrayStoreOpcode(componentType));

                methodVisitor.visitIincInsn(iSlot, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, unpackLoopStart);

                // Frame at unpackLoopEnd
                methodVisitor.visitLabel(unpackLoopEnd);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    7,
                    new Object[] {
                        className,
                        Opcodes.INTEGER,
                        vectorArrayType,
                        fixedBuilderType,
                        primitiveArrayType,
                        Opcodes.INTEGER,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                // result.appendXxx(p, FunctionClass.process(valuesValues));
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesValuesSlot);

                // Call static process method
                Method processMethod;
                try {
                    processMethod = spec.processMethod();
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException("Failed to find process method", e);
                }
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    Type.getInternalName(processMethod.getDeclaringClass()),
                    processMethod.getName(),
                    Type.getMethodDescriptor(processMethod),
                    false
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    fixedBuilderType,
                    getAppendMethod(returnType),
                    getAppendDescriptor(returnType),
                    true
                );
                methodVisitor.visitInsn(Opcodes.POP);

                methodVisitor.visitIincInsn(pSlot, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, positionLoopStart);

                // Frame at positionLoopEnd
                methodVisitor.visitLabel(positionLoopEnd);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] { className, Opcodes.INTEGER, vectorArrayType, fixedBuilderType, primitiveArrayType, Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                // Build result
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, fixedBuilderType, "build", "()L" + resultVectorType + ";", true);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, builtVectorSlot);

                methodVisitor.visitLabel(tryEnd);

                // Close builder (no-op after build, but required for try-finally pattern)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);

                // Return
                methodVisitor.visitVarInsn(Opcodes.ALOAD, builtVectorSlot);
                methodVisitor.visitInsn(Opcodes.ARETURN);

                // Finally handler - catches any throwable, closes builder, re-throws
                Object[] frameLocals6 = new Object[] {
                    className,
                    Opcodes.INTEGER,
                    vectorArrayType,
                    fixedBuilderType,
                    primitiveArrayType,
                    Opcodes.INTEGER };
                methodVisitor.visitLabel(finallyHandler);
                methodVisitor.visitFrame(Opcodes.F_FULL, 6, frameLocals6, 1, new Object[] { "java/lang/Throwable" });
                methodVisitor.visitVarInsn(Opcodes.ASTORE, exceptionSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, exceptionSlot);
                methodVisitor.visitInsn(Opcodes.ATHROW);

                // Register the try-finally handler (null type catches all throwables)
                methodVisitor.visitTryCatchBlock(tryStart, tryEnd, finallyHandler, null);

                return new ByteCodeAppender.Size(6, exceptionSlot + 1);
            };
        }

        private ByteCodeAppender.Size generateBytesRefVariadicVectorEval(
            MethodVisitor methodVisitor,
            String className,
            Class<?> componentType,
            Class<?> returnType
        ) {
            String vectorType = getVectorTypeInternal(componentType);
            String vectorArrayType = "[L" + vectorType + ";";
            String resultVectorType = "org/elasticsearch/compute/data/BytesRefVector";
            String builderType = resultVectorType + "$Builder";
            String primitiveArrayType = "[Lorg/apache/lucene/util/BytesRef;";

            // Local variables (matching the pattern of the non-BytesRef implementation):
            // 0: this
            // 1: positionCount
            // 2: valuesVectors (Vector[])
            // 3: result (Builder)
            // 4: valuesValues (BytesRef[])
            // 5: valuesScratch (BytesRef[] - one scratch per parameter, like compile-time generator)
            // 6: p (position loop counter)
            // 7: i (inner loop counter)
            // 8: builtVector (for storing result before close)
            // 9: exception (for finally handler)

            int positionCountSlot = 1;
            int valuesVectorsSlot = 2;
            int resultSlot = 3;
            int valuesValuesSlot = 4;
            int valuesScratchSlot = 5;
            int pSlot = 6;
            int iSlot = 7;
            int builtVectorSlot = 8;
            int exceptionSlot = 9;

            // try (Builder result = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount)) {
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
            methodVisitor.visitFieldInsn(
                Opcodes.GETFIELD,
                className,
                "driverContext",
                "Lorg/elasticsearch/compute/operator/DriverContext;"
            );
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                "org/elasticsearch/compute/operator/DriverContext",
                "blockFactory",
                "()Lorg/elasticsearch/compute/data/BlockFactory;",
                false
            );
            methodVisitor.visitVarInsn(Opcodes.ILOAD, positionCountSlot);
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                "org/elasticsearch/compute/data/BlockFactory",
                "newBytesRefVectorBuilder",
                "(I)L" + builderType + ";",
                false
            );
            methodVisitor.visitVarInsn(Opcodes.ASTORE, resultSlot);

            // BytesRef[] valuesValues = new BytesRef[values.length];
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
            methodVisitor.visitFieldInsn(
                Opcodes.GETFIELD,
                className,
                "values",
                "[Lorg/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator;"
            );
            methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
            methodVisitor.visitTypeInsn(Opcodes.ANEWARRAY, "org/apache/lucene/util/BytesRef");
            methodVisitor.visitVarInsn(Opcodes.ASTORE, valuesValuesSlot);

            // BytesRef[] valuesScratch = new BytesRef[values.length];
            // (Like compile-time generator - one scratch per parameter)
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
            methodVisitor.visitFieldInsn(
                Opcodes.GETFIELD,
                className,
                "values",
                "[Lorg/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator;"
            );
            methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
            methodVisitor.visitTypeInsn(Opcodes.ANEWARRAY, "org/apache/lucene/util/BytesRef");
            methodVisitor.visitVarInsn(Opcodes.ASTORE, valuesScratchSlot);

            // for (int i = 0; i < values.length; i++) { valuesScratch[i] = new BytesRef(); }
            Label scratchInitLoopStart = new Label();
            Label scratchInitLoopEnd = new Label();

            methodVisitor.visitInsn(Opcodes.ICONST_0);
            methodVisitor.visitVarInsn(Opcodes.ISTORE, pSlot); // reuse pSlot temporarily as loop counter

            methodVisitor.visitLabel(scratchInitLoopStart);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                7,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    vectorArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );

            methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesScratchSlot);
            methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
            methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, scratchInitLoopEnd);

            // valuesScratch[i] = new BytesRef();
            methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesScratchSlot);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
            methodVisitor.visitTypeInsn(Opcodes.NEW, "org/apache/lucene/util/BytesRef");
            methodVisitor.visitInsn(Opcodes.DUP);
            methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/lucene/util/BytesRef", "<init>", "()V", false);
            methodVisitor.visitInsn(Opcodes.AASTORE);

            methodVisitor.visitIincInsn(pSlot, 1);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, scratchInitLoopStart);

            methodVisitor.visitLabel(scratchInitLoopEnd);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                7,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    vectorArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );

            // p = 0 (now for the actual position loop)
            methodVisitor.visitInsn(Opcodes.ICONST_0);
            methodVisitor.visitVarInsn(Opcodes.ISTORE, pSlot);

            // Set up try-finally for proper resource cleanup
            Label tryStart = new Label();
            Label tryEnd = new Label();
            Label finallyHandler = new Label();

            methodVisitor.visitLabel(tryStart);

            Label positionLoopStart = new Label();
            Label positionLoopEnd = new Label();

            // Frame at positionLoopStart - 7 locals (with valuesScratch)
            methodVisitor.visitLabel(positionLoopStart);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                7,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    vectorArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );

            methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, positionCountSlot);
            methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, positionLoopEnd);

            // unpack valuesVectors into valuesValues
            // for (int i = 0; i < valuesVectors.length; i++) {
            methodVisitor.visitInsn(Opcodes.ICONST_0);
            methodVisitor.visitVarInsn(Opcodes.ISTORE, iSlot);

            Label unpackLoopStart = new Label();
            Label unpackLoopEnd = new Label();

            // Frame at unpackLoopStart - 8 locals (with valuesScratch array, not single scratch)
            methodVisitor.visitLabel(unpackLoopStart);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                8,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    vectorArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );

            methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesVectorsSlot);
            methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
            methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, unpackLoopEnd);

            // valuesValues[i] = valuesVectors[i].getBytesRef(p, valuesScratch[i]);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesValuesSlot);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesVectorsSlot);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
            methodVisitor.visitInsn(Opcodes.AALOAD);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
            // Use valuesScratch[i] instead of single scratch
            methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesScratchSlot);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
            methodVisitor.visitInsn(Opcodes.AALOAD);
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEINTERFACE,
                vectorType,
                "getBytesRef",
                "(ILorg/apache/lucene/util/BytesRef;)Lorg/apache/lucene/util/BytesRef;",
                true
            );
            methodVisitor.visitInsn(Opcodes.AASTORE);

            methodVisitor.visitIincInsn(iSlot, 1);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, unpackLoopStart);

            // Frame at unpackLoopEnd - 8 locals (with valuesScratch array)
            methodVisitor.visitLabel(unpackLoopEnd);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                8,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    vectorArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );

            // result.appendBytesRef(FunctionClass.process(valuesValues));
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, valuesValuesSlot);

            // Call static process method
            Method processMethod;
            try {
                processMethod = spec.processMethod();
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Failed to find process method", e);
            }
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKESTATIC,
                Type.getInternalName(processMethod.getDeclaringClass()),
                processMethod.getName(),
                Type.getMethodDescriptor(processMethod),
                false
            );
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEINTERFACE,
                builderType,
                "appendBytesRef",
                "(Lorg/apache/lucene/util/BytesRef;)L" + builderType + ";",
                true
            );
            methodVisitor.visitInsn(Opcodes.POP);

            methodVisitor.visitIincInsn(pSlot, 1);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, positionLoopStart);

            // Frame at positionLoopEnd - 7 locals (with valuesScratch)
            methodVisitor.visitLabel(positionLoopEnd);
            methodVisitor.visitFrame(
                Opcodes.F_FULL,
                7,
                new Object[] {
                    className,
                    Opcodes.INTEGER,
                    vectorArrayType,
                    builderType,
                    primitiveArrayType,
                    primitiveArrayType,
                    Opcodes.INTEGER },
                0,
                new Object[] {}
            );

            // Build result
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderType, "build", "()L" + resultVectorType + ";", true);
            methodVisitor.visitVarInsn(Opcodes.ASTORE, builtVectorSlot);

            methodVisitor.visitLabel(tryEnd);

            // Close builder
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);

            // Return
            methodVisitor.visitVarInsn(Opcodes.ALOAD, builtVectorSlot);
            methodVisitor.visitInsn(Opcodes.ARETURN);

            // Finally handler - 7 locals (with valuesScratch)
            Object[] frameLocals7 = new Object[] {
                className,
                Opcodes.INTEGER,
                vectorArrayType,
                builderType,
                primitiveArrayType,
                primitiveArrayType,
                Opcodes.INTEGER };
            methodVisitor.visitLabel(finallyHandler);
            methodVisitor.visitFrame(Opcodes.F_FULL, 7, frameLocals7, 1, new Object[] { "java/lang/Throwable" });
            methodVisitor.visitVarInsn(Opcodes.ASTORE, exceptionSlot);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, resultSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, exceptionSlot);
            methodVisitor.visitInsn(Opcodes.ATHROW);

            // Register the try-finally handler
            methodVisitor.visitTryCatchBlock(tryStart, tryEnd, finallyHandler, null);

            return new ByteCodeAppender.Size(6, exceptionSlot + 1);
        }

        private String getPrimitiveArrayType(Class<?> fieldType) {
            if (fieldType == double.class) return "[D";
            if (fieldType == long.class) return "[J";
            if (fieldType == int.class) return "[I";
            if (fieldType == boolean.class) return "[Z";
            if (fieldType == BytesRef.class) return "[Lorg/apache/lucene/util/BytesRef;";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getVectorTypeInternal(Class<?> fieldType) {
            if (fieldType == double.class) return "org/elasticsearch/compute/data/DoubleVector";
            if (fieldType == long.class) return "org/elasticsearch/compute/data/LongVector";
            if (fieldType == int.class) return "org/elasticsearch/compute/data/IntVector";
            if (fieldType == boolean.class) return "org/elasticsearch/compute/data/BooleanVector";
            if (fieldType == BytesRef.class) return "org/elasticsearch/compute/data/BytesRefVector";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getFixedBuilderFactoryMethod(Class<?> fieldType) {
            if (fieldType == double.class) return "newDoubleVectorFixedBuilder";
            if (fieldType == long.class) return "newLongVectorFixedBuilder";
            if (fieldType == int.class) return "newIntVectorFixedBuilder";
            if (fieldType == boolean.class) return "newBooleanVectorFixedBuilder";
            throw new IllegalArgumentException("Unsupported field type for fixed builder: " + fieldType);
        }

        private String getFixedBuilderFactoryDescriptor(Class<?> fieldType) {
            if (fieldType == double.class) return "(I)Lorg/elasticsearch/compute/data/DoubleVector$FixedBuilder;";
            if (fieldType == long.class) return "(I)Lorg/elasticsearch/compute/data/LongVector$FixedBuilder;";
            if (fieldType == int.class) return "(I)Lorg/elasticsearch/compute/data/IntVector$FixedBuilder;";
            if (fieldType == boolean.class) return "(I)Lorg/elasticsearch/compute/data/BooleanVector$FixedBuilder;";
            throw new IllegalArgumentException("Unsupported field type for fixed builder: " + fieldType);
        }

        private int getArrayElementType(Class<?> fieldType) {
            if (fieldType == double.class) return Opcodes.T_DOUBLE;
            if (fieldType == long.class) return Opcodes.T_LONG;
            if (fieldType == int.class) return Opcodes.T_INT;
            if (fieldType == boolean.class) return Opcodes.T_BOOLEAN;
            if (fieldType == BytesRef.class) return -1;
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private int getArrayStoreOpcode(Class<?> fieldType) {
            if (fieldType == double.class) return Opcodes.DASTORE;
            if (fieldType == long.class) return Opcodes.LASTORE;
            if (fieldType == int.class) return Opcodes.IASTORE;
            if (fieldType == boolean.class) return Opcodes.BASTORE;
            if (fieldType == BytesRef.class) return Opcodes.AASTORE;
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getVectorGetterName(Class<?> fieldType) {
            if (fieldType == double.class) return "getDouble";
            if (fieldType == long.class) return "getLong";
            if (fieldType == int.class) return "getInt";
            if (fieldType == boolean.class) return "getBoolean";
            if (fieldType == BytesRef.class) return "getBytesRef";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getVectorGetterDescriptor(Class<?> fieldType) {
            if (fieldType == double.class) return "(I)D";
            if (fieldType == long.class) return "(I)J";
            if (fieldType == int.class) return "(I)I";
            if (fieldType == boolean.class) return "(I)Z";
            if (fieldType == BytesRef.class) return "(ILorg/apache/lucene/util/BytesRef;)Lorg/apache/lucene/util/BytesRef;";
            throw new IllegalArgumentException("Unsupported field type: " + fieldType);
        }

        private String getAppendMethod(Class<?> fieldType) {
            if (fieldType == double.class) return "appendDouble";
            if (fieldType == long.class) return "appendLong";
            if (fieldType == int.class) return "appendInt";
            if (fieldType == boolean.class) return "appendBoolean";
            throw new IllegalArgumentException("Unsupported field type for fixed builder: " + fieldType);
        }

        private String getAppendDescriptor(Class<?> fieldType) {
            String vectorType = getVectorTypeInternal(fieldType);
            String fixedBuilderType = vectorType + "$FixedBuilder";
            if (fieldType == double.class) return "(ID)L" + fixedBuilderType + ";";
            if (fieldType == long.class) return "(IJ)L" + fixedBuilderType + ";";
            if (fieldType == int.class) return "(II)L" + fixedBuilderType + ";";
            if (fieldType == boolean.class) return "(IZ)L" + fixedBuilderType + ";";
            throw new IllegalArgumentException("Unsupported field type for fixed builder: " + fieldType);
        }
    }

    /**
     * Implements baseRamBytesUsed() for variadic evaluators.
     */
    private class VariadicBaseRamBytesUsedImplementation implements Implementation {
        private final EvaluatorSpec spec;

        VariadicBaseRamBytesUsedImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = spec.evaluatorInternalName();

                // long baseRamBytesUsed = BASE_RAM_BYTES_USED;
                methodVisitor.visitFieldInsn(Opcodes.GETSTATIC, className, "BASE_RAM_BYTES_USED", "J");

                // for (ExpressionEvaluator e : values) { baseRamBytesUsed += e.baseRamBytesUsed(); }
                // Locals: 0=this, 1-2=result(long), 3=i
                int resultSlot = 1;
                int iSlot = 3;

                methodVisitor.visitVarInsn(Opcodes.LSTORE, resultSlot);

                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, iSlot);

                Label loopStart = new Label();
                Label loopEnd = new Label();

                // Frame at loop start: locals = [this, result (long), i]
                // Note: For F_FULL, we don't include TOP for long's second slot
                methodVisitor.visitLabel(loopStart);
                methodVisitor.visitFrame(Opcodes.F_FULL, 3, new Object[] { className, Opcodes.LONG, Opcodes.INTEGER }, 0, new Object[] {});

                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    className,
                    "values",
                    "[Lorg/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator;"
                );
                methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, loopEnd);

                // baseRamBytesUsed += values[i].baseRamBytesUsed();
                methodVisitor.visitVarInsn(Opcodes.LLOAD, resultSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    className,
                    "values",
                    "[Lorg/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator;"
                );
                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
                methodVisitor.visitInsn(Opcodes.AALOAD);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    "org/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator",
                    "baseRamBytesUsed",
                    "()J",
                    true
                );
                methodVisitor.visitInsn(Opcodes.LADD);
                methodVisitor.visitVarInsn(Opcodes.LSTORE, resultSlot);

                methodVisitor.visitIincInsn(iSlot, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, loopStart);

                // Frame at loop end: same as loop start
                methodVisitor.visitLabel(loopEnd);
                methodVisitor.visitFrame(Opcodes.F_FULL, 3, new Object[] { className, Opcodes.LONG, Opcodes.INTEGER }, 0, new Object[] {});

                methodVisitor.visitVarInsn(Opcodes.LLOAD, resultSlot);
                methodVisitor.visitInsn(Opcodes.LRETURN);

                return new ByteCodeAppender.Size(4, 4);
            };
        }
    }

    /**
     * Implements close() for variadic evaluators.
     */
    private class VariadicCloseMethodImplementation implements Implementation {
        private final EvaluatorSpec spec;

        VariadicCloseMethodImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = spec.evaluatorInternalName();

                // Releasables.closeExpectNoException(() -> Releasables.close(values));
                // For simplicity, we'll just close each element in a loop
                int iSlot = 1;

                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, iSlot);

                Label loopStart = new Label();
                Label loopEnd = new Label();

                // Frame at loop start: locals = [this, i]
                methodVisitor.visitLabel(loopStart);
                methodVisitor.visitFrame(Opcodes.F_FULL, 2, new Object[] { className, Opcodes.INTEGER }, 0, new Object[] {});

                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    className,
                    "values",
                    "[Lorg/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator;"
                );
                methodVisitor.visitInsn(Opcodes.ARRAYLENGTH);
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, loopEnd);

                // values[i].close();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    className,
                    "values",
                    "[Lorg/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator;"
                );
                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot);
                methodVisitor.visitInsn(Opcodes.AALOAD);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);

                methodVisitor.visitIincInsn(iSlot, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, loopStart);

                // Frame at loop end: same as loop start
                methodVisitor.visitLabel(loopEnd);
                methodVisitor.visitFrame(Opcodes.F_FULL, 2, new Object[] { className, Opcodes.INTEGER }, 0, new Object[] {});

                methodVisitor.visitInsn(Opcodes.RETURN);

                return new ByteCodeAppender.Size(3, 2);
            };
        }
    }

    /**
     * Implements toString() for variadic evaluators.
     */
    private class VariadicToStringImplementation implements Implementation {
        private final EvaluatorSpec spec;

        VariadicToStringImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = spec.evaluatorInternalName();

                // return "EvaluatorName[values=" + Arrays.toString(values) + "]";
                methodVisitor.visitTypeInsn(Opcodes.NEW, "java/lang/StringBuilder");
                methodVisitor.visitInsn(Opcodes.DUP);
                methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/StringBuilder", "<init>", "()V", false);

                methodVisitor.visitLdcInsn(spec.evaluatorSimpleName() + "[values=");
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "java/lang/StringBuilder",
                    "append",
                    "(Ljava/lang/String;)Ljava/lang/StringBuilder;",
                    false
                );

                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    className,
                    "values",
                    "[Lorg/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator;"
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    "java/util/Arrays",
                    "toString",
                    "([Ljava/lang/Object;)Ljava/lang/String;",
                    false
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "java/lang/StringBuilder",
                    "append",
                    "(Ljava/lang/String;)Ljava/lang/StringBuilder;",
                    false
                );

                methodVisitor.visitLdcInsn("]");
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "java/lang/StringBuilder",
                    "append",
                    "(Ljava/lang/String;)Ljava/lang/StringBuilder;",
                    false
                );

                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/StringBuilder", "toString", "()Ljava/lang/String;", false);
                methodVisitor.visitInsn(Opcodes.ARETURN);

                return new ByteCodeAppender.Size(3, 1);
            };
        }
    }

    private static final String WARNINGS_INTERNAL = "org/elasticsearch/compute/operator/Warnings";

    // ============================================================================================
    // MULTI-VALUE (MV) EVALUATOR IMPLEMENTATIONS
    // ============================================================================================
    // These implementations handle multi-value functions (e.g., MV_SUM, MV_AVG, MV_MIN).
    // MvEvaluators extend AbstractMultivalueFunction$AbstractNullableEvaluator and have:
    // - Constructor: (Source, ExpressionEvaluator, DriverContext) - calls super(driverContext, field)
    // - name() - returns function name
    // - evalNullable(Block) - pairwise processing loop
    // - warnings() - if warnExceptions
    // - baseRamBytesUsed() - returns BASE_RAM_BYTES_USED + field.baseRamBytesUsed()
    // Reference: See compile-time MvEvaluatorImplementer and generated MvSumIntEvaluator.java
    // ============================================================================================

    /**
     * Implements constructor for MvEvaluator.
     * <p>
     * Generates equivalent bytecode to:
     * <pre>{@code
     * public MvSum2IntEvaluator(Source source, ExpressionEvaluator field, DriverContext driverContext) {
     *     super(driverContext, field);
     *     this.source = source;
     * }
     * }</pre>
     * <p>
     * Reference: MvEvaluatorImplementer#ctor()
     */
    private class MvConstructorImplementation implements Implementation {
        private final EvaluatorSpec spec;
        private final boolean hasSource;

        MvConstructorImplementation(EvaluatorSpec spec, boolean hasSource) {
            this.spec = spec;
            this.hasSource = hasSource;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = spec.evaluatorInternalName();
                // Use correct base class based on warnExceptions
                // See MvEvaluatorImplementer.type() lines 133-139
                String mvBase = "org/elasticsearch/xpack/esql/expression/function/scalar/multivalue/";
                String baseEvaluatorInternal = hasSource
                    ? mvBase + "AbstractMultivalueFunction$AbstractNullableEvaluator"
                    : mvBase + "AbstractMultivalueFunction$AbstractEvaluator";

                // Call super(driverContext, field)
                // Stack: this, driverContext, field
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this

                if (hasSource) {
                    // Constructor params: (Source source, ExpressionEvaluator field, DriverContext driverContext)
                    // Slot 1 = source, Slot 2 = field, Slot 3 = driverContext
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // driverContext
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // field
                } else {
                    // Constructor params: (ExpressionEvaluator field, DriverContext driverContext)
                    // Slot 1 = field, Slot 2 = driverContext
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // driverContext
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // field
                }

                String initDesc = "(Lorg/elasticsearch/compute/operator/DriverContext;"
                    + "Lorg/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator;)V";
                methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, baseEvaluatorInternal, "<init>", initDesc, false);

                // this.source = source (if hasSource)
                if (hasSource) {
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // source
                    methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "source", "L" + SOURCE_INTERNAL + ";");
                }

                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(3, hasSource ? 4 : 3);
            };
        }
    }

    /**
     * Implements name() method for MvEvaluator.
     * <p>
     * Generates equivalent bytecode to:
     * <pre>{@code
     * @Override
     * public String name() {
     *     return "MvSum2";
     * }
     * }</pre>
     */
    private class MvNameMethodImplementation implements Implementation {
        private final EvaluatorSpec spec;

        MvNameMethodImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                // Return the function name (without the type suffix)
                String functionName = spec.declaringClass().getSimpleName();
                methodVisitor.visitLdcInsn(functionName);
                methodVisitor.visitInsn(Opcodes.ARETURN);
                return new ByteCodeAppender.Size(1, 1);
            };
        }
    }

    /**
     * Implements evalNullable(Block) method for MvEvaluator with pairwise processing.
     * <p>
     * Generates equivalent bytecode to:
     * <pre>{@code
     * @Override
     * public Block evalNullable(Block fieldVal) {
     *     IntBlock v = (IntBlock) fieldVal;
     *     int positionCount = v.getPositionCount();
     *     try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
     *         for (int p = 0; p < positionCount; p++) {
     *             int valueCount = v.getValueCount(p);
     *             if (valueCount == 0) {
     *                 builder.appendNull();
     *                 continue;
     *             }
     *             try {
     *                 int first = v.getFirstValueIndex(p);
     *                 int end = first + valueCount;
     *                 int value = v.getInt(first);
     *                 for (int i = first + 1; i < end; i++) {
     *                     int next = v.getInt(i);
     *                     value = MvSum2.process(value, next);
     *                 }
     *                 builder.appendInt(value);
     *             } catch (ArithmeticException e) {
     *                 warnings().registerException(e);
     *                 builder.appendNull();
     *             }
     *         }
     *         return builder.build();
     *     }
     * }
     * }</pre>
     * <p>
     * Reference: MvEvaluatorImplementer#eval()
     */
    private class MvEvalNullableImplementation implements Implementation {
        private final EvaluatorSpec spec;

        MvEvalNullableImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = spec.evaluatorInternalName();
                Class<?> fieldType = spec.parameterType(0);
                Class<?> returnType = spec.returnType();
                String blockType = getBlockType(fieldType);
                String blockInternal = getBlockInternal(fieldType);
                String builderInternal = blockInternal + "$Builder";
                String getValueMethod = getGetValueMethod(fieldType);
                String appendMethod = getAppendMethod(returnType);

                // Check for ascending optimization first
                // if (fieldVal.mvSortedAscending()) { return evalAscendingNullable(fieldVal); }
                // See MvEvaluatorImplementer.eval() lines 266-271
                if (spec.hasAscendingMethod()) {
                    Label notAscending = new Label();
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // fieldVal
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEINTERFACE,
                        "org/elasticsearch/compute/data/Block",
                        "mvSortedAscending",
                        "()Z",
                        true
                    );
                    methodVisitor.visitJumpInsn(Opcodes.IFEQ, notAscending);
                    // return evalAscendingNullable(fieldVal)
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // fieldVal
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        className,
                        "evalAscendingNullable",
                        "(Lorg/elasticsearch/compute/data/Block;)Lorg/elasticsearch/compute/data/Block;",
                        false
                    );
                    methodVisitor.visitInsn(Opcodes.ARETURN);
                    methodVisitor.visitLabel(notAscending);
                    methodVisitor.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
                }

                // Cast fieldVal to the appropriate block type
                // IntBlock v = (IntBlock) fieldVal;
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // fieldVal
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, blockInternal);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 2); // v

                // int positionCount = v.getPositionCount();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockInternal, "getPositionCount", "()I", true);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 3); // positionCount

                // try (Builder builder = driverContext.blockFactory().newXxxBlockBuilder(positionCount)) {
                Label tryStart = new Label();
                Label tryEnd = new Label();
                Label catchHandler = new Label();
                Label finallyHandler = new Label();

                // Get builder: driverContext.blockFactory().newXxxBlockBuilder(positionCount)
                String mvBase2 = "org/elasticsearch/xpack/esql/expression/function/scalar/multivalue/";
                String nullableEval = mvBase2 + "AbstractMultivalueFunction$AbstractNullableEvaluator";
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    nullableEval,
                    "driverContext",
                    "Lorg/elasticsearch/compute/operator/DriverContext;"
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/operator/DriverContext",
                    "blockFactory",
                    "()Lorg/elasticsearch/compute/data/BlockFactory;",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 3); // positionCount
                String newBuilderMethod = "new" + blockType + "Builder";
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/data/BlockFactory",
                    newBuilderMethod,
                    "(I)L" + builderInternal + ";",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 4); // builder

                methodVisitor.visitLabel(tryStart);

                // for (int p = 0; p < positionCount; p++) {
                Label positionLoopStart = new Label();
                Label positionLoopEnd = new Label();
                Label continuePosition = new Label();

                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 5); // p = 0

                methodVisitor.visitLabel(positionLoopStart);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] {
                        className,
                        "org/elasticsearch/compute/data/Block",
                        blockInternal,
                        Opcodes.INTEGER,
                        builderInternal,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                methodVisitor.visitVarInsn(Opcodes.ILOAD, 5); // p
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 3); // positionCount
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, positionLoopEnd);

                // int valueCount = v.getValueCount(p);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 5); // p
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockInternal, "getValueCount", "(I)I", true);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 6); // valueCount

                // if (valueCount == 0) { builder.appendNull(); continue; }
                Label notNull = new Label();
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 6); // valueCount
                methodVisitor.visitJumpInsn(Opcodes.IFNE, notNull);

                // builder.appendNull()
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderInternal, "appendNull", "()L" + builderInternal + ";", true);
                methodVisitor.visitInsn(Opcodes.POP);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, continuePosition);

                methodVisitor.visitLabel(notNull);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    7,
                    new Object[] {
                        className,
                        "org/elasticsearch/compute/data/Block",
                        blockInternal,
                        Opcodes.INTEGER,
                        builderInternal,
                        Opcodes.INTEGER,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                // Process values with optional try-catch for warnExceptions
                Label innerTryStart = null;
                Label innerTryEnd = null;
                Label innerCatchHandler = null;

                if (spec.hasWarnExceptions()) {
                    innerTryStart = new Label();
                    innerTryEnd = new Label();
                    innerCatchHandler = new Label();
                    methodVisitor.visitLabel(innerTryStart);
                }

                // int first = v.getFirstValueIndex(p);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 5); // p
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockInternal, "getFirstValueIndex", "(I)I", true);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 7); // first

                // int end = first + valueCount;
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 7); // first
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 6); // valueCount
                methodVisitor.visitInsn(Opcodes.IADD);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 8); // end

                // T value = v.getXxx(first);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 7); // first
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    blockInternal,
                    getValueMethod,
                    "(I)" + Type.getDescriptor(fieldType),
                    true
                );
                int valueSlot = 9;
                storeValue(methodVisitor, fieldType, valueSlot); // value

                // for (int i = first + 1; i < end; i++) {
                Label innerLoopStart = new Label();
                Label innerLoopEnd = new Label();

                methodVisitor.visitVarInsn(Opcodes.ILOAD, 7); // first
                methodVisitor.visitInsn(Opcodes.ICONST_1);
                methodVisitor.visitInsn(Opcodes.IADD);
                int iSlot = valueSlot + (fieldType == long.class || fieldType == double.class ? 2 : 1);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, iSlot); // i = first + 1

                methodVisitor.visitLabel(innerLoopStart);
                // Frame at inner loop start
                Object[] innerLoopLocals = buildMvInnerLoopLocals(className, blockInternal, builderInternal, fieldType, iSlot);
                methodVisitor.visitFrame(Opcodes.F_FULL, innerLoopLocals.length, innerLoopLocals, 0, new Object[] {});

                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot); // i
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 8); // end
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, innerLoopEnd);

                // T next = v.getXxx(i);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot); // i
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    blockInternal,
                    getValueMethod,
                    "(I)" + Type.getDescriptor(fieldType),
                    true
                );
                int nextSlot = iSlot + 1;
                storeValue(methodVisitor, fieldType, nextSlot); // next

                // value = FunctionClass.process(value, next);
                loadValue(methodVisitor, fieldType, valueSlot); // value
                loadValue(methodVisitor, fieldType, nextSlot); // next
                String processDesc = "("
                    + Type.getDescriptor(fieldType)
                    + Type.getDescriptor(fieldType)
                    + ")"
                    + Type.getDescriptor(returnType);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    spec.declaringClassInternalName(),
                    spec.processMethodName(),
                    processDesc,
                    false
                );
                storeValue(methodVisitor, returnType, valueSlot); // value = result

                // i++
                methodVisitor.visitIincInsn(iSlot, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, innerLoopStart);

                methodVisitor.visitLabel(innerLoopEnd);
                // Frame at inner loop end
                methodVisitor.visitFrame(Opcodes.F_FULL, innerLoopLocals.length, innerLoopLocals, 0, new Object[] {});

                // builder.appendXxx(value);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                loadValue(methodVisitor, returnType, valueSlot); // value
                String appendDesc = "(" + Type.getDescriptor(returnType) + ")L" + builderInternal + ";";
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderInternal, appendMethod, appendDesc, true);
                methodVisitor.visitInsn(Opcodes.POP);

                if (spec.hasWarnExceptions()) {
                    methodVisitor.visitLabel(innerTryEnd);
                    methodVisitor.visitJumpInsn(Opcodes.GOTO, continuePosition);

                    // catch (Exception e) { warnings().registerException(e); builder.appendNull(); }
                    methodVisitor.visitLabel(innerCatchHandler);
                    methodVisitor.visitFrame(
                        Opcodes.F_FULL,
                        7,
                        new Object[] {
                            className,
                            "org/elasticsearch/compute/data/Block",
                            blockInternal,
                            Opcodes.INTEGER,
                            builderInternal,
                            Opcodes.INTEGER,
                            Opcodes.INTEGER },
                        1,
                        new Object[] { "java/lang/Exception" }
                    );
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, 7); // e

                    // warnings().registerException(e)
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        className,
                        "warnings",
                        "()Lorg/elasticsearch/compute/operator/Warnings;",
                        false
                    );
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 7); // e
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        WARNINGS_INTERNAL,
                        "registerException",
                        "(Ljava/lang/Exception;)V",
                        false
                    );

                    // builder.appendNull()
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEINTERFACE,
                        builderInternal,
                        "appendNull",
                        "()L" + builderInternal + ";",
                        true
                    );
                    methodVisitor.visitInsn(Opcodes.POP);

                    // Register exception handler for each warn exception type
                    for (Class<? extends Exception> exceptionClass : spec.warnExceptions()) {
                        methodVisitor.visitTryCatchBlock(
                            innerTryStart,
                            innerTryEnd,
                            innerCatchHandler,
                            Type.getInternalName(exceptionClass)
                        );
                    }
                }

                // continue (p++)
                methodVisitor.visitLabel(continuePosition);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] {
                        className,
                        "org/elasticsearch/compute/data/Block",
                        blockInternal,
                        Opcodes.INTEGER,
                        builderInternal,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );
                methodVisitor.visitIincInsn(5, 1); // p++
                methodVisitor.visitJumpInsn(Opcodes.GOTO, positionLoopStart);

                methodVisitor.visitLabel(positionLoopEnd);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    5,
                    new Object[] { className, "org/elasticsearch/compute/data/Block", blockInternal, Opcodes.INTEGER, builderInternal },
                    0,
                    new Object[] {}
                );

                // return builder.build();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderInternal, "build", "()L" + blockInternal + ";", true);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 5); // result

                // Close builder (finally block)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);

                methodVisitor.visitVarInsn(Opcodes.ALOAD, 5); // result
                methodVisitor.visitInsn(Opcodes.ARETURN);

                methodVisitor.visitLabel(tryEnd);

                // Exception handler for try-with-resources
                methodVisitor.visitLabel(finallyHandler);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    5,
                    new Object[] { className, "org/elasticsearch/compute/data/Block", blockInternal, Opcodes.INTEGER, builderInternal },
                    1,
                    new Object[] { "java/lang/Throwable" }
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 5); // exception

                // Close builder
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);

                // Rethrow
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 5);
                methodVisitor.visitInsn(Opcodes.ATHROW);

                // Register try-with-resources handler
                methodVisitor.visitTryCatchBlock(tryStart, tryEnd, finallyHandler, "java/lang/Throwable");

                // Calculate max locals based on type
                // For double/long: value takes 2 slots, next takes 2 slots
                // Locals: this(0), fieldVal(1), v(2), positionCount(3), builder(4), p(5), valueCount(6),
                // first(7), end(8), value(9-10 for double/long), i(11), next(12-13 for double/long)
                int maxLocals = (fieldType == double.class || fieldType == long.class) ? 14 : 12;
                return new ByteCodeAppender.Size(4, maxLocals);
            };
        }

        private Object[] buildMvInnerLoopLocals(
            String className,
            String blockInternal,
            String builderInternal,
            Class<?> fieldType,
            int iSlot
        ) {
            // Locals: this, fieldVal, v, positionCount, builder, p, valueCount, first, end, value, i
            // For long/double, value takes 2 slots
            if (fieldType == long.class) {
                return new Object[] {
                    className,
                    "org/elasticsearch/compute/data/Block",
                    blockInternal,
                    Opcodes.INTEGER,
                    builderInternal,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.LONG,
                    Opcodes.INTEGER };
            } else if (fieldType == double.class) {
                return new Object[] {
                    className,
                    "org/elasticsearch/compute/data/Block",
                    blockInternal,
                    Opcodes.INTEGER,
                    builderInternal,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.DOUBLE,
                    Opcodes.INTEGER };
            } else {
                return new Object[] {
                    className,
                    "org/elasticsearch/compute/data/Block",
                    blockInternal,
                    Opcodes.INTEGER,
                    builderInternal,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER };
            }
        }

        private void storeValue(MethodVisitor mv, Class<?> type, int slot) {
            if (type == int.class || type == boolean.class) {
                mv.visitVarInsn(Opcodes.ISTORE, slot);
            } else if (type == long.class) {
                mv.visitVarInsn(Opcodes.LSTORE, slot);
            } else if (type == double.class) {
                mv.visitVarInsn(Opcodes.DSTORE, slot);
            } else {
                mv.visitVarInsn(Opcodes.ASTORE, slot);
            }
        }

        private void loadValue(MethodVisitor mv, Class<?> type, int slot) {
            if (type == int.class || type == boolean.class) {
                mv.visitVarInsn(Opcodes.ILOAD, slot);
            } else if (type == long.class) {
                mv.visitVarInsn(Opcodes.LLOAD, slot);
            } else if (type == double.class) {
                mv.visitVarInsn(Opcodes.DLOAD, slot);
            } else {
                mv.visitVarInsn(Opcodes.ALOAD, slot);
            }
        }
    }

    /**
     * Implements evalNotNullable(Block) method for MvEvaluator with pairwise processing.
     * This is only generated when warnExceptions is empty.
     * <p>
     * Key differences from evalNullable:
     * - Uses Vector.FixedBuilder instead of Block.Builder (for non-BytesRef types)
     * - Does NOT check for valueCount == 0 (no null handling)
     * - Returns builder.build().asBlock() instead of builder.build()
     * <p>
     * Generates equivalent bytecode to:
     * <pre>{@code
     * @Override
     * public Block evalNotNullable(Block fieldVal) {
     *     IntBlock v = (IntBlock) fieldVal;
     *     int positionCount = v.getPositionCount();
     *     try (IntVector.FixedBuilder builder = driverContext.blockFactory().newIntVectorFixedBuilder(positionCount)) {
     *         for (int p = 0; p < positionCount; p++) {
     *             int valueCount = v.getValueCount(p);
     *             int first = v.getFirstValueIndex(p);
     *             int end = first + valueCount;
     *             int value = v.getInt(first);
     *             for (int i = first + 1; i < end; i++) {
     *                 int next = v.getInt(i);
     *                 value = MvSum2.process(value, next);
     *             }
     *             builder.appendInt(value);
     *         }
     *         return builder.build().asBlock();
     *     }
     * }
     * }</pre>
     * <p>
     * Reference: MvEvaluatorImplementer#eval("evalNotNullable", false)
     */
    private class MvEvalNotNullableImplementation implements Implementation {
        private final EvaluatorSpec spec;

        MvEvalNotNullableImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = spec.evaluatorInternalName();
                Class<?> fieldType = spec.parameterType(0);
                Class<?> returnType = spec.returnType();
                String blockType = getBlockType(fieldType);
                String blockInternal = getBlockInternal(fieldType);
                String vectorInternal = getVectorInternal(returnType);
                String fixedBuilderInternal = vectorInternal + "$FixedBuilder";
                String getValueMethod = getGetValueMethod(fieldType);
                String appendMethod = getAppendMethod(returnType);

                // For BytesRef, we use BytesRefVector.Builder instead of FixedBuilder
                // See MvEvaluatorImplementer.evalShell() lines 210-216
                boolean useBytesRefBuilder = returnType == BytesRef.class;
                String builderInternal = useBytesRefBuilder
                    ? "org/elasticsearch/compute/data/BytesRefVector$Builder"
                    : fixedBuilderInternal;

                // Check for ascending optimization first
                // if (fieldVal.mvSortedAscending()) { return evalAscendingNotNullable(fieldVal); }
                // See MvEvaluatorImplementer.eval() lines 266-271
                if (spec.hasAscendingMethod()) {
                    Label notAscending = new Label();
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // fieldVal
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEINTERFACE,
                        "org/elasticsearch/compute/data/Block",
                        "mvSortedAscending",
                        "()Z",
                        true
                    );
                    methodVisitor.visitJumpInsn(Opcodes.IFEQ, notAscending);
                    // return evalAscendingNotNullable(fieldVal)
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // fieldVal
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        className,
                        "evalAscendingNotNullable",
                        "(Lorg/elasticsearch/compute/data/Block;)Lorg/elasticsearch/compute/data/Block;",
                        false
                    );
                    methodVisitor.visitInsn(Opcodes.ARETURN);
                    methodVisitor.visitLabel(notAscending);
                    methodVisitor.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
                }

                // Cast fieldVal to the appropriate block type
                // IntBlock v = (IntBlock) fieldVal;
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // fieldVal
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, blockInternal);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 2); // v

                // int positionCount = v.getPositionCount();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockInternal, "getPositionCount", "()I", true);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 3); // positionCount

                // try (FixedBuilder builder = driverContext.blockFactory().newXxxVectorFixedBuilder(positionCount)) {
                Label tryStart = new Label();
                Label tryEnd = new Label();
                Label finallyHandler = new Label();

                // Get builder: driverContext.blockFactory().newXxxVectorFixedBuilder(positionCount)
                // Use AbstractEvaluator base class (no warnExceptions)
                String baseEvaluatorInternal =
                    "org/elasticsearch/xpack/esql/expression/function/scalar/multivalue/AbstractMultivalueFunction$AbstractEvaluator";

                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    baseEvaluatorInternal,
                    "driverContext",
                    "Lorg/elasticsearch/compute/operator/DriverContext;"
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/operator/DriverContext",
                    "blockFactory",
                    "()Lorg/elasticsearch/compute/data/BlockFactory;",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 3); // positionCount

                if (useBytesRefBuilder) {
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        "org/elasticsearch/compute/data/BlockFactory",
                        "newBytesRefVectorBuilder",
                        "(I)L" + builderInternal + ";",
                        false
                    );
                } else {
                    String newBuilderMethod = "new" + getVectorType(returnType) + "FixedBuilder";
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        "org/elasticsearch/compute/data/BlockFactory",
                        newBuilderMethod,
                        "(I)L" + builderInternal + ";",
                        false
                    );
                }
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 4); // builder

                methodVisitor.visitLabel(tryStart);

                // for (int p = 0; p < positionCount; p++) {
                Label positionLoopStart = new Label();
                Label positionLoopEnd = new Label();

                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 5); // p = 0

                methodVisitor.visitLabel(positionLoopStart);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] {
                        className,
                        "org/elasticsearch/compute/data/Block",
                        blockInternal,
                        Opcodes.INTEGER,
                        builderInternal,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                methodVisitor.visitVarInsn(Opcodes.ILOAD, 5); // p
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 3); // positionCount
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, positionLoopEnd);

                // int valueCount = v.getValueCount(p);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 5); // p
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockInternal, "getValueCount", "(I)I", true);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 6); // valueCount

                // NO null check - this is evalNotNullable

                // int first = v.getFirstValueIndex(p);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 5); // p
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockInternal, "getFirstValueIndex", "(I)I", true);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 7); // first

                // int end = first + valueCount;
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 7); // first
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 6); // valueCount
                methodVisitor.visitInsn(Opcodes.IADD);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 8); // end

                // T value = v.getXxx(first);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 7); // first
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    blockInternal,
                    getValueMethod,
                    "(I)" + Type.getDescriptor(fieldType),
                    true
                );
                int valueSlot = 9;
                storeValue(methodVisitor, fieldType, valueSlot); // value

                // for (int i = first + 1; i < end; i++) {
                Label innerLoopStart = new Label();
                Label innerLoopEnd = new Label();

                methodVisitor.visitVarInsn(Opcodes.ILOAD, 7); // first
                methodVisitor.visitInsn(Opcodes.ICONST_1);
                methodVisitor.visitInsn(Opcodes.IADD);
                int iSlot = valueSlot + (fieldType == long.class || fieldType == double.class ? 2 : 1);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, iSlot); // i = first + 1

                methodVisitor.visitLabel(innerLoopStart);
                // Frame at inner loop start
                Object[] innerLoopLocals = buildMvNotNullableInnerLoopLocals(className, blockInternal, builderInternal, fieldType, iSlot);
                methodVisitor.visitFrame(Opcodes.F_FULL, innerLoopLocals.length, innerLoopLocals, 0, new Object[] {});

                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot); // i
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 8); // end
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, innerLoopEnd);

                // T next = v.getXxx(i);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, iSlot); // i
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    blockInternal,
                    getValueMethod,
                    "(I)" + Type.getDescriptor(fieldType),
                    true
                );
                int nextSlot = iSlot + 1;
                storeValue(methodVisitor, fieldType, nextSlot); // next

                // value = FunctionClass.process(value, next);
                loadValue(methodVisitor, fieldType, valueSlot); // value
                loadValue(methodVisitor, fieldType, nextSlot); // next
                String processDesc = "("
                    + Type.getDescriptor(fieldType)
                    + Type.getDescriptor(fieldType)
                    + ")"
                    + Type.getDescriptor(returnType);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    spec.declaringClassInternalName(),
                    spec.processMethodName(),
                    processDesc,
                    false
                );
                storeValue(methodVisitor, returnType, valueSlot); // value = result

                // i++
                methodVisitor.visitIincInsn(iSlot, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, innerLoopStart);

                methodVisitor.visitLabel(innerLoopEnd);
                // Frame at inner loop end
                methodVisitor.visitFrame(Opcodes.F_FULL, innerLoopLocals.length, innerLoopLocals, 0, new Object[] {});

                // builder.appendXxx(value);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                loadValue(methodVisitor, returnType, valueSlot); // value
                String appendDesc = "(" + Type.getDescriptor(returnType) + ")L" + builderInternal + ";";
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderInternal, appendMethod, appendDesc, true);
                methodVisitor.visitInsn(Opcodes.POP);

                // p++
                methodVisitor.visitIincInsn(5, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, positionLoopStart);

                methodVisitor.visitLabel(positionLoopEnd);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    5,
                    new Object[] { className, "org/elasticsearch/compute/data/Block", blockInternal, Opcodes.INTEGER, builderInternal },
                    0,
                    new Object[] {}
                );

                // return builder.build().asBlock();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderInternal, "build", "()L" + vectorInternal + ";", true);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, vectorInternal, "asBlock", "()L" + blockInternal + ";", true);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 5); // result

                // Close builder (finally block)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);

                methodVisitor.visitVarInsn(Opcodes.ALOAD, 5); // result
                methodVisitor.visitInsn(Opcodes.ARETURN);

                methodVisitor.visitLabel(tryEnd);

                // Exception handler for try-with-resources
                methodVisitor.visitLabel(finallyHandler);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    5,
                    new Object[] { className, "org/elasticsearch/compute/data/Block", blockInternal, Opcodes.INTEGER, builderInternal },
                    1,
                    new Object[] { "java/lang/Throwable" }
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 5); // exception

                // Close builder
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);

                // Rethrow
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 5);
                methodVisitor.visitInsn(Opcodes.ATHROW);

                // Register try-with-resources handler
                methodVisitor.visitTryCatchBlock(tryStart, tryEnd, finallyHandler, "java/lang/Throwable");

                // Calculate max locals based on type
                int maxLocals = (fieldType == double.class || fieldType == long.class) ? 14 : 12;
                return new ByteCodeAppender.Size(4, maxLocals);
            };
        }

        private Object[] buildMvNotNullableInnerLoopLocals(
            String className,
            String blockInternal,
            String builderInternal,
            Class<?> fieldType,
            int iSlot
        ) {
            // Locals: this, fieldVal, v, positionCount, builder, p, valueCount, first, end, value, i
            // For long/double, value takes 2 slots
            if (fieldType == long.class) {
                return new Object[] {
                    className,
                    "org/elasticsearch/compute/data/Block",
                    blockInternal,
                    Opcodes.INTEGER,
                    builderInternal,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.LONG,
                    Opcodes.INTEGER };
            } else if (fieldType == double.class) {
                return new Object[] {
                    className,
                    "org/elasticsearch/compute/data/Block",
                    blockInternal,
                    Opcodes.INTEGER,
                    builderInternal,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.DOUBLE,
                    Opcodes.INTEGER };
            } else {
                return new Object[] {
                    className,
                    "org/elasticsearch/compute/data/Block",
                    blockInternal,
                    Opcodes.INTEGER,
                    builderInternal,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER,
                    Opcodes.INTEGER };
            }
        }

        private void storeValue(MethodVisitor mv, Class<?> type, int slot) {
            if (type == int.class || type == boolean.class) {
                mv.visitVarInsn(Opcodes.ISTORE, slot);
            } else if (type == long.class) {
                mv.visitVarInsn(Opcodes.LSTORE, slot);
            } else if (type == double.class) {
                mv.visitVarInsn(Opcodes.DSTORE, slot);
            } else {
                mv.visitVarInsn(Opcodes.ASTORE, slot);
            }
        }

        private void loadValue(MethodVisitor mv, Class<?> type, int slot) {
            if (type == int.class || type == boolean.class) {
                mv.visitVarInsn(Opcodes.ILOAD, slot);
            } else if (type == long.class) {
                mv.visitVarInsn(Opcodes.LLOAD, slot);
            } else if (type == double.class) {
                mv.visitVarInsn(Opcodes.DLOAD, slot);
            } else {
                mv.visitVarInsn(Opcodes.ALOAD, slot);
            }
        }
    }

    /**
     * Implements evalSingleValuedNullable(Block) for MvEvaluator.
     * Generated when single value function is specified.
     * <p>
     * Generates equivalent bytecode to:
     * <pre>{@code
     * public Block evalSingleValuedNullable(Block fieldVal) {
     *     IntBlock v = (IntBlock) fieldVal;
     *     int positionCount = v.getPositionCount();
     *     try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
     *         for (int p = 0; p < positionCount; p++) {
     *             int valueCount = v.getValueCount(p);
     *             if (valueCount == 0) {
     *                 builder.appendNull();
     *                 continue;
     *             }
     *             assert valueCount == 1;
     *             int first = v.getFirstValueIndex(p);
     *             int value = v.getInt(first);
     *             int result = FunctionClass.single(value);
     *             builder.appendInt(result);
     *         }
     *         return builder.build();
     *     }
     * }
     * }</pre>
     * <p>
     * Reference: MvEvaluatorImplementer#evalSingleValued()
     */
    private class MvEvalSingleValuedNullableImplementation implements Implementation {
        private final EvaluatorSpec spec;

        MvEvalSingleValuedNullableImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = spec.evaluatorInternalName();
                Class<?> fieldType = spec.parameterType(0);
                Class<?> returnType = spec.returnType();
                String blockInternal = getBlockInternal(fieldType);
                String resultBlockInternal = getBlockInternal(returnType);
                String builderInternal = resultBlockInternal + "$Builder";
                String getValueMethod = getGetValueMethod(fieldType);
                String appendMethod = getAppendMethod(returnType);

                // Get the single method
                java.lang.reflect.Method singleMethod = spec.findSingleMethod()
                    .orElseThrow(() -> new IllegalStateException("Single method not found"));

                // Cast fieldVal to the appropriate block type
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // fieldVal
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, blockInternal);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 2); // v

                // int positionCount = v.getPositionCount();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockInternal, "getPositionCount", "()I", true);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 3); // positionCount

                // try (Builder builder = driverContext.blockFactory().newXxxBlockBuilder(positionCount)) {
                Label tryStart = new Label();
                Label tryEnd = new Label();
                Label finallyHandler = new Label();

                // Get base class for driverContext field access
                String mvBasePath = "org/elasticsearch/xpack/esql/expression/function/scalar/multivalue/";
                String baseEvaluatorInternal = spec.hasWarnExceptions()
                    ? mvBasePath + "AbstractMultivalueFunction$AbstractNullableEvaluator"
                    : mvBasePath + "AbstractMultivalueFunction$AbstractEvaluator";

                // Get builder
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    baseEvaluatorInternal,
                    "driverContext",
                    "Lorg/elasticsearch/compute/operator/DriverContext;"
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/operator/DriverContext",
                    "blockFactory",
                    "()Lorg/elasticsearch/compute/data/BlockFactory;",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 3); // positionCount
                String newBuilderMethod = "new" + getBlockType(returnType) + "Builder";
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/data/BlockFactory",
                    newBuilderMethod,
                    "(I)L" + builderInternal + ";",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 4); // builder

                methodVisitor.visitLabel(tryStart);

                // for (int p = 0; p < positionCount; p++) {
                Label positionLoopStart = new Label();
                Label positionLoopEnd = new Label();
                Label continuePosition = new Label();

                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 5); // p = 0

                methodVisitor.visitLabel(positionLoopStart);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] {
                        className,
                        "org/elasticsearch/compute/data/Block",
                        blockInternal,
                        Opcodes.INTEGER,
                        builderInternal,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                methodVisitor.visitVarInsn(Opcodes.ILOAD, 5); // p
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 3); // positionCount
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, positionLoopEnd);

                // int valueCount = v.getValueCount(p);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 5); // p
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockInternal, "getValueCount", "(I)I", true);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 6); // valueCount

                // if (valueCount == 0) { builder.appendNull(); continue; }
                Label notNull = new Label();
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 6); // valueCount
                methodVisitor.visitJumpInsn(Opcodes.IFNE, notNull);

                // builder.appendNull()
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderInternal, "appendNull", "()L" + builderInternal + ";", true);
                methodVisitor.visitInsn(Opcodes.POP);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, continuePosition);

                methodVisitor.visitLabel(notNull);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    7,
                    new Object[] {
                        className,
                        "org/elasticsearch/compute/data/Block",
                        blockInternal,
                        Opcodes.INTEGER,
                        builderInternal,
                        Opcodes.INTEGER,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                // assert valueCount == 1 (we skip the assertion in bytecode)

                // int first = v.getFirstValueIndex(p);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 5); // p
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockInternal, "getFirstValueIndex", "(I)I", true);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 7); // first

                // builder.appendXxx(FunctionClass.single(v.getXxx(first)));
                // Load builder first to avoid SWAP issues with long/double
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 7); // first
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    blockInternal,
                    getValueMethod,
                    "(I)" + Type.getDescriptor(fieldType),
                    true
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    spec.declaringClassInternalName(),
                    singleMethod.getName(),
                    Type.getMethodDescriptor(singleMethod),
                    false
                );
                String appendDesc = "(" + Type.getDescriptor(returnType) + ")L" + builderInternal + ";";
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderInternal, appendMethod, appendDesc, true);
                methodVisitor.visitInsn(Opcodes.POP);

                // p++; continue
                methodVisitor.visitLabel(continuePosition);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] {
                        className,
                        "org/elasticsearch/compute/data/Block",
                        blockInternal,
                        Opcodes.INTEGER,
                        builderInternal,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );
                methodVisitor.visitIincInsn(5, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, positionLoopStart);

                methodVisitor.visitLabel(positionLoopEnd);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] {
                        className,
                        "org/elasticsearch/compute/data/Block",
                        blockInternal,
                        Opcodes.INTEGER,
                        builderInternal,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                // return builder.build();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderInternal, "build", "()L" + resultBlockInternal + ";", true);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 6); // result block

                methodVisitor.visitLabel(tryEnd);

                // Close builder
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);

                // Return result
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 6); // result block
                methodVisitor.visitInsn(Opcodes.ARETURN);

                // Finally handler - frame must match state at tryStart (5 locals: this, fieldVal, v, positionCount, builder)
                methodVisitor.visitLabel(finallyHandler);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    5,
                    new Object[] { className, "org/elasticsearch/compute/data/Block", blockInternal, Opcodes.INTEGER, builderInternal },
                    1,
                    new Object[] { "java/lang/Throwable" }
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 5); // exception (use slot 5 since p is not defined at tryStart)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 5); // exception
                methodVisitor.visitInsn(Opcodes.ATHROW);

                methodVisitor.visitTryCatchBlock(tryStart, tryEnd, finallyHandler, null);

                return new ByteCodeAppender.Size(4, 8);
            };
        }
    }

    /**
     * Implements evalSingleValuedNotNullable(Block) for MvEvaluator.
     * Generated when single value function is specified and no warnExceptions.
     * Uses FixedBuilder instead of Block.Builder.
     * <p>
     * Reference: MvEvaluatorImplementer#evalSingleValued("evalSingleValuedNotNullable", false)
     */
    private class MvEvalSingleValuedNotNullableImplementation implements Implementation {
        private final EvaluatorSpec spec;

        MvEvalSingleValuedNotNullableImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = spec.evaluatorInternalName();
                Class<?> fieldType = spec.parameterType(0);
                Class<?> returnType = spec.returnType();
                String blockInternal = getBlockInternal(fieldType);
                String vectorInternal = getVectorInternal(returnType);
                String fixedBuilderInternal = vectorInternal + "$FixedBuilder";
                String getValueMethod = getGetValueMethod(fieldType);
                String appendMethod = getAppendMethod(returnType);

                // For BytesRef, we use BytesRefVector.Builder instead of FixedBuilder
                boolean useBytesRefBuilder = returnType == BytesRef.class;
                String builderInternal = useBytesRefBuilder
                    ? "org/elasticsearch/compute/data/BytesRefVector$Builder"
                    : fixedBuilderInternal;

                // Get the single method
                java.lang.reflect.Method singleMethod = spec.findSingleMethod()
                    .orElseThrow(() -> new IllegalStateException("Single method not found"));

                // Cast fieldVal to the appropriate block type
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // fieldVal
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, blockInternal);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 2); // v

                // int positionCount = v.getPositionCount();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockInternal, "getPositionCount", "()I", true);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 3); // positionCount

                // try (FixedBuilder builder = driverContext.blockFactory().newXxxVectorFixedBuilder(positionCount)) {
                Label tryStart = new Label();
                Label tryEnd = new Label();
                Label finallyHandler = new Label();

                String baseEvaluatorInternal =
                    "org/elasticsearch/xpack/esql/expression/function/scalar/multivalue/AbstractMultivalueFunction$AbstractEvaluator";

                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    baseEvaluatorInternal,
                    "driverContext",
                    "Lorg/elasticsearch/compute/operator/DriverContext;"
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/operator/DriverContext",
                    "blockFactory",
                    "()Lorg/elasticsearch/compute/data/BlockFactory;",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 3); // positionCount

                if (useBytesRefBuilder) {
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        "org/elasticsearch/compute/data/BlockFactory",
                        "newBytesRefVectorBuilder",
                        "(I)L" + builderInternal + ";",
                        false
                    );
                } else {
                    String newBuilderMethod = "new" + getVectorType(returnType) + "FixedBuilder";
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        "org/elasticsearch/compute/data/BlockFactory",
                        newBuilderMethod,
                        "(I)L" + builderInternal + ";",
                        false
                    );
                }
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 4); // builder

                methodVisitor.visitLabel(tryStart);

                // for (int p = 0; p < positionCount; p++) {
                Label positionLoopStart = new Label();
                Label positionLoopEnd = new Label();

                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 5); // p = 0

                methodVisitor.visitLabel(positionLoopStart);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] {
                        className,
                        "org/elasticsearch/compute/data/Block",
                        blockInternal,
                        Opcodes.INTEGER,
                        builderInternal,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                methodVisitor.visitVarInsn(Opcodes.ILOAD, 5); // p
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 3); // positionCount
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, positionLoopEnd);

                // int valueCount = v.getValueCount(p); (for assertion, but we skip it)
                // assert valueCount == 1 (we skip the assertion in bytecode)

                // int first = v.getFirstValueIndex(p);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 5); // p
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockInternal, "getFirstValueIndex", "(I)I", true);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 6); // first

                // builder.appendXxx(FunctionClass.single(v.getXxx(first)));
                // Load builder first to avoid SWAP issues with long/double
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 6); // first
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    blockInternal,
                    getValueMethod,
                    "(I)" + Type.getDescriptor(fieldType),
                    true
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    spec.declaringClassInternalName(),
                    singleMethod.getName(),
                    Type.getMethodDescriptor(singleMethod),
                    false
                );
                String appendDesc = "(" + Type.getDescriptor(returnType) + ")L" + builderInternal + ";";
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderInternal, appendMethod, appendDesc, true);
                methodVisitor.visitInsn(Opcodes.POP);

                // p++
                methodVisitor.visitIincInsn(5, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, positionLoopStart);

                methodVisitor.visitLabel(positionLoopEnd);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] {
                        className,
                        "org/elasticsearch/compute/data/Block",
                        blockInternal,
                        Opcodes.INTEGER,
                        builderInternal,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                // return builder.build().asBlock();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderInternal, "build", "()L" + vectorInternal + ";", true);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    vectorInternal,
                    "asBlock",
                    "()L" + getBlockInternal(returnType) + ";",
                    true
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 6); // result block

                methodVisitor.visitLabel(tryEnd);

                // Close builder
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);

                // Return result
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 6); // result block
                methodVisitor.visitInsn(Opcodes.ARETURN);

                // Finally handler - frame must match state at tryStart (5 locals: this, fieldVal, v, positionCount, builder)
                methodVisitor.visitLabel(finallyHandler);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    5,
                    new Object[] { className, "org/elasticsearch/compute/data/Block", blockInternal, Opcodes.INTEGER, builderInternal },
                    1,
                    new Object[] { "java/lang/Throwable" }
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 5); // exception (use slot 5 since p is not defined at tryStart)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 5); // exception
                methodVisitor.visitInsn(Opcodes.ATHROW);

                methodVisitor.visitTryCatchBlock(tryStart, tryEnd, finallyHandler, null);

                return new ByteCodeAppender.Size(4, 8);
            };
        }
    }

    /**
     * Implements evalAscendingNullable(Block) for MvEvaluator.
     * Generated when ascending function is specified.
     * <p>
     * For index lookup mode (ascending(int valueCount) returns index):
     * <pre>{@code
     * private Block evalAscendingNullable(Block fieldVal) {
     *     IntBlock v = (IntBlock) fieldVal;
     *     int positionCount = v.getPositionCount();
     *     try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
     *         for (int p = 0; p < positionCount; p++) {
     *             int valueCount = v.getValueCount(p);
     *             if (valueCount == 0) {
     *                 builder.appendNull();
     *                 continue;
     *             }
     *             int first = v.getFirstValueIndex(p);
     *             int idx = FunctionClass.ascendingIndex(valueCount);
     *             int result = v.getInt(first + idx);
     *             builder.appendInt(result);
     *         }
     *         return builder.build();
     *     }
     * }
     * }</pre>
     * <p>
     * Reference: MvEvaluatorImplementer#evalAscending() and AscendingFunction.call()
     */
    private class MvEvalAscendingNullableImplementation implements Implementation {
        private final EvaluatorSpec spec;

        MvEvalAscendingNullableImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = spec.evaluatorInternalName();
                Class<?> fieldType = spec.parameterType(0);
                Class<?> returnType = spec.returnType();
                String blockInternal = getBlockInternal(fieldType);
                String resultBlockInternal = getBlockInternal(returnType);
                String builderInternal = resultBlockInternal + "$Builder";
                String getValueMethod = getGetValueMethod(fieldType);
                String appendMethod = getAppendMethod(returnType);

                // Get the ascending method
                java.lang.reflect.Method ascendingMethod = spec.findAscendingMethod()
                    .orElseThrow(() -> new IllegalStateException("Ascending method not found"));

                // Get base class for driverContext field access
                String mvPath = "org/elasticsearch/xpack/esql/expression/function/scalar/multivalue/";
                String baseEvaluatorInternal = spec.hasWarnExceptions()
                    ? mvPath + "AbstractMultivalueFunction$AbstractNullableEvaluator"
                    : mvPath + "AbstractMultivalueFunction$AbstractEvaluator";

                // Cast fieldVal to the appropriate block type
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // fieldVal
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, blockInternal);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 2); // v

                // int positionCount = v.getPositionCount();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockInternal, "getPositionCount", "()I", true);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 3); // positionCount

                // try (Builder builder = driverContext.blockFactory().newXxxBlockBuilder(positionCount)) {
                Label tryStart = new Label();
                Label tryEnd = new Label();
                Label finallyHandler = new Label();

                // Get builder
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    baseEvaluatorInternal,
                    "driverContext",
                    "Lorg/elasticsearch/compute/operator/DriverContext;"
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/operator/DriverContext",
                    "blockFactory",
                    "()Lorg/elasticsearch/compute/data/BlockFactory;",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 3); // positionCount
                String newBuilderMethod = "new" + getBlockType(returnType) + "Builder";
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/data/BlockFactory",
                    newBuilderMethod,
                    "(I)L" + builderInternal + ";",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 4); // builder

                methodVisitor.visitLabel(tryStart);

                // for (int p = 0; p < positionCount; p++) {
                Label positionLoopStart = new Label();
                Label positionLoopEnd = new Label();
                Label continuePosition = new Label();

                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 5); // p = 0

                methodVisitor.visitLabel(positionLoopStart);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] {
                        className,
                        "org/elasticsearch/compute/data/Block",
                        blockInternal,
                        Opcodes.INTEGER,
                        builderInternal,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                methodVisitor.visitVarInsn(Opcodes.ILOAD, 5); // p
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 3); // positionCount
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, positionLoopEnd);

                // int valueCount = v.getValueCount(p);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 5); // p
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockInternal, "getValueCount", "(I)I", true);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 6); // valueCount

                // if (valueCount == 0) { builder.appendNull(); continue; }
                Label notNull = new Label();
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 6); // valueCount
                methodVisitor.visitJumpInsn(Opcodes.IFNE, notNull);

                // builder.appendNull()
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderInternal, "appendNull", "()L" + builderInternal + ";", true);
                methodVisitor.visitInsn(Opcodes.POP);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, continuePosition);

                methodVisitor.visitLabel(notNull);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    7,
                    new Object[] {
                        className,
                        "org/elasticsearch/compute/data/Block",
                        blockInternal,
                        Opcodes.INTEGER,
                        builderInternal,
                        Opcodes.INTEGER,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                // int first = v.getFirstValueIndex(p);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 5); // p
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockInternal, "getFirstValueIndex", "(I)I", true);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 7); // first

                // int idx = FunctionClass.ascendingIndex(valueCount);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 6); // valueCount
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    spec.declaringClassInternalName(),
                    ascendingMethod.getName(),
                    Type.getMethodDescriptor(ascendingMethod),
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 8); // idx

                // builder.appendXxx(v.getXxx(first + idx));
                // Load builder first, then compute and load the value
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 7); // first
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 8); // idx
                methodVisitor.visitInsn(Opcodes.IADD);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    blockInternal,
                    getValueMethod,
                    "(I)" + Type.getDescriptor(returnType),
                    true
                );
                String appendDesc = "(" + Type.getDescriptor(returnType) + ")L" + builderInternal + ";";
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderInternal, appendMethod, appendDesc, true);
                methodVisitor.visitInsn(Opcodes.POP);

                // p++; continue
                methodVisitor.visitLabel(continuePosition);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] {
                        className,
                        "org/elasticsearch/compute/data/Block",
                        blockInternal,
                        Opcodes.INTEGER,
                        builderInternal,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );
                methodVisitor.visitIincInsn(5, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, positionLoopStart);

                methodVisitor.visitLabel(positionLoopEnd);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] {
                        className,
                        "org/elasticsearch/compute/data/Block",
                        blockInternal,
                        Opcodes.INTEGER,
                        builderInternal,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                // return builder.build();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderInternal, "build", "()L" + resultBlockInternal + ";", true);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 6); // result block

                methodVisitor.visitLabel(tryEnd);

                // Close builder
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);

                // Return result
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 6); // result block
                methodVisitor.visitInsn(Opcodes.ARETURN);

                // Finally handler - frame must match state at tryStart (5 locals: this, fieldVal, v, positionCount, builder)
                methodVisitor.visitLabel(finallyHandler);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    5,
                    new Object[] { className, "org/elasticsearch/compute/data/Block", blockInternal, Opcodes.INTEGER, builderInternal },
                    1,
                    new Object[] { "java/lang/Throwable" }
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 5); // exception (use slot 5 since p is not defined at tryStart)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 5); // exception
                methodVisitor.visitInsn(Opcodes.ATHROW);

                methodVisitor.visitTryCatchBlock(tryStart, tryEnd, finallyHandler, null);

                return new ByteCodeAppender.Size(4, 9);
            };
        }
    }

    /**
     * Implements evalAscendingNotNullable(Block) for MvEvaluator.
     * Generated when ascending function is specified.
     * Uses FixedBuilder instead of Block.Builder.
     * <p>
     * Reference: MvEvaluatorImplementer#evalAscending("evalAscendingNotNullable", false)
     */
    private class MvEvalAscendingNotNullableImplementation implements Implementation {
        private final EvaluatorSpec spec;

        MvEvalAscendingNotNullableImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = spec.evaluatorInternalName();
                Class<?> fieldType = spec.parameterType(0);
                Class<?> returnType = spec.returnType();
                String blockInternal = getBlockInternal(fieldType);
                String vectorInternal = getVectorInternal(returnType);
                String fixedBuilderInternal = vectorInternal + "$FixedBuilder";
                String getValueMethod = getGetValueMethod(fieldType);
                String appendMethod = getAppendMethod(returnType);

                // For BytesRef, we use BytesRefVector.Builder instead of FixedBuilder
                boolean useBytesRefBuilder = returnType == BytesRef.class;
                String builderInternal = useBytesRefBuilder
                    ? "org/elasticsearch/compute/data/BytesRefVector$Builder"
                    : fixedBuilderInternal;

                // Get the ascending method
                java.lang.reflect.Method ascendingMethod = spec.findAscendingMethod()
                    .orElseThrow(() -> new IllegalStateException("Ascending method not found"));

                // Get base class for driverContext field access
                String mvPath2 = "org/elasticsearch/xpack/esql/expression/function/scalar/multivalue/";
                String baseEvaluatorInternal = spec.hasWarnExceptions()
                    ? mvPath2 + "AbstractMultivalueFunction$AbstractNullableEvaluator"
                    : mvPath2 + "AbstractMultivalueFunction$AbstractEvaluator";

                // Cast fieldVal to the appropriate block type
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // fieldVal
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, blockInternal);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 2); // v

                // int positionCount = v.getPositionCount();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockInternal, "getPositionCount", "()I", true);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 3); // positionCount

                // try (FixedBuilder builder = driverContext.blockFactory().newXxxVectorFixedBuilder(positionCount)) {
                Label tryStart = new Label();
                Label tryEnd = new Label();
                Label finallyHandler = new Label();

                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    baseEvaluatorInternal,
                    "driverContext",
                    "Lorg/elasticsearch/compute/operator/DriverContext;"
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/operator/DriverContext",
                    "blockFactory",
                    "()Lorg/elasticsearch/compute/data/BlockFactory;",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 3); // positionCount

                if (useBytesRefBuilder) {
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        "org/elasticsearch/compute/data/BlockFactory",
                        "newBytesRefVectorBuilder",
                        "(I)L" + builderInternal + ";",
                        false
                    );
                } else {
                    String newBuilderMethod = "new" + getVectorType(returnType) + "FixedBuilder";
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        "org/elasticsearch/compute/data/BlockFactory",
                        newBuilderMethod,
                        "(I)L" + builderInternal + ";",
                        false
                    );
                }
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 4); // builder

                methodVisitor.visitLabel(tryStart);

                // for (int p = 0; p < positionCount; p++) {
                Label positionLoopStart = new Label();
                Label positionLoopEnd = new Label();

                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 5); // p = 0

                methodVisitor.visitLabel(positionLoopStart);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] {
                        className,
                        "org/elasticsearch/compute/data/Block",
                        blockInternal,
                        Opcodes.INTEGER,
                        builderInternal,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                methodVisitor.visitVarInsn(Opcodes.ILOAD, 5); // p
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 3); // positionCount
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, positionLoopEnd);

                // int valueCount = v.getValueCount(p);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 5); // p
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockInternal, "getValueCount", "(I)I", true);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 6); // valueCount

                // int first = v.getFirstValueIndex(p);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 5); // p
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockInternal, "getFirstValueIndex", "(I)I", true);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 7); // first

                // int idx = FunctionClass.ascendingIndex(valueCount);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 6); // valueCount
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    spec.declaringClassInternalName(),
                    ascendingMethod.getName(),
                    Type.getMethodDescriptor(ascendingMethod),
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 8); // idx

                // T result = v.getXxx(first + idx);
                // builder.appendXxx(v.getXxx(first + idx));
                // Load builder first to avoid SWAP issues with long/double
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // v
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 7); // first
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 8); // idx
                methodVisitor.visitInsn(Opcodes.IADD);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    blockInternal,
                    getValueMethod,
                    "(I)" + Type.getDescriptor(returnType),
                    true
                );
                String appendDesc = "(" + Type.getDescriptor(returnType) + ")L" + builderInternal + ";";
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderInternal, appendMethod, appendDesc, true);
                methodVisitor.visitInsn(Opcodes.POP);

                // p++
                methodVisitor.visitIincInsn(5, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, positionLoopStart);

                methodVisitor.visitLabel(positionLoopEnd);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    6,
                    new Object[] {
                        className,
                        "org/elasticsearch/compute/data/Block",
                        blockInternal,
                        Opcodes.INTEGER,
                        builderInternal,
                        Opcodes.INTEGER },
                    0,
                    new Object[] {}
                );

                // return builder.build().asBlock();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, builderInternal, "build", "()L" + vectorInternal + ";", true);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    vectorInternal,
                    "asBlock",
                    "()L" + getBlockInternal(returnType) + ";",
                    true
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 6); // result block

                methodVisitor.visitLabel(tryEnd);

                // Close builder
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);

                // Return result
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 6); // result block
                methodVisitor.visitInsn(Opcodes.ARETURN);

                // Finally handler - frame must match state at tryStart (5 locals: this, fieldVal, v, positionCount, builder)
                methodVisitor.visitLabel(finallyHandler);
                methodVisitor.visitFrame(
                    Opcodes.F_FULL,
                    5,
                    new Object[] { className, "org/elasticsearch/compute/data/Block", blockInternal, Opcodes.INTEGER, builderInternal },
                    1,
                    new Object[] { "java/lang/Throwable" }
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 5); // exception (use slot 5 since p is not defined at tryStart)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // builder
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "org/elasticsearch/core/Releasable", "close", "()V", true);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 5); // exception
                methodVisitor.visitInsn(Opcodes.ATHROW);

                methodVisitor.visitTryCatchBlock(tryStart, tryEnd, finallyHandler, null);

                return new ByteCodeAppender.Size(4, 9);
            };
        }
    }

    /**
     * Implements warnings() method for MvEvaluator.
     * <p>
     * Generates equivalent bytecode to:
     * <pre>{@code
     * private Warnings warnings() {
     *     if (warnings == null) {
     *         this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source);
     *     }
     *     return warnings;
     * }
     * }</pre>
     */
    private class MvWarningsMethodImplementation implements Implementation {
        private final EvaluatorSpec spec;

        MvWarningsMethodImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = spec.evaluatorInternalName();
                String mvBasePath = "org/elasticsearch/xpack/esql/expression/function/scalar/multivalue/";
                String abstractNullableEvaluatorInternal = mvBasePath + "AbstractMultivalueFunction$AbstractNullableEvaluator";

                Label notNull = new Label();

                // if (warnings == null)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "warnings", "L" + WARNINGS_INTERNAL + ";");
                methodVisitor.visitJumpInsn(Opcodes.IFNONNULL, notNull);

                // this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this

                // driverContext.warningsMode()
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    abstractNullableEvaluatorInternal,
                    "driverContext",
                    "Lorg/elasticsearch/compute/operator/DriverContext;"
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/operator/DriverContext",
                    "warningsMode",
                    "()L" + WARNINGS_MODE_INTERNAL + ";",
                    false
                );

                // source
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "source", "L" + SOURCE_INTERNAL + ";");

                // Warnings.createWarnings(warningsMode, source)
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    WARNINGS_INTERNAL,
                    "createWarnings",
                    "(L" + WARNINGS_MODE_INTERNAL + ";L" + SOURCE_INTERNAL + ";)L" + WARNINGS_INTERNAL + ";",
                    false
                );
                methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "warnings", "L" + WARNINGS_INTERNAL + ";");

                methodVisitor.visitLabel(notNull);
                methodVisitor.visitFrame(Opcodes.F_SAME, 0, null, 0, null);

                // return warnings
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "warnings", "L" + WARNINGS_INTERNAL + ";");
                methodVisitor.visitInsn(Opcodes.ARETURN);

                return new ByteCodeAppender.Size(3, 1);
            };
        }
    }

    /**
     * Implements baseRamBytesUsed() method for MvEvaluator.
     * <p>
     * Generates equivalent bytecode to:
     * <pre>{@code
     * @Override
     * public long baseRamBytesUsed() {
     *     return BASE_RAM_BYTES_USED + field.baseRamBytesUsed();
     * }
     * }</pre>
     */
    private class MvBaseRamBytesUsedImplementation implements Implementation {
        private final EvaluatorSpec spec;

        MvBaseRamBytesUsedImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = spec.evaluatorInternalName();
                String mvBase3 = "org/elasticsearch/xpack/esql/expression/function/scalar/multivalue/";
                String abstractNullableEvaluatorInternal = mvBase3 + "AbstractMultivalueFunction$AbstractNullableEvaluator";

                // BASE_RAM_BYTES_USED
                methodVisitor.visitFieldInsn(Opcodes.GETSTATIC, className, "BASE_RAM_BYTES_USED", "J");

                // + field.baseRamBytesUsed()
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETFIELD,
                    abstractNullableEvaluatorInternal,
                    "field",
                    "Lorg/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator;"
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    "org/elasticsearch/compute/operator/EvalOperator$ExpressionEvaluator",
                    "baseRamBytesUsed",
                    "()J",
                    true
                );
                methodVisitor.visitInsn(Opcodes.LADD);
                methodVisitor.visitInsn(Opcodes.LRETURN);

                return new ByteCodeAppender.Size(4, 1);
            };
        }
    }

    // ============================================================================================
    // SHARED UTILITIES AND HELPER METHODS
    // ============================================================================================
    // Helper methods used across different evaluator types for type mapping and bytecode generation.
    // ============================================================================================

    // Helper methods for MvEvaluator
    private String getBlockInternal(Class<?> fieldType) {
        if (fieldType == double.class) return "org/elasticsearch/compute/data/DoubleBlock";
        if (fieldType == long.class) return "org/elasticsearch/compute/data/LongBlock";
        if (fieldType == int.class) return "org/elasticsearch/compute/data/IntBlock";
        if (fieldType == boolean.class) return "org/elasticsearch/compute/data/BooleanBlock";
        if (fieldType == BytesRef.class) return "org/elasticsearch/compute/data/BytesRefBlock";
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }

    private String getVectorInternal(Class<?> fieldType) {
        if (fieldType == double.class) return "org/elasticsearch/compute/data/DoubleVector";
        if (fieldType == long.class) return "org/elasticsearch/compute/data/LongVector";
        if (fieldType == int.class) return "org/elasticsearch/compute/data/IntVector";
        if (fieldType == boolean.class) return "org/elasticsearch/compute/data/BooleanVector";
        if (fieldType == BytesRef.class) return "org/elasticsearch/compute/data/BytesRefVector";
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }

    private String getGetValueMethod(Class<?> fieldType) {
        if (fieldType == double.class) return "getDouble";
        if (fieldType == long.class) return "getLong";
        if (fieldType == int.class) return "getInt";
        if (fieldType == boolean.class) return "getBoolean";
        if (fieldType == BytesRef.class) return "getBytesRef";
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }

    private String getAppendMethod(Class<?> fieldType) {
        if (fieldType == double.class) return "appendDouble";
        if (fieldType == long.class) return "appendLong";
        if (fieldType == int.class) return "appendInt";
        if (fieldType == boolean.class) return "appendBoolean";
        if (fieldType == BytesRef.class) return "appendBytesRef";
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }

    /**
     * Implements static initializer for BASE_RAM_BYTES_USED.
     * <p>
     * Generates equivalent bytecode to:
     * <pre>{@code
     * private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Evaluator.class);
     * }</pre>
     * <p>
     * Reference: {@code EvaluatorImplementer#baseRamBytesUsed()}
     */
    private static class StaticInitializerImplementation implements Implementation {
        private final EvaluatorSpec spec;

        StaticInitializerImplementation(EvaluatorSpec spec) {
            this.spec = spec;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                // BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(EvaluatorClass.class)
                methodVisitor.visitLdcInsn(Type.getObjectType(implementationContext.getInstrumentedType().getInternalName()));
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    "org/apache/lucene/util/RamUsageEstimator",
                    "shallowSizeOfInstance",
                    "(Ljava/lang/Class;)J",
                    false
                );
                methodVisitor.visitFieldInsn(
                    Opcodes.PUTSTATIC,
                    implementationContext.getInstrumentedType().getInternalName(),
                    "BASE_RAM_BYTES_USED",
                    "J"
                );
                methodVisitor.visitInsn(Opcodes.RETURN);

                return new ByteCodeAppender.Size(2, 0);
            };
        }
    }
}

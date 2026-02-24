/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.runtime;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.ClassFileVersion;
import net.bytebuddy.asm.AsmVisitorWrapper;
import net.bytebuddy.description.field.FieldDescription;
import net.bytebuddy.description.field.FieldList;
import net.bytebuddy.description.method.MethodList;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.jar.asm.ClassVisitor;
import net.bytebuddy.jar.asm.ClassWriter;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;
import net.bytebuddy.pool.TypePool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Generates aggregator bytecode at runtime using ByteBuddy.
 * This class generates aggregator classes equivalent to those produced by the
 * compile-time {@code AggregatorImplementer}, but creates bytecode directly
 * using ByteBuddy's low-level ASM visitor API instead of JavaPoet source generation.
 *
 * <p>The generated aggregators follow the same pattern as static-generated ones:
 * <ul>
 *   <li>Implement {@code AggregatorFunction}</li>
 *   <li>Have fields for driverContext, state, channels</li>
 *   <li>Have methods: create(), intermediateStateDesc(), intermediateBlockCount(),
 *       addRawInput(), addIntermediateInput(), evaluateIntermediate(), evaluateFinal(),
 *       toString(), close()</li>
 * </ul>
 *
 * <p><b>Reference Implementation:</b> This class must stay aligned with the compile-time generator.
 * When implementing or modifying features, always reference:
 * <ul>
 *   <li>{@code org.elasticsearch.compute.gen.AggregatorImplementer} - main compile-time generator</li>
 *   <li>{@code org.elasticsearch.compute.gen.GroupingAggregatorImplementer} - grouping aggregator generator</li>
 *   <li>Generated aggregators in {@code x-pack/plugin/esql/compute/src/main/generated/} (e.g., SumIntAggregatorFunction.java)</li>
 * </ul>
 */
public final class RuntimeAggregatorGenerator {

    private static final Logger logger = LogManager.getLogger(RuntimeAggregatorGenerator.class);

    private final AggregatorCache cache;
    private final ClassLoader parentClassLoader;

    private static final ConcurrentHashMap<ClassLoader, RuntimeAggregatorGenerator> INSTANCES = new ConcurrentHashMap<>();

    public RuntimeAggregatorGenerator(ClassLoader parentClassLoader) {
        this.cache = new AggregatorCache(parentClassLoader);
        this.parentClassLoader = parentClassLoader;
    }

    /**
     * Gets the generator instance for the given classloader.
     */
    public static RuntimeAggregatorGenerator getInstance(ClassLoader classLoader) {
        return INSTANCES.computeIfAbsent(classLoader, RuntimeAggregatorGenerator::new);
    }

    /**
     * Generates or retrieves a cached aggregator class for the given spec.
     */
    public Class<? extends AggregatorFunction> getOrGenerateAggregator(AggregatorSpec spec) {
        return cache.getOrGenerateAggregator(spec, this::generateAggregatorBytecode);
    }

    /**
     * Generates or retrieves a cached grouping aggregator class for the given spec.
     * This also generates the AddInput inner classes for optimal performance.
     */
    public Class<? extends GroupingAggregatorFunction> getOrGenerateGroupingAggregator(AggregatorSpec spec) {
        return cache.getOrGenerateGroupingAggregatorWithAddInput(
            spec,
            this::generateGroupingAggregatorBytecode,
            this::generateBlockAddInputBytecode,
            this::generateVectorAddInputBytecode
        );
    }

    /**
     * Generates bytecode for a non-grouping aggregator based on the specification.
     * <p>
     * Reference: {@code AggregatorImplementer.type()} - generates the TypeSpec
     * </p>
     */
    public byte[] generateAggregatorBytecode(AggregatorSpec spec) {
        try {
            logger.debug("Generating aggregator for {}", spec.aggregatorSimpleName());

            Class<?> stateWrapperClass = loadStateClass(spec.stateWrapperClassName());

            DynamicType.Builder<?> builder = new ByteBuddy().with(ClassFileVersion.JAVA_V21)
                .subclass(Object.class)
                .name(spec.aggregatorClassName())
                .implement(AggregatorFunction.class);

            // Add static field: INTERMEDIATE_STATE_DESC
            builder = builder.defineField(
                "INTERMEDIATE_STATE_DESC",
                List.class,
                Visibility.PRIVATE,
                net.bytebuddy.description.modifier.FieldManifestation.FINAL,
                net.bytebuddy.description.modifier.Ownership.STATIC
            );

            // Add instance fields
            // When warnExceptions is non-empty, add warnings field (lazily initialized)
            if (spec.hasWarnExceptions()) {
                builder = builder.defineField("warnings", Warnings.class, Visibility.PRIVATE);
            }

            builder = builder.defineField(
                "driverContext",
                DriverContext.class,
                Visibility.PRIVATE,
                net.bytebuddy.description.modifier.FieldManifestation.FINAL
            )
                .defineField("state", stateWrapperClass, Visibility.PRIVATE, net.bytebuddy.description.modifier.FieldManifestation.FINAL)
                .defineField("channels", List.class, Visibility.PRIVATE, net.bytebuddy.description.modifier.FieldManifestation.FINAL);

            // Add constructor - same signature regardless of warnExceptions
            // Warnings is lazily initialized from driverContext when needed
            builder = builder.defineConstructor(Visibility.PUBLIC)
                .withParameters(DriverContext.class, List.class, stateWrapperClass)
                .intercept(new AggregatorConstructorImplementation(spec, stateWrapperClass));

            // Add warnings() method when warnExceptions is non-empty
            if (spec.hasWarnExceptions()) {
                builder = builder.defineMethod("warnings", Warnings.class, Visibility.PRIVATE)
                    .intercept(new AggregatorWarningsMethodImplementation(spec));
            }

            // Add static create() method - returns AggregatorFunction (the interface)
            // Same signature regardless of warnExceptions - warnings is lazily initialized
            builder = builder.defineMethod(
                "create",
                AggregatorFunction.class,
                Visibility.PUBLIC,
                net.bytebuddy.description.modifier.Ownership.STATIC
            ).withParameters(DriverContext.class, List.class).intercept(new AggregatorCreateMethodImplementation(spec, stateWrapperClass));

            // Add static intermediateStateDesc() method
            builder = builder.defineMethod(
                "intermediateStateDesc",
                List.class,
                Visibility.PUBLIC,
                net.bytebuddy.description.modifier.Ownership.STATIC
            ).intercept(new IntermediateStateDescImplementation(spec));

            // Add intermediateBlockCount() method
            builder = builder.defineMethod("intermediateBlockCount", int.class, Visibility.PUBLIC)
                .intercept(new IntermediateBlockCountImplementation(spec));

            // Add addRawInput(Page, BooleanVector) method
            builder = builder.defineMethod("addRawInput", void.class, Visibility.PUBLIC)
                .withParameters(Page.class, BooleanVector.class)
                .intercept(new AddRawInputImplementation(spec));

            // Add private helper methods for addRawInput
            builder = builder.defineMethod("addRawInputNotMasked", void.class, Visibility.PRIVATE)
                .withParameters(Page.class)
                .intercept(new AddRawInputNotMaskedImplementation(spec));

            builder = builder.defineMethod("addRawInputMasked", void.class, Visibility.PRIVATE)
                .withParameters(Page.class, BooleanVector.class)
                .intercept(new AddRawInputMaskedImplementation(spec));

            // Add vector and block processing methods
            Class<?> valueBlockClass = getBlockClass(spec.valueType());
            Class<?> valueVectorClass = getVectorClass(spec.valueType());

            builder = builder.defineMethod("addRawVector", void.class, Visibility.PRIVATE)
                .withParameters(valueVectorClass)
                .intercept(new AddRawVectorImplementation(spec, false));

            builder = builder.defineMethod("addRawVector", void.class, Visibility.PRIVATE)
                .withParameters(valueVectorClass, BooleanVector.class)
                .intercept(new AddRawVectorImplementation(spec, true));

            builder = builder.defineMethod("addRawBlock", void.class, Visibility.PRIVATE)
                .withParameters(valueBlockClass)
                .intercept(new AddRawBlockImplementation(spec, false));

            builder = builder.defineMethod("addRawBlock", void.class, Visibility.PRIVATE)
                .withParameters(valueBlockClass, BooleanVector.class)
                .intercept(new AddRawBlockImplementation(spec, true));

            // Add addIntermediateInput(Page) method
            builder = builder.defineMethod("addIntermediateInput", void.class, Visibility.PUBLIC)
                .withParameters(Page.class)
                .intercept(new AddIntermediateInputImplementation(spec));

            // Add evaluateIntermediate method
            builder = builder.defineMethod("evaluateIntermediate", void.class, Visibility.PUBLIC)
                .withParameters(Block[].class, int.class, DriverContext.class)
                .intercept(new EvaluateIntermediateImplementation(spec));

            // Add evaluateFinal method
            builder = builder.defineMethod("evaluateFinal", void.class, Visibility.PUBLIC)
                .withParameters(Block[].class, int.class, DriverContext.class)
                .intercept(new EvaluateFinalImplementation(spec));

            // Add toString() method
            builder = builder.defineMethod("toString", String.class, Visibility.PUBLIC)
                .intercept(new AggregatorToStringImplementation(spec));

            // Add close() method
            builder = builder.defineMethod("close", void.class, Visibility.PUBLIC).intercept(new AggregatorCloseImplementation(spec));

            // Add static initializer for INTERMEDIATE_STATE_DESC
            builder = builder.invokable(net.bytebuddy.matcher.ElementMatchers.isTypeInitializer())
                .intercept(new AggregatorStaticInitializerImplementation(spec));

            DynamicType.Unloaded<?> unloaded = builder.make();
            return unloaded.getBytes();

        } catch (Exception e) {
            logger.error("Failed to generate aggregator for " + spec.aggregatorSimpleName(), e);
            throw new RuntimeException("Failed to generate aggregator: " + e.getMessage(), e);
        }
    }

    /**
     * Generates bytecode for a grouping aggregator based on the specification.
     * <p>
     * Reference: {@code GroupingAggregatorImplementer.type()} - generates the TypeSpec
     * </p>
     */
    public byte[] generateGroupingAggregatorBytecode(AggregatorSpec spec) {
        try {
            logger.debug("Generating grouping aggregator for {}", spec.groupingAggregatorSimpleName());

            Class<?> arrayStateWrapperClass = loadStateClass(spec.arrayStateWrapperClassName());

            DynamicType.Builder<?> builder = new ByteBuddy().with(ClassFileVersion.JAVA_V21)
                .subclass(Object.class)
                .name(spec.groupingAggregatorClassName())
                .implement(GroupingAggregatorFunction.class)
                .visit(new FrameComputingVisitorWrapper());

            // Add static field: INTERMEDIATE_STATE_DESC
            builder = builder.defineField(
                "INTERMEDIATE_STATE_DESC",
                List.class,
                Visibility.PRIVATE,
                net.bytebuddy.description.modifier.FieldManifestation.FINAL,
                net.bytebuddy.description.modifier.Ownership.STATIC
            );

            // Add static fields for AddInput constructors (set by AggregatorCache after class definition)
            builder = builder.defineField(
                "BLOCK_ADD_INPUT_CTOR",
                java.lang.reflect.Constructor.class,
                Visibility.PACKAGE_PRIVATE,
                net.bytebuddy.description.modifier.Ownership.STATIC
            )
                .defineField(
                    "VECTOR_ADD_INPUT_CTOR",
                    java.lang.reflect.Constructor.class,
                    Visibility.PACKAGE_PRIVATE,
                    net.bytebuddy.description.modifier.Ownership.STATIC
                );

            // Add instance fields
            builder = builder.defineField(
                "state",
                arrayStateWrapperClass,
                Visibility.PRIVATE,
                net.bytebuddy.description.modifier.FieldManifestation.FINAL
            )
                .defineField("channels", List.class, Visibility.PRIVATE, net.bytebuddy.description.modifier.FieldManifestation.FINAL)
                .defineField(
                    "driverContext",
                    DriverContext.class,
                    Visibility.PRIVATE,
                    net.bytebuddy.description.modifier.FieldManifestation.FINAL
                );

            // Add constructor
            builder = builder.defineConstructor(Visibility.PUBLIC)
                .withParameters(List.class, arrayStateWrapperClass, DriverContext.class)
                .intercept(new GroupingAggregatorConstructorImplementation(spec, arrayStateWrapperClass));

            // Add static create() method - returns GroupingAggregatorFunction (the interface)
            builder = builder.defineMethod(
                "create",
                GroupingAggregatorFunction.class,
                Visibility.PUBLIC,
                net.bytebuddy.description.modifier.Ownership.STATIC
            )
                .withParameters(List.class, DriverContext.class)
                .intercept(new GroupingAggregatorCreateMethodImplementation(spec, arrayStateWrapperClass));

            // Add static intermediateStateDesc() method
            builder = builder.defineMethod(
                "intermediateStateDesc",
                List.class,
                Visibility.PUBLIC,
                net.bytebuddy.description.modifier.Ownership.STATIC
            ).intercept(new IntermediateStateDescImplementation(spec));

            // Add intermediateBlockCount() method
            builder = builder.defineMethod("intermediateBlockCount", int.class, Visibility.PUBLIC)
                .intercept(new IntermediateBlockCountImplementation(spec));

            // Add prepareProcessRawInputPage method
            builder = builder.defineMethod("prepareProcessRawInputPage", GroupingAggregatorFunction.AddInput.class, Visibility.PUBLIC)
                .withParameters(loadSeenGroupIdsClass(), Page.class)
                .intercept(new PrepareProcessRawInputPageImplementation(spec));

            // Add addRawInput methods for different group types
            // These are package-private (not private) so that the generated AddInput classes
            // can call them directly without reflection. This matches the effective visibility
            // of private methods accessed via synthetic accessors in compile-time generated code.
            Class<?> valueBlockClass = getBlockClass(spec.valueType());
            Class<?> valueVectorClass = getVectorClass(spec.valueType());

            // IntVector groups
            builder = builder.defineMethod("addRawInput", void.class, Visibility.PACKAGE_PRIVATE)
                .withParameters(int.class, IntVector.class, valueBlockClass)
                .intercept(new GroupingAddRawInputBlockImplementation(spec, IntVector.class));

            builder = builder.defineMethod("addRawInput", void.class, Visibility.PACKAGE_PRIVATE)
                .withParameters(int.class, IntVector.class, valueVectorClass)
                .intercept(new GroupingAddRawInputVectorImplementation(spec, IntVector.class));

            // IntArrayBlock groups
            builder = builder.defineMethod("addRawInput", void.class, Visibility.PACKAGE_PRIVATE)
                .withParameters(int.class, loadIntArrayBlockClass(), valueBlockClass)
                .intercept(new GroupingAddRawInputBlockImplementation(spec, loadIntArrayBlockClass()));

            builder = builder.defineMethod("addRawInput", void.class, Visibility.PACKAGE_PRIVATE)
                .withParameters(int.class, loadIntArrayBlockClass(), valueVectorClass)
                .intercept(new GroupingAddRawInputVectorImplementation(spec, loadIntArrayBlockClass()));

            // IntBigArrayBlock groups
            builder = builder.defineMethod("addRawInput", void.class, Visibility.PACKAGE_PRIVATE)
                .withParameters(int.class, loadIntBigArrayBlockClass(), valueBlockClass)
                .intercept(new GroupingAddRawInputBlockImplementation(spec, loadIntBigArrayBlockClass()));

            builder = builder.defineMethod("addRawInput", void.class, Visibility.PACKAGE_PRIVATE)
                .withParameters(int.class, loadIntBigArrayBlockClass(), valueVectorClass)
                .intercept(new GroupingAddRawInputVectorImplementation(spec, loadIntBigArrayBlockClass()));

            // Add addIntermediateInput methods for different group types
            builder = builder.defineMethod("addIntermediateInput", void.class, Visibility.PUBLIC)
                .withParameters(int.class, loadIntArrayBlockClass(), Page.class)
                .intercept(new GroupingAddIntermediateInputImplementation(spec, loadIntArrayBlockClass()));

            builder = builder.defineMethod("addIntermediateInput", void.class, Visibility.PUBLIC)
                .withParameters(int.class, loadIntBigArrayBlockClass(), Page.class)
                .intercept(new GroupingAddIntermediateInputImplementation(spec, loadIntBigArrayBlockClass()));

            builder = builder.defineMethod("addIntermediateInput", void.class, Visibility.PUBLIC)
                .withParameters(int.class, IntVector.class, Page.class)
                .intercept(new GroupingAddIntermediateInputImplementation(spec, IntVector.class));

            // Add maybeEnableGroupIdTracking method
            builder = builder.defineMethod("maybeEnableGroupIdTracking", void.class, Visibility.PRIVATE)
                .withParameters(loadSeenGroupIdsClass(), valueBlockClass)
                .intercept(new MaybeEnableGroupIdTrackingImplementation(spec));

            // Add selectedMayContainUnseenGroups method
            builder = builder.defineMethod("selectedMayContainUnseenGroups", void.class, Visibility.PUBLIC)
                .withParameters(loadSeenGroupIdsClass())
                .intercept(new SelectedMayContainUnseenGroupsImplementation(spec));

            // Add evaluateIntermediate method
            builder = builder.defineMethod("evaluateIntermediate", void.class, Visibility.PUBLIC)
                .withParameters(Block[].class, int.class, IntVector.class)
                .intercept(new GroupingEvaluateIntermediateImplementation(spec));

            // Add evaluateFinal method
            builder = builder.defineMethod("evaluateFinal", void.class, Visibility.PUBLIC)
                .withParameters(Block[].class, int.class, IntVector.class, loadGroupingAggregatorEvaluationContextClass())
                .intercept(new GroupingEvaluateFinalImplementation(spec));

            // Add toString() method
            builder = builder.defineMethod("toString", String.class, Visibility.PUBLIC)
                .intercept(new AggregatorToStringImplementation(spec));

            // Add close() method - grouping aggregator uses array state wrapper
            builder = builder.defineMethod("close", void.class, Visibility.PUBLIC)
                .intercept(new GroupingAggregatorCloseImplementation(spec));

            // Add static initializer for INTERMEDIATE_STATE_DESC
            builder = builder.invokable(net.bytebuddy.matcher.ElementMatchers.isTypeInitializer())
                .intercept(new AggregatorStaticInitializerImplementation(spec));

            DynamicType.Unloaded<?> unloaded = builder.make();
            return unloaded.getBytes();

        } catch (Exception e) {
            logger.error("Failed to generate grouping aggregator for " + spec.groupingAggregatorSimpleName(), e);
            throw new RuntimeException("Failed to generate grouping aggregator: " + e.getMessage(), e);
        }
    }

    // ============================================================================================
    // ADD INPUT CLASS GENERATION
    // ============================================================================================

    /**
     * Generates bytecode for the BlockAddInput inner class.
     * This class implements GroupingAggregatorFunction.AddInput and holds a reference to
     * the aggregator and a Block value, providing direct method calls for optimal performance.
     */
    public byte[] generateBlockAddInputBytecode(AggregatorSpec spec, Class<?> aggregatorClass) {
        try {
            String className = spec.groupingAggregatorClassName() + "$BlockAddInput";
            Class<?> blockType = getBlockTypeClass(spec.valueType());

            DynamicType.Builder<?> builder = new ByteBuddy().with(ClassFileVersion.JAVA_V21)
                .subclass(Object.class)
                .name(className)
                .implement(GroupingAggregatorFunction.AddInput.class);

            // Add fields: aggregator and block
            builder = builder.defineField(
                "aggregator",
                aggregatorClass,
                Visibility.PRIVATE,
                net.bytebuddy.description.modifier.FieldManifestation.FINAL
            ).defineField("block", blockType, Visibility.PRIVATE, net.bytebuddy.description.modifier.FieldManifestation.FINAL);

            // Add constructor
            builder = builder.defineConstructor(Visibility.PUBLIC)
                .withParameters(aggregatorClass, blockType)
                .intercept(new BlockAddInputConstructorImplementation(aggregatorClass, blockType));

            // Add add(int, IntArrayBlock) method
            builder = builder.defineMethod("add", void.class, Visibility.PUBLIC)
                .withParameters(int.class, IntArrayBlock.class)
                .intercept(new BlockAddInputAddMethodImplementation(spec, aggregatorClass, blockType, IntArrayBlock.class));

            // Add add(int, IntBigArrayBlock) method
            builder = builder.defineMethod("add", void.class, Visibility.PUBLIC)
                .withParameters(int.class, IntBigArrayBlock.class)
                .intercept(new BlockAddInputAddMethodImplementation(spec, aggregatorClass, blockType, IntBigArrayBlock.class));

            // Add add(int, IntVector) method
            builder = builder.defineMethod("add", void.class, Visibility.PUBLIC)
                .withParameters(int.class, IntVector.class)
                .intercept(new BlockAddInputAddMethodImplementation(spec, aggregatorClass, blockType, IntVector.class));

            // Add close() method
            builder = builder.defineMethod("close", void.class, Visibility.PUBLIC).intercept(new AddInputCloseImplementation());

            DynamicType.Unloaded<?> unloaded = builder.make();
            return unloaded.getBytes();

        } catch (Exception e) {
            logger.error("Failed to generate BlockAddInput for " + spec.groupingAggregatorSimpleName(), e);
            throw new RuntimeException("Failed to generate BlockAddInput: " + e.getMessage(), e);
        }
    }

    /**
     * Generates bytecode for the VectorAddInput inner class.
     * This class implements GroupingAggregatorFunction.AddInput and holds a reference to
     * the aggregator and a Vector value, providing direct method calls for optimal performance.
     */
    public byte[] generateVectorAddInputBytecode(AggregatorSpec spec, Class<?> aggregatorClass) {
        try {
            String className = spec.groupingAggregatorClassName() + "$VectorAddInput";
            Class<?> vectorType = getVectorTypeClass(spec.valueType());

            DynamicType.Builder<?> builder = new ByteBuddy().with(ClassFileVersion.JAVA_V21)
                .subclass(Object.class)
                .name(className)
                .implement(GroupingAggregatorFunction.AddInput.class);

            // Add fields: aggregator and vector
            builder = builder.defineField(
                "aggregator",
                aggregatorClass,
                Visibility.PRIVATE,
                net.bytebuddy.description.modifier.FieldManifestation.FINAL
            ).defineField("vector", vectorType, Visibility.PRIVATE, net.bytebuddy.description.modifier.FieldManifestation.FINAL);

            // Add constructor
            builder = builder.defineConstructor(Visibility.PUBLIC)
                .withParameters(aggregatorClass, vectorType)
                .intercept(new VectorAddInputConstructorImplementation(aggregatorClass, vectorType));

            // Add add(int, IntArrayBlock) method
            builder = builder.defineMethod("add", void.class, Visibility.PUBLIC)
                .withParameters(int.class, IntArrayBlock.class)
                .intercept(new VectorAddInputAddMethodImplementation(spec, aggregatorClass, vectorType, IntArrayBlock.class));

            // Add add(int, IntBigArrayBlock) method
            builder = builder.defineMethod("add", void.class, Visibility.PUBLIC)
                .withParameters(int.class, IntBigArrayBlock.class)
                .intercept(new VectorAddInputAddMethodImplementation(spec, aggregatorClass, vectorType, IntBigArrayBlock.class));

            // Add add(int, IntVector) method
            builder = builder.defineMethod("add", void.class, Visibility.PUBLIC)
                .withParameters(int.class, IntVector.class)
                .intercept(new VectorAddInputAddMethodImplementation(spec, aggregatorClass, vectorType, IntVector.class));

            // Add close() method
            builder = builder.defineMethod("close", void.class, Visibility.PUBLIC).intercept(new AddInputCloseImplementation());

            DynamicType.Unloaded<?> unloaded = builder.make();
            return unloaded.getBytes();

        } catch (Exception e) {
            logger.error("Failed to generate VectorAddInput for " + spec.groupingAggregatorSimpleName(), e);
            throw new RuntimeException("Failed to generate VectorAddInput: " + e.getMessage(), e);
        }
    }

    // ============================================================================================
    // HELPER METHODS
    // ============================================================================================

    private Class<?> loadStateClass(String className) {
        try {
            return parentClassLoader.loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load state class: " + className, e);
        }
    }

    private Class<?> loadSeenGroupIdsClass() {
        try {
            return parentClassLoader.loadClass("org.elasticsearch.compute.aggregation.SeenGroupIds");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load SeenGroupIds class", e);
        }
    }

    private Class<?> loadIntArrayBlockClass() {
        try {
            return parentClassLoader.loadClass("org.elasticsearch.compute.data.IntArrayBlock");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load IntArrayBlock class", e);
        }
    }

    private Class<?> loadIntBigArrayBlockClass() {
        try {
            return parentClassLoader.loadClass("org.elasticsearch.compute.data.IntBigArrayBlock");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load IntBigArrayBlock class", e);
        }
    }

    private Class<?> loadGroupingAggregatorEvaluationContextClass() {
        try {
            return parentClassLoader.loadClass("org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load GroupingAggregatorEvaluationContext class", e);
        }
    }

    private static Class<?> getBlockClass(Class<?> fieldType) {
        if (fieldType == double.class) return DoubleBlock.class;
        if (fieldType == long.class) return LongBlock.class;
        if (fieldType == int.class) return IntBlock.class;
        if (fieldType == boolean.class) return BooleanBlock.class;
        if (fieldType == BytesRef.class) return BytesRefBlock.class;
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }

    private static Class<?> getVectorClass(Class<?> fieldType) {
        if (fieldType == double.class) return DoubleVector.class;
        if (fieldType == long.class) return LongVector.class;
        if (fieldType == int.class) return IntVector.class;
        if (fieldType == boolean.class) return BooleanVector.class;
        if (fieldType == BytesRef.class) return BytesRefVector.class;
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }

    private static String getBlockTypeInternal(Class<?> fieldType) {
        if (fieldType == double.class) return "org/elasticsearch/compute/data/DoubleBlock";
        if (fieldType == long.class) return "org/elasticsearch/compute/data/LongBlock";
        if (fieldType == int.class) return "org/elasticsearch/compute/data/IntBlock";
        if (fieldType == boolean.class) return "org/elasticsearch/compute/data/BooleanBlock";
        if (fieldType == BytesRef.class) return "org/elasticsearch/compute/data/BytesRefBlock";
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }

    private static String getVectorTypeInternal(Class<?> fieldType) {
        if (fieldType == double.class) return "org/elasticsearch/compute/data/DoubleVector";
        if (fieldType == long.class) return "org/elasticsearch/compute/data/LongVector";
        if (fieldType == int.class) return "org/elasticsearch/compute/data/IntVector";
        if (fieldType == boolean.class) return "org/elasticsearch/compute/data/BooleanVector";
        if (fieldType == BytesRef.class) return "org/elasticsearch/compute/data/BytesRefVector";
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }

    private static Class<?> getBlockTypeClass(Class<?> fieldType) {
        if (fieldType == double.class) return DoubleBlock.class;
        if (fieldType == long.class) return LongBlock.class;
        if (fieldType == int.class) return IntBlock.class;
        if (fieldType == boolean.class) return BooleanBlock.class;
        if (fieldType == BytesRef.class) return BytesRefBlock.class;
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }

    private static Class<?> getVectorTypeClass(Class<?> fieldType) {
        if (fieldType == double.class) return DoubleVector.class;
        if (fieldType == long.class) return LongVector.class;
        if (fieldType == int.class) return IntVector.class;
        if (fieldType == boolean.class) return BooleanVector.class;
        if (fieldType == BytesRef.class) return BytesRefVector.class;
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }

    private static String getVectorAccessorName(Class<?> fieldType) {
        if (fieldType == double.class) return "getDouble";
        if (fieldType == long.class) return "getLong";
        if (fieldType == int.class) return "getInt";
        if (fieldType == boolean.class) return "getBoolean";
        if (fieldType == BytesRef.class) return "getBytesRef";
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }

    private static String getBlockAccessorName(Class<?> fieldType) {
        if (fieldType == double.class) return "getDouble";
        if (fieldType == long.class) return "getLong";
        if (fieldType == int.class) return "getInt";
        if (fieldType == boolean.class) return "getBoolean";
        if (fieldType == BytesRef.class) return "getBytesRef";
        throw new IllegalArgumentException("Unsupported field type: " + fieldType);
    }

    /**
     * Returns true if the value type requires a scratch object for reading (BytesRef).
     */
    private static boolean requiresScratch(Class<?> valueType) {
        return valueType == BytesRef.class;
    }

    private static String getStateValueMethod(Class<?> stateType) {
        if (stateType == long.class) return "longValue";
        if (stateType == int.class) return "intValue";
        if (stateType == double.class) return "doubleValue";
        if (stateType == boolean.class) return "booleanValue";
        throw new IllegalArgumentException("Unsupported state type: " + stateType);
    }

    /**
     * Helper class to track labels for warnings try-catch blocks.
     * Used when warnExceptions is non-empty to wrap combine() calls.
     */
    private static class WarningsTryCatch {
        final Label tryStart;
        final Label tryEnd;
        final Label catchHandler;
        final Label afterCatch;

        WarningsTryCatch() {
            this.tryStart = new Label();
            this.tryEnd = new Label();
            this.catchHandler = new Label();
            this.afterCatch = new Label();
        }

        /**
         * Emits the try block start label.
         */
        void emitTryStart(MethodVisitor mv) {
            mv.visitLabel(tryStart);
        }

        /**
         * Emits the try block end, catch handler, and exception handling code.
         * The catch handler:
         * 1. Stores the exception
         * 2. Calls warnings.registerException(e)
         * 3. Calls state.failed(true)
         * 4. Returns from the method
         *
         * @param mv the method visitor
         * @param className the internal name of the aggregator class
         * @param stateWrapperInternal the internal name of the state wrapper class
         * @param spec the aggregator spec (for warnExceptions list)
         * @param exceptionSlot the local variable slot for storing the caught exception
         * @param frameLocals the local variable types for stack frame
         * @param frameLocalsCount the number of local variables
         */
        void emitCatchHandler(
            MethodVisitor mv,
            String className,
            String stateWrapperInternal,
            AggregatorSpec spec,
            int exceptionSlot,
            Object[] frameLocals,
            int frameLocalsCount
        ) {
            mv.visitLabel(tryEnd);
            mv.visitJumpInsn(Opcodes.GOTO, afterCatch);

            mv.visitLabel(catchHandler);
            mv.visitFrame(Opcodes.F_FULL, frameLocalsCount, frameLocals, 1, new Object[] { "java/lang/Exception" });

            // Store exception
            mv.visitVarInsn(Opcodes.ASTORE, exceptionSlot);

            // this.warnings().registerException(e)
            mv.visitVarInsn(Opcodes.ALOAD, 0); // this
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, className, "warnings", "()" + Type.getDescriptor(Warnings.class), false);
            mv.visitVarInsn(Opcodes.ALOAD, exceptionSlot);
            mv.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(Warnings.class),
                "registerException",
                "(Ljava/lang/Exception;)V",
                false
            );

            // this.state.failed(true)
            mv.visitVarInsn(Opcodes.ALOAD, 0); // this
            mv.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
            mv.visitInsn(Opcodes.ICONST_1);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, stateWrapperInternal, "failed", "(Z)V", false);

            // return
            mv.visitInsn(Opcodes.RETURN);

            // Register exception handlers for each warnException type
            for (Class<? extends Exception> exceptionType : spec.warnExceptions()) {
                mv.visitTryCatchBlock(tryStart, tryEnd, catchHandler, Type.getInternalName(exceptionType));
            }

            mv.visitLabel(afterCatch);
        }
    }

    private static int getLoadOpcode(Class<?> type) {
        if (type == int.class || type == short.class || type == byte.class || type == char.class || type == boolean.class) {
            return Opcodes.ILOAD;
        } else if (type == long.class) {
            return Opcodes.LLOAD;
        } else if (type == float.class) {
            return Opcodes.FLOAD;
        } else if (type == double.class) {
            return Opcodes.DLOAD;
        } else {
            return Opcodes.ALOAD;
        }
    }

    private static int getStoreOpcode(Class<?> type) {
        if (type == int.class || type == short.class || type == byte.class || type == char.class || type == boolean.class) {
            return Opcodes.ISTORE;
        } else if (type == long.class) {
            return Opcodes.LSTORE;
        } else if (type == float.class) {
            return Opcodes.FSTORE;
        } else if (type == double.class) {
            return Opcodes.DSTORE;
        } else {
            return Opcodes.ASTORE;
        }
    }

    private static int getReturnOpcode(Class<?> type) {
        if (type == int.class || type == short.class || type == byte.class || type == char.class || type == boolean.class) {
            return Opcodes.IRETURN;
        } else if (type == long.class) {
            return Opcodes.LRETURN;
        } else if (type == float.class) {
            return Opcodes.FRETURN;
        } else if (type == double.class) {
            return Opcodes.DRETURN;
        } else if (type == void.class) {
            return Opcodes.RETURN;
        } else {
            return Opcodes.ARETURN;
        }
    }

    // ============================================================================================
    // NON-GROUPING AGGREGATOR IMPLEMENTATIONS
    // ============================================================================================

    /**
     * Implements the constructor for non-grouping aggregator.
     * Reference: {@code AggregatorImplementer#ctor()}
     */
    private static class AggregatorConstructorImplementation implements Implementation {
        private final AggregatorSpec spec;
        private final Class<?> stateClass;

        AggregatorConstructorImplementation(AggregatorSpec spec, Class<?> stateClass) {
            this.spec = spec;
            this.stateClass = stateClass;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();

                // Parameter slots: this=0, driverContext=1, channels=2, state=3
                // (warnings is lazily initialized, not passed to constructor)

                // super()
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);

                // this.driverContext = driverContext
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1);
                methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "driverContext", Type.getDescriptor(DriverContext.class));

                // this.channels = channels
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2);
                methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "channels", Type.getDescriptor(List.class));

                // this.state = state
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3);
                methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "state", Type.getDescriptor(stateClass));

                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(2, 4);
            };
        }
    }

    /**
     * Implements the static create() method.
     * Reference: {@code AggregatorImplementer#create()}
     */
    private static class AggregatorCreateMethodImplementation implements Implementation {
        private final AggregatorSpec spec;
        private final Class<?> stateClass;

        AggregatorCreateMethodImplementation(AggregatorSpec spec, Class<?> stateClass) {
            this.spec = spec;
            this.stateClass = stateClass;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                String declaringClassInternal = spec.declaringClassInternalName();

                // Parameter slots for static create method: driverContext=0, channels=1

                // new AggregatorFunction(driverContext, channels, state)
                methodVisitor.visitTypeInsn(Opcodes.NEW, className);
                methodVisitor.visitInsn(Opcodes.DUP);

                // driverContext
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);

                // channels
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1);

                // Create state based on type
                if (spec.isPrimitiveState()) {
                    // Primitive state: new StateWrapper(DeclaringClass.init())
                    methodVisitor.visitTypeInsn(Opcodes.NEW, stateClass.getName().replace('.', '/'));
                    methodVisitor.visitInsn(Opcodes.DUP);

                    // Call init method (no parameters for primitive state)
                    String initDesc = Type.getMethodDescriptor(spec.initMethod());
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKESTATIC,
                        declaringClassInternal,
                        spec.initMethod().getName(),
                        initDesc,
                        false
                    );

                    // Call state wrapper constructor
                    String stateCtorDesc = "(" + Type.getDescriptor(spec.stateType()) + ")V";
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKESPECIAL,
                        stateClass.getName().replace('.', '/'),
                        "<init>",
                        stateCtorDesc,
                        false
                    );
                } else if (spec.isCustomState()) {
                    // Custom state (implements AggregatorState): DeclaringClass.initSingle(driverContext)
                    // Push driverContext for initSingle call
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);

                    // Call initSingle(driverContext)
                    String initDesc = Type.getMethodDescriptor(spec.initMethod());
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKESTATIC,
                        declaringClassInternal,
                        spec.initMethod().getName(),
                        initDesc,
                        false
                    );
                } else {
                    // Non-primitive, non-custom state - just call init directly
                    String initDesc = Type.getMethodDescriptor(spec.initMethod());
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKESTATIC,
                        declaringClassInternal,
                        spec.initMethod().getName(),
                        initDesc,
                        false
                    );
                }

                // Call constructor: (DriverContext, List, State)
                String ctorDesc = "("
                    + Type.getDescriptor(DriverContext.class)
                    + Type.getDescriptor(List.class)
                    + Type.getDescriptor(stateClass)
                    + ")V";
                methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, className, "<init>", ctorDesc, false);

                methodVisitor.visitInsn(Opcodes.ARETURN);
                // Stack analysis for primitive long state:
                // NEW, DUP (2), ALOAD x2 (4), NEW, DUP (6), INVOKESTATIC returns long (8 - long is 2 slots)
                // After INVOKESPECIAL LongState.<init> (5), after main INVOKESPECIAL (1)
                // Max stack = 8 for long/double state types, 7 for int/boolean, 5 for custom state
                int stackSize;
                if (spec.isPrimitiveState()) {
                    stackSize = (spec.stateType() == long.class || spec.stateType() == double.class) ? 8 : 7;
                } else {
                    stackSize = 5; // NEW, DUP, ALOAD, ALOAD, ALOAD, INVOKESTATIC, INVOKESPECIAL
                }
                return new ByteCodeAppender.Size(stackSize, 2);
            };
        }
    }

    /**
     * Implements the warnings() method for lazy initialization of Warnings.
     * Generated when warnExceptions is non-empty.
     * <p>
     * Generates:
     * <pre>
     * private Warnings warnings() {
     *     if (warnings == null) {
     *         this.warnings = Warnings.NOOP_WARNINGS;
     *     }
     *     return warnings;
     * }
     * </pre>
     * Note: Using NOOP_WARNINGS for simplicity. A full implementation would
     * need to pass source location information to create proper warnings.
     */
    private static class AggregatorWarningsMethodImplementation implements Implementation {
        private final AggregatorSpec spec;

        AggregatorWarningsMethodImplementation(AggregatorSpec spec) {
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

                // if (warnings == null) {
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "warnings", Type.getDescriptor(Warnings.class));
                Label notNull = new Label();
                methodVisitor.visitJumpInsn(Opcodes.IFNONNULL, notNull);

                // this.warnings = Warnings.NOOP_WARNINGS
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(
                    Opcodes.GETSTATIC,
                    Type.getInternalName(Warnings.class),
                    "NOOP_WARNINGS",
                    Type.getDescriptor(Warnings.class)
                );
                methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "warnings", Type.getDescriptor(Warnings.class));

                // }
                methodVisitor.visitLabel(notNull);
                methodVisitor.visitFrame(Opcodes.F_SAME, 0, null, 0, null);

                // return warnings;
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "warnings", Type.getDescriptor(Warnings.class));
                methodVisitor.visitInsn(Opcodes.ARETURN);

                return new ByteCodeAppender.Size(2, 1);
            };
        }
    }

    /**
     * Implements intermediateStateDesc() method.
     * Reference: {@code AggregatorImplementer#intermediateStateDesc()}
     */
    private static class IntermediateStateDescImplementation implements Implementation {
        private final AggregatorSpec spec;

        IntermediateStateDescImplementation(AggregatorSpec spec) {
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
                methodVisitor.visitFieldInsn(Opcodes.GETSTATIC, className, "INTERMEDIATE_STATE_DESC", Type.getDescriptor(List.class));
                methodVisitor.visitInsn(Opcodes.ARETURN);
                // Static method with no parameters: 0 locals needed
                return new ByteCodeAppender.Size(1, 0);
            };
        }
    }

    /**
     * Implements intermediateBlockCount() method (instance method).
     * Reference: {@code AggregatorImplementer#intermediateBlockCount()}
     */
    private static class IntermediateBlockCountImplementation implements Implementation {
        private final AggregatorSpec spec;

        IntermediateBlockCountImplementation(AggregatorSpec spec) {
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
                // return INTERMEDIATE_STATE_DESC.size()
                methodVisitor.visitFieldInsn(Opcodes.GETSTATIC, className, "INTERMEDIATE_STATE_DESC", Type.getDescriptor(List.class));
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/List", "size", "()I", true);
                methodVisitor.visitInsn(Opcodes.IRETURN);
                return new ByteCodeAppender.Size(1, 1);
            };
        }
    }

    /**
     * Implements addRawInput(Page, BooleanVector) method.
     * Reference: {@code AggregatorImplementer#addRawInput()}
     */
    private static class AddRawInputImplementation implements Implementation {
        private final AggregatorSpec spec;

        AddRawInputImplementation(AggregatorSpec spec) {
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

                // if (mask.allFalse()) { return; }
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // mask
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(BooleanVector.class), "allFalse", "()Z", true);
                Label notAllFalse = new Label();
                methodVisitor.visitJumpInsn(Opcodes.IFEQ, notAllFalse);
                methodVisitor.visitInsn(Opcodes.RETURN);

                methodVisitor.visitLabel(notAllFalse);
                methodVisitor.visitFrame(Opcodes.F_SAME, 0, null, 0, null);

                // else if (mask.allTrue()) { addRawInputNotMasked(page); }
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // mask
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(BooleanVector.class), "allTrue", "()Z", true);
                Label notAllTrue = new Label();
                methodVisitor.visitJumpInsn(Opcodes.IFEQ, notAllTrue);

                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // page
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    className,
                    "addRawInputNotMasked",
                    "(" + Type.getDescriptor(Page.class) + ")V",
                    false
                );
                methodVisitor.visitInsn(Opcodes.RETURN);

                // else { addRawInputMasked(page, mask); }
                methodVisitor.visitLabel(notAllTrue);
                methodVisitor.visitFrame(Opcodes.F_SAME, 0, null, 0, null);

                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // page
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // mask
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    className,
                    "addRawInputMasked",
                    "(" + Type.getDescriptor(Page.class) + Type.getDescriptor(BooleanVector.class) + ")V",
                    false
                );
                methodVisitor.visitInsn(Opcodes.RETURN);

                return new ByteCodeAppender.Size(3, 3);
            };
        }
    }

    /**
     * Implements addRawInputNotMasked(Page) method.
     * Reference: {@code AggregatorImplementer#addRawInputExploded(false)}
     */
    private static class AddRawInputNotMaskedImplementation implements Implementation {
        private final AggregatorSpec spec;

        AddRawInputNotMaskedImplementation(AggregatorSpec spec) {
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
                Class<?> valueType = spec.valueType();
                String blockType = getBlockTypeInternal(valueType);
                String vectorType = getVectorTypeInternal(valueType);

                // IntBlock vBlock = page.getBlock(channels.get(0));
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // page
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "channels", Type.getDescriptor(List.class));
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/List", "get", "(I)Ljava/lang/Object;", true);
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue", "()I", false);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    Type.getInternalName(Page.class),
                    "getBlock",
                    "(I)L" + Type.getInternalName(Block.class) + ";",
                    false
                );
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, blockType);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 2); // vBlock

                // IntVector vVector = vBlock.asVector();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "asVector", "()L" + vectorType + ";", true);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 3); // vVector

                // if (vVector == null) { addRawBlock(vBlock); return; }
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3);
                Label vectorNotNull = new Label();
                methodVisitor.visitJumpInsn(Opcodes.IFNONNULL, vectorNotNull);

                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // vBlock
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, className, "addRawBlock", "(L" + blockType + ";)V", false);
                methodVisitor.visitInsn(Opcodes.RETURN);

                // addRawVector(vVector);
                methodVisitor.visitLabel(vectorNotNull);
                methodVisitor.visitFrame(Opcodes.F_APPEND, 2, new Object[] { blockType, vectorType }, 0, null);

                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // vVector
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, className, "addRawVector", "(L" + vectorType + ";)V", false);
                methodVisitor.visitInsn(Opcodes.RETURN);

                return new ByteCodeAppender.Size(4, 4);
            };
        }
    }

    /**
     * Implements addRawInputMasked(Page, BooleanVector) method.
     * Reference: {@code AggregatorImplementer#addRawInputExploded(true)}
     */
    private static class AddRawInputMaskedImplementation implements Implementation {
        private final AggregatorSpec spec;

        AddRawInputMaskedImplementation(AggregatorSpec spec) {
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
                Class<?> valueType = spec.valueType();
                String blockType = getBlockTypeInternal(valueType);
                String vectorType = getVectorTypeInternal(valueType);

                // IntBlock vBlock = page.getBlock(channels.get(0));
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // page
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "channels", Type.getDescriptor(List.class));
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/List", "get", "(I)Ljava/lang/Object;", true);
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue", "()I", false);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    Type.getInternalName(Page.class),
                    "getBlock",
                    "(I)L" + Type.getInternalName(Block.class) + ";",
                    false
                );
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, blockType);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 3); // vBlock

                // IntVector vVector = vBlock.asVector();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "asVector", "()L" + vectorType + ";", true);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 4); // vVector

                // if (vVector == null) { addRawBlock(vBlock, mask); return; }
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4);
                Label vectorNotNull = new Label();
                methodVisitor.visitJumpInsn(Opcodes.IFNONNULL, vectorNotNull);

                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // vBlock
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // mask
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    className,
                    "addRawBlock",
                    "(L" + blockType + ";" + Type.getDescriptor(BooleanVector.class) + ")V",
                    false
                );
                methodVisitor.visitInsn(Opcodes.RETURN);

                // addRawVector(vVector, mask);
                methodVisitor.visitLabel(vectorNotNull);
                methodVisitor.visitFrame(Opcodes.F_APPEND, 2, new Object[] { blockType, vectorType }, 0, null);

                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // vVector
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // mask
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    className,
                    "addRawVector",
                    "(L" + vectorType + ";" + Type.getDescriptor(BooleanVector.class) + ")V",
                    false
                );
                methodVisitor.visitInsn(Opcodes.RETURN);

                return new ByteCodeAppender.Size(4, 5);
            };
        }
    }

    /**
     * Implements addRawVector method.
     * Reference: {@code AggregatorImplementer#addRawVector()}
     *
     * For non-primitive state with first method, uses two-loop pattern:
     * - First loop: finds initial value using first(), sets state.seen(true), then breaks
     * - Second loop: processes remaining values using combine()
     */
    private static class AddRawVectorImplementation implements Implementation {
        private final AggregatorSpec spec;
        private final boolean masked;

        AddRawVectorImplementation(AggregatorSpec spec, boolean masked) {
            this.spec = spec;
            this.masked = masked;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                Class<?> valueType = spec.valueType();
                Class<?> stateType = spec.stateType();
                String vectorType = getVectorTypeInternal(valueType);
                String declaringClassInternal = spec.declaringClassInternalName();
                String stateWrapperInternal = spec.stateWrapperClassName().replace('.', '/');
                boolean needsScratch = requiresScratch(valueType);
                boolean hasFirstMethod = spec.hasFirstMethod();
                boolean isPrimitive = spec.isPrimitiveState();
                boolean isCustomState = spec.isCustomState();

                // For primitive state without first method: state.seen(true) at the start
                // For custom state without first method: no seen() call needed (state handles it internally)
                if (hasFirstMethod == false && isCustomState == false) {
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                    methodVisitor.visitInsn(Opcodes.ICONST_1);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, stateWrapperInternal, "seen", "(Z)V", false);
                }

                int nextSlot = masked ? 3 : 2;

                // For BytesRef: BytesRef scratch = new BytesRef();
                int scratchSlot = -1;
                if (needsScratch) {
                    scratchSlot = nextSlot++;
                    methodVisitor.visitTypeInsn(Opcodes.NEW, Type.getInternalName(BytesRef.class));
                    methodVisitor.visitInsn(Opcodes.DUP);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, Type.getInternalName(BytesRef.class), "<init>", "()V", false);
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, scratchSlot);
                }

                // int valuesPosition = 0;
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                int posSlot = nextSlot++;
                methodVisitor.visitVarInsn(Opcodes.ISTORE, posSlot);

                if (hasFirstMethod) {
                    // Two-loop pattern for first method
                    emitFirstLoop(
                        methodVisitor,
                        className,
                        valueType,
                        vectorType,
                        declaringClassInternal,
                        stateWrapperInternal,
                        needsScratch,
                        scratchSlot,
                        posSlot
                    );
                    emitSecondLoop(
                        methodVisitor,
                        className,
                        valueType,
                        stateType,
                        vectorType,
                        declaringClassInternal,
                        stateWrapperInternal,
                        needsScratch,
                        scratchSlot,
                        posSlot,
                        isPrimitive
                    );
                } else if (isCustomState) {
                    // Single loop for custom state - just call combine(state, value)
                    emitCustomStateSingleLoop(
                        methodVisitor,
                        className,
                        valueType,
                        vectorType,
                        declaringClassInternal,
                        stateWrapperInternal,
                        needsScratch,
                        scratchSlot,
                        posSlot
                    );
                } else {
                    // Single loop for primitive state
                    emitSingleLoop(
                        methodVisitor,
                        className,
                        valueType,
                        stateType,
                        vectorType,
                        declaringClassInternal,
                        stateWrapperInternal,
                        needsScratch,
                        scratchSlot,
                        posSlot
                    );
                }

                methodVisitor.visitInsn(Opcodes.RETURN);

                // Calculate max locals: posSlot + 1 (for value) + extra slot for double/long
                int valueSlotSize = (valueType == double.class || valueType == long.class) ? 2 : 1;
                int maxLocals = posSlot + 1 + valueSlotSize + 1; // +1 for safety margin
                return new ByteCodeAppender.Size(6, maxLocals);
            };
        }

        private void emitFirstLoop(
            MethodVisitor mv,
            String className,
            Class<?> valueType,
            String vectorType,
            String declaringClassInternal,
            String stateWrapperInternal,
            boolean needsScratch,
            int scratchSlot,
            int posSlot
        ) {
            Label firstLoopStart = new Label();
            Label firstLoopEnd = new Label();

            mv.visitLabel(firstLoopStart);
            if (needsScratch) {
                mv.visitFrame(Opcodes.F_APPEND, 2, new Object[] { Type.getInternalName(BytesRef.class), Opcodes.INTEGER }, 0, null);
            } else {
                mv.visitFrame(Opcodes.F_APPEND, 1, new Object[] { Opcodes.INTEGER }, 0, null);
            }

            // while (state.seen() == false && valuesPosition < vVector.getPositionCount())
            mv.visitVarInsn(Opcodes.ALOAD, 0); // this
            mv.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, stateWrapperInternal, "seen", "()Z", false);
            mv.visitJumpInsn(Opcodes.IFNE, firstLoopEnd);

            mv.visitVarInsn(Opcodes.ILOAD, posSlot);
            mv.visitVarInsn(Opcodes.ALOAD, 1); // vVector
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, vectorType, "getPositionCount", "()I", true);
            mv.visitJumpInsn(Opcodes.IF_ICMPGE, firstLoopEnd);

            if (masked) {
                // if (mask.getBoolean(valuesPosition) == false) { valuesPosition++; continue; }
                mv.visitVarInsn(Opcodes.ALOAD, 2); // mask
                mv.visitVarInsn(Opcodes.ILOAD, posSlot);
                mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(BooleanVector.class), "getBoolean", "(I)Z", true);
                Label notMasked = new Label();
                mv.visitJumpInsn(Opcodes.IFNE, notMasked);
                mv.visitIincInsn(posSlot, 1);
                mv.visitJumpInsn(Opcodes.GOTO, firstLoopStart);
                mv.visitLabel(notMasked);
                mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
            }

            // Get value from vector
            if (needsScratch) {
                mv.visitVarInsn(Opcodes.ALOAD, 1); // vVector
                mv.visitVarInsn(Opcodes.ILOAD, posSlot);
                mv.visitVarInsn(Opcodes.ALOAD, scratchSlot);
                mv.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    vectorType,
                    "getBytesRef",
                    "(I" + Type.getDescriptor(BytesRef.class) + ")" + Type.getDescriptor(BytesRef.class),
                    true
                );
            } else {
                mv.visitVarInsn(Opcodes.ALOAD, 1); // vVector
                mv.visitVarInsn(Opcodes.ILOAD, posSlot);
                mv.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    vectorType,
                    getVectorAccessorName(valueType),
                    "(I)" + Type.getDescriptor(valueType),
                    true
                );
            }

            // Call first(state, value)
            mv.visitVarInsn(Opcodes.ALOAD, 0); // this
            mv.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
            mv.visitInsn(Opcodes.SWAP); // swap state and value
            String firstDesc = Type.getMethodDescriptor(spec.firstMethod());
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "first", firstDesc, false);

            // valuesPosition++
            mv.visitIincInsn(posSlot, 1);

            // state.seen(true)
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
            mv.visitInsn(Opcodes.ICONST_1);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, stateWrapperInternal, "seen", "(Z)V", false);

            // break
            mv.visitJumpInsn(Opcodes.GOTO, firstLoopEnd);

            mv.visitLabel(firstLoopEnd);
            if (needsScratch) {
                mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
            } else {
                mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
            }
        }

        private void emitSecondLoop(
            MethodVisitor mv,
            String className,
            Class<?> valueType,
            Class<?> stateType,
            String vectorType,
            String declaringClassInternal,
            String stateWrapperInternal,
            boolean needsScratch,
            int scratchSlot,
            int posSlot,
            boolean isPrimitive
        ) {
            Label secondLoopStart = new Label();
            Label secondLoopEnd = new Label();

            mv.visitLabel(secondLoopStart);
            mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);

            // while (valuesPosition < vVector.getPositionCount())
            mv.visitVarInsn(Opcodes.ILOAD, posSlot);
            mv.visitVarInsn(Opcodes.ALOAD, 1); // vVector
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, vectorType, "getPositionCount", "()I", true);
            mv.visitJumpInsn(Opcodes.IF_ICMPGE, secondLoopEnd);

            if (masked) {
                // if (mask.getBoolean(valuesPosition) == false) { valuesPosition++; continue; }
                mv.visitVarInsn(Opcodes.ALOAD, 2); // mask
                mv.visitVarInsn(Opcodes.ILOAD, posSlot);
                mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(BooleanVector.class), "getBoolean", "(I)Z", true);
                Label notMasked = new Label();
                mv.visitJumpInsn(Opcodes.IFNE, notMasked);
                mv.visitIincInsn(posSlot, 1);
                mv.visitJumpInsn(Opcodes.GOTO, secondLoopStart);
                mv.visitLabel(notMasked);
                mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
            }

            // Get value from vector
            if (needsScratch) {
                mv.visitVarInsn(Opcodes.ALOAD, 1); // vVector
                mv.visitVarInsn(Opcodes.ILOAD, posSlot);
                mv.visitVarInsn(Opcodes.ALOAD, scratchSlot);
                mv.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    vectorType,
                    "getBytesRef",
                    "(I" + Type.getDescriptor(BytesRef.class) + ")" + Type.getDescriptor(BytesRef.class),
                    true
                );
            } else {
                mv.visitVarInsn(Opcodes.ALOAD, 1); // vVector
                mv.visitVarInsn(Opcodes.ILOAD, posSlot);
                mv.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    vectorType,
                    getVectorAccessorName(valueType),
                    "(I)" + Type.getDescriptor(valueType),
                    true
                );
            }

            // Call combine(state, value) - for non-primitive, combine is void
            mv.visitVarInsn(Opcodes.ALOAD, 0); // this
            mv.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
            mv.visitInsn(Opcodes.SWAP); // swap state and value
            String combineDesc = Type.getMethodDescriptor(spec.combineMethod());
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "combine", combineDesc, false);

            // valuesPosition++
            mv.visitIincInsn(posSlot, 1);
            mv.visitJumpInsn(Opcodes.GOTO, secondLoopStart);

            mv.visitLabel(secondLoopEnd);
            mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
        }

        private void emitSingleLoop(
            MethodVisitor mv,
            String className,
            Class<?> valueType,
            Class<?> stateType,
            String vectorType,
            String declaringClassInternal,
            String stateWrapperInternal,
            boolean needsScratch,
            int scratchSlot,
            int posSlot
        ) {
            Label loopStart = new Label();
            Label loopEnd = new Label();

            mv.visitLabel(loopStart);
            if (needsScratch) {
                mv.visitFrame(Opcodes.F_APPEND, 2, new Object[] { Type.getInternalName(BytesRef.class), Opcodes.INTEGER }, 0, null);
            } else {
                mv.visitFrame(Opcodes.F_APPEND, 1, new Object[] { Opcodes.INTEGER }, 0, null);
            }

            // valuesPosition < vVector.getPositionCount()
            mv.visitVarInsn(Opcodes.ILOAD, posSlot);
            mv.visitVarInsn(Opcodes.ALOAD, 1); // vVector
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, vectorType, "getPositionCount", "()I", true);
            mv.visitJumpInsn(Opcodes.IF_ICMPGE, loopEnd);

            if (masked) {
                // if (mask.getBoolean(valuesPosition) == false) continue;
                mv.visitVarInsn(Opcodes.ALOAD, 2); // mask
                mv.visitVarInsn(Opcodes.ILOAD, posSlot);
                mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(BooleanVector.class), "getBoolean", "(I)Z", true);
                Label notMasked = new Label();
                mv.visitJumpInsn(Opcodes.IFNE, notMasked);
                mv.visitIincInsn(posSlot, 1);
                mv.visitJumpInsn(Opcodes.GOTO, loopStart);
                mv.visitLabel(notMasked);
                mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
            }

            // Get value from vector and store in local
            int valueSlot = posSlot + 1;
            if (needsScratch) {
                mv.visitVarInsn(Opcodes.ALOAD, 1); // vVector
                mv.visitVarInsn(Opcodes.ILOAD, posSlot);
                mv.visitVarInsn(Opcodes.ALOAD, scratchSlot);
                mv.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    vectorType,
                    "getBytesRef",
                    "(I" + Type.getDescriptor(BytesRef.class) + ")" + Type.getDescriptor(BytesRef.class),
                    true
                );
                mv.visitVarInsn(Opcodes.ASTORE, valueSlot);
            } else {
                mv.visitVarInsn(Opcodes.ALOAD, 1); // vVector
                mv.visitVarInsn(Opcodes.ILOAD, posSlot);
                mv.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    vectorType,
                    getVectorAccessorName(valueType),
                    "(I)" + Type.getDescriptor(valueType),
                    true
                );
                mv.visitVarInsn(getStoreOpcode(valueType), valueSlot);
            }

            // state.longValue(DeclaringClass.combine(state.longValue(), vValue))
            mv.visitVarInsn(Opcodes.ALOAD, 0); // this
            mv.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");

            mv.visitVarInsn(Opcodes.ALOAD, 0); // this
            mv.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
            String stateValueMethod = getStateValueMethod(stateType);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, stateWrapperInternal, stateValueMethod, "()" + Type.getDescriptor(stateType), false);

            mv.visitVarInsn(getLoadOpcode(valueType), valueSlot);

            String combineDesc = Type.getMethodDescriptor(spec.combineMethod());
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "combine", combineDesc, false);

            mv.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                stateWrapperInternal,
                stateValueMethod,
                "(" + Type.getDescriptor(stateType) + ")V",
                false
            );

            // valuesPosition++
            mv.visitIincInsn(posSlot, 1);
            mv.visitJumpInsn(Opcodes.GOTO, loopStart);

            mv.visitLabel(loopEnd);
            if (needsScratch) {
                mv.visitFrame(Opcodes.F_CHOP, 2, null, 0, null);
            } else {
                mv.visitFrame(Opcodes.F_CHOP, 1, null, 0, null);
            }
        }

        /**
         * Emits a single loop for custom state types (without first method).
         * For custom state, we just call combine(state, value) directly - the state
         * handles the "first value" logic internally.
         *
         * Reference: MaxBytesRefAggregatorFunction.addRawVector()
         */
        private void emitCustomStateSingleLoop(
            MethodVisitor mv,
            String className,
            Class<?> valueType,
            String vectorType,
            String declaringClassInternal,
            String stateWrapperInternal,
            boolean needsScratch,
            int scratchSlot,
            int posSlot
        ) {
            Label loopStart = new Label();
            Label loopEnd = new Label();

            mv.visitLabel(loopStart);
            if (needsScratch) {
                mv.visitFrame(Opcodes.F_APPEND, 2, new Object[] { Type.getInternalName(BytesRef.class), Opcodes.INTEGER }, 0, null);
            } else {
                mv.visitFrame(Opcodes.F_APPEND, 1, new Object[] { Opcodes.INTEGER }, 0, null);
            }

            // valuesPosition < vVector.getPositionCount()
            mv.visitVarInsn(Opcodes.ILOAD, posSlot);
            mv.visitVarInsn(Opcodes.ALOAD, 1); // vVector
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, vectorType, "getPositionCount", "()I", true);
            mv.visitJumpInsn(Opcodes.IF_ICMPGE, loopEnd);

            if (masked) {
                // if (mask.getBoolean(valuesPosition) == false) continue;
                mv.visitVarInsn(Opcodes.ALOAD, 2); // mask
                mv.visitVarInsn(Opcodes.ILOAD, posSlot);
                mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(BooleanVector.class), "getBoolean", "(I)Z", true);
                Label notMasked = new Label();
                mv.visitJumpInsn(Opcodes.IFNE, notMasked);
                mv.visitIincInsn(posSlot, 1);
                mv.visitJumpInsn(Opcodes.GOTO, loopStart);
                mv.visitLabel(notMasked);
                mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
            }

            // Get value from vector
            if (needsScratch) {
                // BytesRef value = vVector.getBytesRef(valuesPosition, scratch)
                mv.visitVarInsn(Opcodes.ALOAD, 1); // vVector
                mv.visitVarInsn(Opcodes.ILOAD, posSlot);
                mv.visitVarInsn(Opcodes.ALOAD, scratchSlot);
                mv.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    vectorType,
                    "getBytesRef",
                    "(I" + Type.getDescriptor(BytesRef.class) + ")" + Type.getDescriptor(BytesRef.class),
                    true
                );
            } else {
                mv.visitVarInsn(Opcodes.ALOAD, 1); // vVector
                mv.visitVarInsn(Opcodes.ILOAD, posSlot);
                mv.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    vectorType,
                    getVectorAccessorName(valueType),
                    "(I)" + Type.getDescriptor(valueType),
                    true
                );
            }

            // Call combine(state, value) - for custom state, combine is void
            mv.visitVarInsn(Opcodes.ALOAD, 0); // this
            mv.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
            mv.visitInsn(Opcodes.SWAP); // swap state and value
            String combineDesc = Type.getMethodDescriptor(spec.combineMethod());
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "combine", combineDesc, false);

            // valuesPosition++
            mv.visitIincInsn(posSlot, 1);
            mv.visitJumpInsn(Opcodes.GOTO, loopStart);

            mv.visitLabel(loopEnd);
            if (needsScratch) {
                mv.visitFrame(Opcodes.F_CHOP, 2, null, 0, null);
            } else {
                mv.visitFrame(Opcodes.F_CHOP, 1, null, 0, null);
            }
        }
    }

    /**
     * Implements addRawBlock method.
     * Reference: {@code AggregatorImplementer#addRawBlock()}
     *
     * For non-primitive state with first method, checks state.seen() in every iteration
     * to decide between first() and combine().
     */
    private static class AddRawBlockImplementation implements Implementation {
        private final AggregatorSpec spec;
        private final boolean masked;

        AddRawBlockImplementation(AggregatorSpec spec, boolean masked) {
            this.spec = spec;
            this.masked = masked;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                Class<?> valueType = spec.valueType();
                Class<?> stateType = spec.stateType();
                String blockType = getBlockTypeInternal(valueType);
                String declaringClassInternal = spec.declaringClassInternalName();
                String stateWrapperInternal = spec.stateWrapperClassName().replace('.', '/');
                boolean needsScratch = requiresScratch(valueType);
                boolean hasFirstMethod = spec.hasFirstMethod();

                int nextSlot = masked ? 3 : 2;

                // For BytesRef: BytesRef scratch = new BytesRef();
                int scratchSlot = -1;
                if (needsScratch) {
                    scratchSlot = nextSlot++;
                    methodVisitor.visitTypeInsn(Opcodes.NEW, Type.getInternalName(BytesRef.class));
                    methodVisitor.visitInsn(Opcodes.DUP);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, Type.getInternalName(BytesRef.class), "<init>", "()V", false);
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, scratchSlot);
                }

                // for (int p = 0; p < vBlock.getPositionCount(); p++)
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                int pSlot = nextSlot++;
                methodVisitor.visitVarInsn(Opcodes.ISTORE, pSlot); // p

                Label outerLoopStart = new Label();
                Label outerLoopEnd = new Label();
                methodVisitor.visitLabel(outerLoopStart);
                if (needsScratch) {
                    methodVisitor.visitFrame(
                        Opcodes.F_APPEND,
                        2,
                        new Object[] { Type.getInternalName(BytesRef.class), Opcodes.INTEGER },
                        0,
                        null
                    );
                } else {
                    methodVisitor.visitFrame(Opcodes.F_APPEND, 1, new Object[] { Opcodes.INTEGER }, 0, null);
                }

                // p < vBlock.getPositionCount()
                methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // vBlock
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "getPositionCount", "()I", true);
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, outerLoopEnd);

                if (masked) {
                    // if (mask.getBoolean(p) == false) continue;
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // mask
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEINTERFACE,
                        Type.getInternalName(BooleanVector.class),
                        "getBoolean",
                        "(I)Z",
                        true
                    );
                    Label notMasked = new Label();
                    methodVisitor.visitJumpInsn(Opcodes.IFNE, notMasked);
                    methodVisitor.visitIincInsn(pSlot, 1);
                    methodVisitor.visitJumpInsn(Opcodes.GOTO, outerLoopStart);
                    methodVisitor.visitLabel(notMasked);
                    methodVisitor.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
                }

                // int vValueCount = vBlock.getValueCount(p);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // vBlock
                methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "getValueCount", "(I)I", true);
                int valueCountSlot = pSlot + 1;
                methodVisitor.visitVarInsn(Opcodes.ISTORE, valueCountSlot);

                // if (vValueCount == 0) continue;
                methodVisitor.visitVarInsn(Opcodes.ILOAD, valueCountSlot);
                Label hasValues = new Label();
                methodVisitor.visitJumpInsn(Opcodes.IFNE, hasValues);
                methodVisitor.visitIincInsn(pSlot, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, outerLoopStart);

                methodVisitor.visitLabel(hasValues);
                methodVisitor.visitFrame(Opcodes.F_APPEND, 1, new Object[] { Opcodes.INTEGER }, 0, null);

                // For primitive state without first method: state.seen(true) once
                // For custom state without first method: no seen() call needed (state handles it internally)
                boolean isCustomState = spec.isCustomState();
                if (hasFirstMethod == false && isCustomState == false) {
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                    methodVisitor.visitInsn(Opcodes.ICONST_1);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, stateWrapperInternal, "seen", "(Z)V", false);
                }

                // int vStart = vBlock.getFirstValueIndex(p);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // vBlock
                methodVisitor.visitVarInsn(Opcodes.ILOAD, pSlot);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "getFirstValueIndex", "(I)I", true);
                int startSlot = valueCountSlot + 1;
                methodVisitor.visitVarInsn(Opcodes.ISTORE, startSlot);

                // int vEnd = vStart + vValueCount;
                methodVisitor.visitVarInsn(Opcodes.ILOAD, startSlot);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, valueCountSlot);
                methodVisitor.visitInsn(Opcodes.IADD);
                int endSlot = startSlot + 1;
                methodVisitor.visitVarInsn(Opcodes.ISTORE, endSlot);

                // for (int vOffset = vStart; vOffset < vEnd; vOffset++)
                methodVisitor.visitVarInsn(Opcodes.ILOAD, startSlot);
                int offsetSlot = endSlot + 1;
                methodVisitor.visitVarInsn(Opcodes.ISTORE, offsetSlot);

                Label innerLoopStart = new Label();
                Label innerLoopEnd = new Label();
                methodVisitor.visitLabel(innerLoopStart);
                methodVisitor.visitFrame(Opcodes.F_APPEND, 3, new Object[] { Opcodes.INTEGER, Opcodes.INTEGER, Opcodes.INTEGER }, 0, null);

                methodVisitor.visitVarInsn(Opcodes.ILOAD, offsetSlot);
                methodVisitor.visitVarInsn(Opcodes.ILOAD, endSlot);
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, innerLoopEnd);

                // Get value from block
                int valueSlot = offsetSlot + 1;
                if (needsScratch) {
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // vBlock
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, offsetSlot);
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, scratchSlot);
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEINTERFACE,
                        blockType,
                        "getBytesRef",
                        "(I" + Type.getDescriptor(BytesRef.class) + ")" + Type.getDescriptor(BytesRef.class),
                        true
                    );
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, valueSlot);
                } else {
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // vBlock
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, offsetSlot);
                    String accessorName = getBlockAccessorName(valueType);
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEINTERFACE,
                        blockType,
                        accessorName,
                        "(I)" + Type.getDescriptor(valueType),
                        true
                    );
                    methodVisitor.visitVarInsn(getStoreOpcode(valueType), valueSlot);
                }

                if (hasFirstMethod) {
                    // Check seen in every iteration: if (state.seen()) { combine } else { state.seen(true); first }
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, stateWrapperInternal, "seen", "()Z", false);
                    Label callFirst = new Label();
                    methodVisitor.visitJumpInsn(Opcodes.IFEQ, callFirst);

                    // Call combine(state, value)
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                    methodVisitor.visitVarInsn(getLoadOpcode(valueType), valueSlot);
                    String combineDesc = Type.getMethodDescriptor(spec.combineMethod());
                    methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "combine", combineDesc, false);
                    Label afterCall = new Label();
                    methodVisitor.visitJumpInsn(Opcodes.GOTO, afterCall);

                    // Call first(state, value) and state.seen(true)
                    methodVisitor.visitLabel(callFirst);
                    methodVisitor.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                    methodVisitor.visitInsn(Opcodes.ICONST_1);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, stateWrapperInternal, "seen", "(Z)V", false);

                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                    methodVisitor.visitVarInsn(getLoadOpcode(valueType), valueSlot);
                    String firstDesc = Type.getMethodDescriptor(spec.firstMethod());
                    methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "first", firstDesc, false);

                    methodVisitor.visitLabel(afterCall);
                    methodVisitor.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
                } else if (isCustomState) {
                    // Custom state: combine(state, value) - state handles seen internally
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                    methodVisitor.visitVarInsn(getLoadOpcode(valueType), valueSlot);
                    String combineDesc = Type.getMethodDescriptor(spec.combineMethod());
                    methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "combine", combineDesc, false);
                } else {
                    // Primitive state: state.setValue(combine(state.getValue(), value))
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");

                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                    String stateValueMethod = getStateValueMethod(stateType);
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        stateWrapperInternal,
                        stateValueMethod,
                        "()" + Type.getDescriptor(stateType),
                        false
                    );

                    methodVisitor.visitVarInsn(getLoadOpcode(valueType), valueSlot);

                    String combineDesc = Type.getMethodDescriptor(spec.combineMethod());
                    methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "combine", combineDesc, false);

                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        stateWrapperInternal,
                        stateValueMethod,
                        "(" + Type.getDescriptor(stateType) + ")V",
                        false
                    );
                }

                // vOffset++
                methodVisitor.visitIincInsn(offsetSlot, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, innerLoopStart);

                methodVisitor.visitLabel(innerLoopEnd);
                methodVisitor.visitFrame(Opcodes.F_CHOP, 3, null, 0, null);

                // p++
                methodVisitor.visitIincInsn(pSlot, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, outerLoopStart);

                methodVisitor.visitLabel(outerLoopEnd);
                if (needsScratch) {
                    methodVisitor.visitFrame(Opcodes.F_CHOP, 2, null, 0, null);
                } else {
                    methodVisitor.visitFrame(Opcodes.F_CHOP, 1, null, 0, null);
                }
                methodVisitor.visitInsn(Opcodes.RETURN);

                int maxLocals = valueSlot + 2;
                return new ByteCodeAppender.Size(6, maxLocals);
            };
        }
    }

    /**
     * Implements addIntermediateInput(Page) method.
     * Reference: {@code AggregatorImplementer#addIntermediateInput()}
     */
    private static class AddIntermediateInputImplementation implements Implementation {
        private final AggregatorSpec spec;

        AddIntermediateInputImplementation(AggregatorSpec spec) {
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
                String stateWrapperInternal = spec.stateWrapperClassName().replace('.', '/');
                String declaringClassInternal = spec.declaringClassInternalName();
                Class<?> stateType = spec.stateType();

                // Get intermediate state info
                List<AggregatorSpec.IntermediateStateInfo> states = spec.intermediateState();

                // For custom state (like MaxBytesRef)
                if (spec.isCustomState() && states.size() >= 2) {
                    return emitCustomStateAddIntermediateInput(
                        methodVisitor,
                        className,
                        stateWrapperInternal,
                        declaringClassInternal,
                        states
                    );
                }

                // For primitive state with seen flag (like SumInt)
                if (spec.isPrimitiveState() && states.size() >= 2) {
                    String valueStateName = states.get(0).name();
                    String valueStateType = states.get(0).elementType();
                    String seenStateName = states.get(1).name();

                    String valueBlockType = getBlockTypeInternalForElementType(valueStateType);
                    String valueVectorType = getVectorTypeInternalForElementType(valueStateType);

                    // Block sumUncast = page.getBlock(channels.get(0));
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // page
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "channels", Type.getDescriptor(List.class));
                    methodVisitor.visitInsn(Opcodes.ICONST_0);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/List", "get", "(I)Ljava/lang/Object;", true);
                    methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Integer");
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue", "()I", false);
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        Type.getInternalName(Page.class),
                        "getBlock",
                        "(I)L" + Type.getInternalName(Block.class) + ";",
                        false
                    );
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, 2); // sumUncast

                    // if (sumUncast.areAllValuesNull()) return;
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2);
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEINTERFACE,
                        Type.getInternalName(Block.class),
                        "areAllValuesNull",
                        "()Z",
                        true
                    );
                    Label sumNotNull = new Label();
                    methodVisitor.visitJumpInsn(Opcodes.IFEQ, sumNotNull);
                    methodVisitor.visitInsn(Opcodes.RETURN);

                    methodVisitor.visitLabel(sumNotNull);
                    methodVisitor.visitFrame(Opcodes.F_APPEND, 1, new Object[] { Type.getInternalName(Block.class) }, 0, null);

                    // LongVector sum = ((LongBlock) sumUncast).asVector();
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2);
                    methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, valueBlockType);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, valueBlockType, "asVector", "()L" + valueVectorType + ";", true);
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, 3); // sum

                    // Block seenUncast = page.getBlock(channels.get(1));
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // page
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "channels", Type.getDescriptor(List.class));
                    methodVisitor.visitInsn(Opcodes.ICONST_1);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/List", "get", "(I)Ljava/lang/Object;", true);
                    methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Integer");
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue", "()I", false);
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        Type.getInternalName(Page.class),
                        "getBlock",
                        "(I)L" + Type.getInternalName(Block.class) + ";",
                        false
                    );
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, 4); // seenUncast

                    // if (seenUncast.areAllValuesNull()) return;
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 4);
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEINTERFACE,
                        Type.getInternalName(Block.class),
                        "areAllValuesNull",
                        "()Z",
                        true
                    );
                    Label seenNotNull = new Label();
                    methodVisitor.visitJumpInsn(Opcodes.IFEQ, seenNotNull);
                    methodVisitor.visitInsn(Opcodes.RETURN);

                    methodVisitor.visitLabel(seenNotNull);
                    methodVisitor.visitFrame(
                        Opcodes.F_APPEND,
                        2,
                        new Object[] { valueVectorType, Type.getInternalName(Block.class) },
                        0,
                        null
                    );

                    // BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 4);
                    methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(BooleanBlock.class));
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEINTERFACE,
                        Type.getInternalName(BooleanBlock.class),
                        "asVector",
                        "()L" + Type.getInternalName(BooleanVector.class) + ";",
                        true
                    );
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, 5); // seen

                    // if (seen.getBoolean(0)) { ... }
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 5);
                    methodVisitor.visitInsn(Opcodes.ICONST_0);
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEINTERFACE,
                        Type.getInternalName(BooleanVector.class),
                        "getBoolean",
                        "(I)Z",
                        true
                    );
                    Label seenFalse = new Label();
                    methodVisitor.visitJumpInsn(Opcodes.IFEQ, seenFalse);

                    // state.longValue(DeclaringClass.combine(state.longValue(), sum.getLong(0)))
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");

                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                    String stateValueMethod = getStateValueMethod(stateType);
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        stateWrapperInternal,
                        stateValueMethod,
                        "()" + Type.getDescriptor(stateType),
                        false
                    );

                    // sum.getLong(0)
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 3);
                    methodVisitor.visitInsn(Opcodes.ICONST_0);
                    String vectorAccessor = getVectorAccessorForElementType(valueStateType);
                    Class<?> valueClass = getClassForElementType(valueStateType);
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEINTERFACE,
                        valueVectorType,
                        vectorAccessor,
                        "(I)" + Type.getDescriptor(valueClass),
                        true
                    );

                    // Call combine (state type, intermediate type)
                    String combineDesc = "("
                        + Type.getDescriptor(stateType)
                        + Type.getDescriptor(valueClass)
                        + ")"
                        + Type.getDescriptor(stateType);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "combine", combineDesc, false);

                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        stateWrapperInternal,
                        stateValueMethod,
                        "(" + Type.getDescriptor(stateType) + ")V",
                        false
                    );

                    // state.seen(true)
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                    methodVisitor.visitInsn(Opcodes.ICONST_1);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, stateWrapperInternal, "seen", "(Z)V", false);

                    methodVisitor.visitLabel(seenFalse);
                    methodVisitor.visitFrame(Opcodes.F_APPEND, 1, new Object[] { Type.getInternalName(BooleanVector.class) }, 0, null);
                }

                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(6, 6);
            };
        }

        private String getBlockTypeInternalForElementType(String elementType) {
            return switch (elementType) {
                case "LONG" -> Type.getInternalName(LongBlock.class);
                case "INT" -> Type.getInternalName(IntBlock.class);
                case "DOUBLE" -> Type.getInternalName(DoubleBlock.class);
                case "BOOLEAN" -> Type.getInternalName(BooleanBlock.class);
                case "BYTES_REF" -> Type.getInternalName(BytesRefBlock.class);
                default -> throw new IllegalArgumentException("Unsupported element type: " + elementType);
            };
        }

        private String getVectorTypeInternalForElementType(String elementType) {
            return switch (elementType) {
                case "LONG" -> Type.getInternalName(LongVector.class);
                case "INT" -> Type.getInternalName(IntVector.class);
                case "DOUBLE" -> Type.getInternalName(DoubleVector.class);
                case "BOOLEAN" -> Type.getInternalName(BooleanVector.class);
                case "BYTES_REF" -> Type.getInternalName(BytesRefVector.class);
                default -> throw new IllegalArgumentException("Unsupported element type: " + elementType);
            };
        }

        private String getVectorAccessorForElementType(String elementType) {
            return switch (elementType) {
                case "LONG" -> "getLong";
                case "INT" -> "getInt";
                case "DOUBLE" -> "getDouble";
                case "BOOLEAN" -> "getBoolean";
                case "BYTES_REF" -> "getBytesRef";
                default -> throw new IllegalArgumentException("Unsupported element type: " + elementType);
            };
        }

        private Class<?> getClassForElementType(String elementType) {
            return switch (elementType) {
                case "LONG" -> long.class;
                case "INT" -> int.class;
                case "DOUBLE" -> double.class;
                case "BOOLEAN" -> boolean.class;
                case "BYTES_REF" -> BytesRef.class;
                default -> throw new IllegalArgumentException("Unsupported element type: " + elementType);
            };
        }

        /**
         * Emits addIntermediateInput for custom state types (like MaxBytesRef).
         * Reference: MaxBytesRefAggregatorFunction.addIntermediateInput()
         */
        private ByteCodeAppender.Size emitCustomStateAddIntermediateInput(
            MethodVisitor mv,
            String className,
            String stateWrapperInternal,
            String declaringClassInternal,
            List<AggregatorSpec.IntermediateStateInfo> states
        ) {

            String valueStateName = states.get(0).name();
            String valueStateType = states.get(0).elementType();
            String seenStateName = states.get(1).name();

            String valueBlockType = getBlockTypeInternalForElementType(valueStateType);
            String valueVectorType = getVectorTypeInternalForElementType(valueStateType);
            boolean needsScratch = valueStateType.equals("BYTES_REF");

            // Block valueUncast = page.getBlock(channels.get(0));
            mv.visitVarInsn(Opcodes.ALOAD, 1); // page
            mv.visitVarInsn(Opcodes.ALOAD, 0); // this
            mv.visitFieldInsn(Opcodes.GETFIELD, className, "channels", Type.getDescriptor(List.class));
            mv.visitInsn(Opcodes.ICONST_0);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/List", "get", "(I)Ljava/lang/Object;", true);
            mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Integer");
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue", "()I", false);
            mv.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(Page.class),
                "getBlock",
                "(I)L" + Type.getInternalName(Block.class) + ";",
                false
            );
            mv.visitVarInsn(Opcodes.ASTORE, 2); // valueUncast

            // if (valueUncast.areAllValuesNull()) return;
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Block.class), "areAllValuesNull", "()Z", true);
            Label valueNotNull = new Label();
            mv.visitJumpInsn(Opcodes.IFEQ, valueNotNull);
            mv.visitInsn(Opcodes.RETURN);

            mv.visitLabel(valueNotNull);
            mv.visitFrame(Opcodes.F_APPEND, 1, new Object[] { Type.getInternalName(Block.class) }, 0, null);

            // XxxVector value = ((XxxBlock) valueUncast).asVector();
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitTypeInsn(Opcodes.CHECKCAST, valueBlockType);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, valueBlockType, "asVector", "()L" + valueVectorType + ";", true);
            mv.visitVarInsn(Opcodes.ASTORE, 3); // value vector

            // Block seenUncast = page.getBlock(channels.get(1));
            mv.visitVarInsn(Opcodes.ALOAD, 1); // page
            mv.visitVarInsn(Opcodes.ALOAD, 0); // this
            mv.visitFieldInsn(Opcodes.GETFIELD, className, "channels", Type.getDescriptor(List.class));
            mv.visitInsn(Opcodes.ICONST_1);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/List", "get", "(I)Ljava/lang/Object;", true);
            mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Integer");
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue", "()I", false);
            mv.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(Page.class),
                "getBlock",
                "(I)L" + Type.getInternalName(Block.class) + ";",
                false
            );
            mv.visitVarInsn(Opcodes.ASTORE, 4); // seenUncast

            // if (seenUncast.areAllValuesNull()) return;
            mv.visitVarInsn(Opcodes.ALOAD, 4);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(Block.class), "areAllValuesNull", "()Z", true);
            Label seenNotNull = new Label();
            mv.visitJumpInsn(Opcodes.IFEQ, seenNotNull);
            mv.visitInsn(Opcodes.RETURN);

            mv.visitLabel(seenNotNull);
            mv.visitFrame(Opcodes.F_APPEND, 2, new Object[] { valueVectorType, Type.getInternalName(Block.class) }, 0, null);

            // BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
            mv.visitVarInsn(Opcodes.ALOAD, 4);
            mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(BooleanBlock.class));
            mv.visitMethodInsn(
                Opcodes.INVOKEINTERFACE,
                Type.getInternalName(BooleanBlock.class),
                "asVector",
                "()L" + Type.getInternalName(BooleanVector.class) + ";",
                true
            );
            mv.visitVarInsn(Opcodes.ASTORE, 5); // seen

            int nextSlot = 6;
            int scratchSlot = -1;
            if (needsScratch) {
                // BytesRef scratch = new BytesRef();
                scratchSlot = nextSlot++;
                mv.visitTypeInsn(Opcodes.NEW, Type.getInternalName(BytesRef.class));
                mv.visitInsn(Opcodes.DUP);
                mv.visitMethodInsn(Opcodes.INVOKESPECIAL, Type.getInternalName(BytesRef.class), "<init>", "()V", false);
                mv.visitVarInsn(Opcodes.ASTORE, scratchSlot);
            }

            // DeclaringClass.combineIntermediate(state, value.getXxx(0, scratch?), seen.getBoolean(0))
            mv.visitVarInsn(Opcodes.ALOAD, 0); // this
            mv.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");

            // Get value from vector
            mv.visitVarInsn(Opcodes.ALOAD, 3); // value vector
            mv.visitInsn(Opcodes.ICONST_0);
            if (needsScratch) {
                mv.visitVarInsn(Opcodes.ALOAD, scratchSlot);
                mv.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    valueVectorType,
                    "getBytesRef",
                    "(I" + Type.getDescriptor(BytesRef.class) + ")" + Type.getDescriptor(BytesRef.class),
                    true
                );
            } else {
                String vectorAccessor = getVectorAccessorForElementType(valueStateType);
                Class<?> valueClass = getClassForElementType(valueStateType);
                mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, valueVectorType, vectorAccessor, "(I)" + Type.getDescriptor(valueClass), true);
            }

            // Get seen boolean
            mv.visitVarInsn(Opcodes.ALOAD, 5); // seen
            mv.visitInsn(Opcodes.ICONST_0);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, Type.getInternalName(BooleanVector.class), "getBoolean", "(I)Z", true);

            // Call combineIntermediate(state, value, seen)
            Class<?> valueClass = getClassForElementType(valueStateType);
            String combineIntermediateDesc = "(L" + stateWrapperInternal + ";" + Type.getDescriptor(valueClass) + "Z)V";
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "combineIntermediate", combineIntermediateDesc, false);

            mv.visitInsn(Opcodes.RETURN);
            return new ByteCodeAppender.Size(6, nextSlot);
        }
    }

    /**
     * Implements evaluateIntermediate method.
     * Reference: {@code AggregatorImplementer#evaluateIntermediate()}
     */
    private static class EvaluateIntermediateImplementation implements Implementation {
        private final AggregatorSpec spec;

        EvaluateIntermediateImplementation(AggregatorSpec spec) {
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
                String stateWrapperInternal = spec.stateWrapperClassName().replace('.', '/');

                // state.toIntermediate(blocks, offset, driverContext)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // blocks
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 2); // offset
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // driverContext
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    stateWrapperInternal,
                    "toIntermediate",
                    "([L" + Type.getInternalName(Block.class) + ";I" + Type.getDescriptor(DriverContext.class) + ")V",
                    false
                );

                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(4, 4);
            };
        }
    }

    /**
     * Implements evaluateFinal method.
     * Reference: {@code AggregatorImplementer#evaluateFinal()}
     *
     * For primitive state: checks seen(), creates constant block with value
     * For custom state: calls DeclaringClass.evaluateFinal(state, driverContext)
     */
    private static class EvaluateFinalImplementation implements Implementation {
        private final AggregatorSpec spec;

        EvaluateFinalImplementation(AggregatorSpec spec) {
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
                String stateWrapperInternal = spec.stateWrapperClassName().replace('.', '/');

                if (spec.isCustomState()) {
                    // Custom state: blocks[offset] = DeclaringClass.evaluateFinal(state, driverContext)
                    return emitCustomStateEvaluateFinal(methodVisitor, className, stateWrapperInternal);
                } else {
                    // Primitive state: check seen(), create constant block
                    return emitPrimitiveStateEvaluateFinal(methodVisitor, className, stateWrapperInternal);
                }
            };
        }

        private ByteCodeAppender.Size emitCustomStateEvaluateFinal(MethodVisitor mv, String className, String stateWrapperInternal) {
            String declaringClassInternal = spec.declaringClassInternalName();

            // blocks[offset] = DeclaringClass.evaluateFinal(state, driverContext)
            mv.visitVarInsn(Opcodes.ALOAD, 1); // blocks
            mv.visitVarInsn(Opcodes.ILOAD, 2); // offset

            // DeclaringClass.evaluateFinal(state, driverContext)
            mv.visitVarInsn(Opcodes.ALOAD, 0); // this
            mv.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
            mv.visitVarInsn(Opcodes.ALOAD, 3); // driverContext

            // Find evaluateFinal method signature
            String evalFinalDesc = "(L"
                + stateWrapperInternal
                + ";"
                + Type.getDescriptor(DriverContext.class)
                + ")L"
                + Type.getInternalName(Block.class)
                + ";";
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "evaluateFinal", evalFinalDesc, false);

            mv.visitInsn(Opcodes.AASTORE);
            mv.visitInsn(Opcodes.RETURN);
            return new ByteCodeAppender.Size(5, 4);
        }

        private ByteCodeAppender.Size emitPrimitiveStateEvaluateFinal(MethodVisitor mv, String className, String stateWrapperInternal) {
            Class<?> stateType = spec.stateType();

            // if (state.seen() == false) { blocks[offset] = driverContext.blockFactory().newConstantNullBlock(1); return; }
            mv.visitVarInsn(Opcodes.ALOAD, 0); // this
            mv.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, stateWrapperInternal, "seen", "()Z", false);
            Label seenTrue = new Label();
            mv.visitJumpInsn(Opcodes.IFNE, seenTrue);

            // blocks[offset] = driverContext.blockFactory().newConstantNullBlock(1)
            mv.visitVarInsn(Opcodes.ALOAD, 1); // blocks
            mv.visitVarInsn(Opcodes.ILOAD, 2); // offset
            mv.visitVarInsn(Opcodes.ALOAD, 3); // driverContext
            mv.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(DriverContext.class),
                "blockFactory",
                "()Lorg/elasticsearch/compute/data/BlockFactory;",
                false
            );
            mv.visitInsn(Opcodes.ICONST_1);
            mv.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                "org/elasticsearch/compute/data/BlockFactory",
                "newConstantNullBlock",
                "(I)L" + Type.getInternalName(Block.class) + ";",
                false
            );
            mv.visitInsn(Opcodes.AASTORE);
            mv.visitInsn(Opcodes.RETURN);

            mv.visitLabel(seenTrue);
            mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);

            // blocks[offset] = driverContext.blockFactory().newConstantXxxBlockWith(state.xxxValue(), 1)
            mv.visitVarInsn(Opcodes.ALOAD, 1); // blocks
            mv.visitVarInsn(Opcodes.ILOAD, 2); // offset
            mv.visitVarInsn(Opcodes.ALOAD, 3); // driverContext
            mv.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(DriverContext.class),
                "blockFactory",
                "()Lorg/elasticsearch/compute/data/BlockFactory;",
                false
            );

            mv.visitVarInsn(Opcodes.ALOAD, 0); // this
            mv.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
            String stateValueMethod = getStateValueMethod(stateType);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, stateWrapperInternal, stateValueMethod, "()" + Type.getDescriptor(stateType), false);
            mv.visitInsn(Opcodes.ICONST_1);

            String constantBlockMethod = getConstantBlockMethod(stateType);
            String constantBlockDesc = "(" + Type.getDescriptor(stateType) + "I)L" + getBlockTypeInternal(stateType) + ";";
            mv.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                "org/elasticsearch/compute/data/BlockFactory",
                constantBlockMethod,
                constantBlockDesc,
                false
            );
            mv.visitInsn(Opcodes.AASTORE);

            mv.visitInsn(Opcodes.RETURN);
            return new ByteCodeAppender.Size(6, 4);
        }

        private String getConstantBlockMethod(Class<?> type) {
            if (type == long.class) return "newConstantLongBlockWith";
            if (type == int.class) return "newConstantIntBlockWith";
            if (type == double.class) return "newConstantDoubleBlockWith";
            if (type == boolean.class) return "newConstantBooleanBlockWith";
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    /**
     * Implements toString() method.
     * Reference: {@code AggregatorImplementer#toStringMethod()}
     */
    private static class AggregatorToStringImplementation implements Implementation {
        private final AggregatorSpec spec;

        AggregatorToStringImplementation(AggregatorSpec spec) {
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

                // StringBuilder sb = new StringBuilder();
                methodVisitor.visitTypeInsn(Opcodes.NEW, "java/lang/StringBuilder");
                methodVisitor.visitInsn(Opcodes.DUP);
                methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/StringBuilder", "<init>", "()V", false);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 1);

                // sb.append(getClass().getSimpleName()).append("[")
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Object", "getClass", "()Ljava/lang/Class;", false);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Class", "getSimpleName", "()Ljava/lang/String;", false);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "java/lang/StringBuilder",
                    "append",
                    "(Ljava/lang/String;)Ljava/lang/StringBuilder;",
                    false
                );
                methodVisitor.visitLdcInsn("[");
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "java/lang/StringBuilder",
                    "append",
                    "(Ljava/lang/String;)Ljava/lang/StringBuilder;",
                    false
                );
                methodVisitor.visitInsn(Opcodes.POP);

                // sb.append("channels=").append(channels)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1);
                methodVisitor.visitLdcInsn("channels=");
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "java/lang/StringBuilder",
                    "append",
                    "(Ljava/lang/String;)Ljava/lang/StringBuilder;",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "channels", Type.getDescriptor(List.class));
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "java/lang/StringBuilder",
                    "append",
                    "(Ljava/lang/Object;)Ljava/lang/StringBuilder;",
                    false
                );
                methodVisitor.visitInsn(Opcodes.POP);

                // sb.append("]")
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1);
                methodVisitor.visitLdcInsn("]");
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "java/lang/StringBuilder",
                    "append",
                    "(Ljava/lang/String;)Ljava/lang/StringBuilder;",
                    false
                );
                methodVisitor.visitInsn(Opcodes.POP);

                // return sb.toString()
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/StringBuilder", "toString", "()Ljava/lang/String;", false);
                methodVisitor.visitInsn(Opcodes.ARETURN);

                return new ByteCodeAppender.Size(3, 2);
            };
        }
    }

    /**
     * Implements close() method.
     * Reference: {@code AggregatorImplementer#close()}
     */
    private static class AggregatorCloseImplementation implements Implementation {
        private final AggregatorSpec spec;

        AggregatorCloseImplementation(AggregatorSpec spec) {
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
                String stateWrapperInternal = spec.stateWrapperClassName().replace('.', '/');

                // state.close()
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, stateWrapperInternal, "close", "()V", false);

                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(1, 1);
            };
        }
    }

    /**
     * Implements close() method for grouping aggregators.
     * Uses arrayStateWrapperClassName instead of stateWrapperClassName.
     */
    private static class GroupingAggregatorCloseImplementation implements Implementation {
        private final AggregatorSpec spec;

        GroupingAggregatorCloseImplementation(AggregatorSpec spec) {
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
                String stateWrapperInternal = spec.arrayStateWrapperClassName().replace('.', '/');

                // state.close()
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, stateWrapperInternal, "close", "()V", false);

                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(1, 1);
            };
        }
    }

    // ============================================================================================
    // ADD INPUT IMPLEMENTATION CLASSES
    // ============================================================================================

    /**
     * Implements constructor for BlockAddInput: stores aggregator and block references.
     */
    private static class BlockAddInputConstructorImplementation implements Implementation {
        private final Class<?> aggregatorClass;
        private final Class<?> blockType;

        BlockAddInputConstructorImplementation(Class<?> aggregatorClass, Class<?> blockType) {
            this.aggregatorClass = aggregatorClass;
            this.blockType = blockType;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                String aggregatorInternal = aggregatorClass.getName().replace('.', '/');
                String blockDescriptor = Type.getDescriptor(blockType);

                // super()
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);

                // this.aggregator = aggregator
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1);
                methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "aggregator", "L" + aggregatorInternal + ";");

                // this.block = block
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2);
                methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "block", blockDescriptor);

                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(2, 3);
            };
        }
    }

    /**
     * Implements constructor for VectorAddInput: stores aggregator and vector references.
     */
    private static class VectorAddInputConstructorImplementation implements Implementation {
        private final Class<?> aggregatorClass;
        private final Class<?> vectorType;

        VectorAddInputConstructorImplementation(Class<?> aggregatorClass, Class<?> vectorType) {
            this.aggregatorClass = aggregatorClass;
            this.vectorType = vectorType;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                String aggregatorInternal = aggregatorClass.getName().replace('.', '/');
                String vectorDescriptor = Type.getDescriptor(vectorType);

                // super()
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);

                // this.aggregator = aggregator
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1);
                methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "aggregator", "L" + aggregatorInternal + ";");

                // this.vector = vector
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2);
                methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "vector", vectorDescriptor);

                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(2, 3);
            };
        }
    }

    /**
     * Implements add() method for BlockAddInput: delegates to aggregator.addRawInput(positionOffset, groupIds, block).
     */
    private static class BlockAddInputAddMethodImplementation implements Implementation {
        private final AggregatorSpec spec;
        private final Class<?> aggregatorClass;
        private final Class<?> blockType;
        private final Class<?> groupIdsType;

        BlockAddInputAddMethodImplementation(AggregatorSpec spec, Class<?> aggregatorClass, Class<?> blockType, Class<?> groupIdsType) {
            this.spec = spec;
            this.aggregatorClass = aggregatorClass;
            this.blockType = blockType;
            this.groupIdsType = groupIdsType;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                String aggregatorInternal = aggregatorClass.getName().replace('.', '/');
                String blockDescriptor = Type.getDescriptor(blockType);
                String groupIdsDescriptor = Type.getDescriptor(groupIdsType);

                // aggregator.addRawInput(positionOffset, groupIds, block)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "aggregator", "L" + aggregatorInternal + ";");
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 1); // positionOffset
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groupIds
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "block", blockDescriptor);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    aggregatorInternal,
                    "addRawInput",
                    "(I" + groupIdsDescriptor + blockDescriptor + ")V",
                    false
                );

                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(4, 3);
            };
        }
    }

    /**
     * Implements add() method for VectorAddInput: delegates to aggregator.addRawInput(positionOffset, groupIds, vector).
     */
    private static class VectorAddInputAddMethodImplementation implements Implementation {
        private final AggregatorSpec spec;
        private final Class<?> aggregatorClass;
        private final Class<?> vectorType;
        private final Class<?> groupIdsType;

        VectorAddInputAddMethodImplementation(AggregatorSpec spec, Class<?> aggregatorClass, Class<?> vectorType, Class<?> groupIdsType) {
            this.spec = spec;
            this.aggregatorClass = aggregatorClass;
            this.vectorType = vectorType;
            this.groupIdsType = groupIdsType;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                String aggregatorInternal = aggregatorClass.getName().replace('.', '/');
                String vectorDescriptor = Type.getDescriptor(vectorType);
                String groupIdsDescriptor = Type.getDescriptor(groupIdsType);

                // aggregator.addRawInput(positionOffset, groupIds, vector)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "aggregator", "L" + aggregatorInternal + ";");
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 1); // positionOffset
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groupIds
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "vector", vectorDescriptor);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    aggregatorInternal,
                    "addRawInput",
                    "(I" + groupIdsDescriptor + vectorDescriptor + ")V",
                    false
                );

                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(4, 3);
            };
        }
    }

    /**
     * Implements close() method for AddInput classes: does nothing (no resources to release).
     */
    private static class AddInputCloseImplementation implements Implementation {
        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(0, 1);
            };
        }
    }

    // ============================================================================================
    // STATIC INITIALIZER AND OTHER IMPLEMENTATIONS
    // ============================================================================================

    /**
     * Implements static initializer for INTERMEDIATE_STATE_DESC.
     * Reference: {@code AggregatorImplementer#initInterState()}
     */
    private static class AggregatorStaticInitializerImplementation implements Implementation {
        private final AggregatorSpec spec;

        AggregatorStaticInitializerImplementation(AggregatorSpec spec) {
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
                List<AggregatorSpec.IntermediateStateInfo> states = spec.intermediateState();

                // Create array for List.of() arguments
                int stateCount = states.size();

                // Push all IntermediateStateDesc instances onto stack
                for (int i = 0; i < stateCount; i++) {
                    AggregatorSpec.IntermediateStateInfo state = states.get(i);

                    methodVisitor.visitTypeInsn(Opcodes.NEW, Type.getInternalName(IntermediateStateDesc.class));
                    methodVisitor.visitInsn(Opcodes.DUP);
                    methodVisitor.visitLdcInsn(state.name());
                    methodVisitor.visitFieldInsn(
                        Opcodes.GETSTATIC,
                        Type.getInternalName(ElementType.class),
                        state.elementType(),
                        Type.getDescriptor(ElementType.class)
                    );
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKESPECIAL,
                        Type.getInternalName(IntermediateStateDesc.class),
                        "<init>",
                        "(Ljava/lang/String;" + Type.getDescriptor(ElementType.class) + ")V",
                        false
                    );
                }

                // Call List.of() with the appropriate number of arguments
                String listOfDesc = switch (stateCount) {
                    case 1 -> "(Ljava/lang/Object;)Ljava/util/List;";
                    case 2 -> "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/util/List;";
                    case 3 -> "(Ljava/lang/Object;Ljava/lang/Object;Ljava/lang/Object;)Ljava/util/List;";
                    default -> throw new IllegalArgumentException("Unsupported state count: " + stateCount);
                };
                methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, "java/util/List", "of", listOfDesc, true);

                methodVisitor.visitFieldInsn(Opcodes.PUTSTATIC, className, "INTERMEDIATE_STATE_DESC", Type.getDescriptor(List.class));

                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(stateCount * 4, 0);
            };
        }
    }

    // ============================================================================================
    // GROUPING AGGREGATOR IMPLEMENTATIONS (Stubs - to be fully implemented)
    // ============================================================================================

    private static class GroupingAggregatorConstructorImplementation implements Implementation {
        private final AggregatorSpec spec;
        private final Class<?> stateClass;

        GroupingAggregatorConstructorImplementation(AggregatorSpec spec, Class<?> stateClass) {
            this.spec = spec;
            this.stateClass = stateClass;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();

                // super()
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);

                // this.channels = channels
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1);
                methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "channels", Type.getDescriptor(List.class));

                // this.state = state
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2);
                methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "state", Type.getDescriptor(stateClass));

                // this.driverContext = driverContext
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3);
                methodVisitor.visitFieldInsn(Opcodes.PUTFIELD, className, "driverContext", Type.getDescriptor(DriverContext.class));

                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(2, 4);
            };
        }
    }

    private static class GroupingAggregatorCreateMethodImplementation implements Implementation {
        private final AggregatorSpec spec;
        private final Class<?> stateClass;

        GroupingAggregatorCreateMethodImplementation(AggregatorSpec spec, Class<?> stateClass) {
            this.spec = spec;
            this.stateClass = stateClass;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                String declaringClassInternal = spec.declaringClassInternalName();

                if (spec.hasCustomGroupingState()) {
                    // Custom grouping state: call initGrouping(driverContext)
                    return emitCustomGroupingStateCreate(methodVisitor, className, declaringClassInternal);
                } else {
                    // Primitive state: new ArrayState(driverContext.bigArrays(), DeclaringClass.init())
                    return emitPrimitiveStateCreate(methodVisitor, className, declaringClassInternal);
                }
            };
        }

        private ByteCodeAppender.Size emitCustomGroupingStateCreate(MethodVisitor mv, String className, String declaringClassInternal) {
            // new GroupingAggregatorFunction(channels, DeclaringClass.initGrouping(driverContext), driverContext)
            mv.visitTypeInsn(Opcodes.NEW, className);
            mv.visitInsn(Opcodes.DUP);

            // channels
            mv.visitVarInsn(Opcodes.ALOAD, 0);

            // DeclaringClass.initGrouping(driverContext)
            mv.visitVarInsn(Opcodes.ALOAD, 1); // driverContext
            java.lang.reflect.Method initGroupingMethod = spec.initGroupingMethod();
            String initDesc = Type.getMethodDescriptor(initGroupingMethod);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "initGrouping", initDesc, false);

            // driverContext
            mv.visitVarInsn(Opcodes.ALOAD, 1);

            // Call constructor
            String ctorDesc = "("
                + Type.getDescriptor(List.class)
                + Type.getDescriptor(stateClass)
                + Type.getDescriptor(DriverContext.class)
                + ")V";
            mv.visitMethodInsn(Opcodes.INVOKESPECIAL, className, "<init>", ctorDesc, false);

            mv.visitInsn(Opcodes.ARETURN);
            return new ByteCodeAppender.Size(5, 2);
        }

        private ByteCodeAppender.Size emitPrimitiveStateCreate(MethodVisitor mv, String className, String declaringClassInternal) {
            // new GroupingAggregatorFunction(channels, new ArrayState(driverContext.bigArrays(), DeclaringClass.init()), driverContext)
            mv.visitTypeInsn(Opcodes.NEW, className);
            mv.visitInsn(Opcodes.DUP);

            // channels
            mv.visitVarInsn(Opcodes.ALOAD, 0);

            // new ArrayState(driverContext.bigArrays(), DeclaringClass.init())
            mv.visitTypeInsn(Opcodes.NEW, stateClass.getName().replace('.', '/'));
            mv.visitInsn(Opcodes.DUP);

            // driverContext.bigArrays()
            mv.visitVarInsn(Opcodes.ALOAD, 1); // driverContext
            mv.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                Type.getInternalName(DriverContext.class),
                "bigArrays",
                "()Lorg/elasticsearch/common/util/BigArrays;",
                false
            );

            // DeclaringClass.init()
            String initDesc = Type.getMethodDescriptor(spec.initMethod());
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, spec.initMethod().getName(), initDesc, false);

            // Call state constructor
            String stateCtorDesc = "(Lorg/elasticsearch/common/util/BigArrays;" + Type.getDescriptor(spec.stateType()) + ")V";
            mv.visitMethodInsn(Opcodes.INVOKESPECIAL, stateClass.getName().replace('.', '/'), "<init>", stateCtorDesc, false);

            // driverContext
            mv.visitVarInsn(Opcodes.ALOAD, 1);

            // Call constructor
            String ctorDesc = "("
                + Type.getDescriptor(List.class)
                + Type.getDescriptor(stateClass)
                + Type.getDescriptor(DriverContext.class)
                + ")V";
            mv.visitMethodInsn(Opcodes.INVOKESPECIAL, className, "<init>", ctorDesc, false);

            mv.visitInsn(Opcodes.ARETURN);
            // Stack analysis for primitive long state:
            // NEW, DUP (2), ALOAD (3), NEW, DUP (5), ALOAD (6), INVOKEVIRTUAL (6), INVOKESTATIC returns long (8)
            // Max stack = 8 for long/double state types, 7 for int/boolean
            int stackSize = (spec.stateType() == long.class || spec.stateType() == double.class) ? 8 : 7;
            return new ByteCodeAppender.Size(stackSize, 2);
        }
    }

    /**
     * Implements prepareProcessRawInputPage for grouping aggregators.
     * Uses the static constructor fields to create AddInput instances for optimal performance.
     * The constructors are set by AggregatorCache after class definition.
     */
    private static class PrepareProcessRawInputPageImplementation implements Implementation {
        private final AggregatorSpec spec;

        PrepareProcessRawInputPageImplementation(AggregatorSpec spec) {
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
                Class<?> valueType = spec.valueType();
                String blockType = getBlockTypeInternal(valueType);
                String vectorType = getVectorTypeInternal(valueType);

                // LongBlock vBlock = page.getBlock(channels.get(0));
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // page
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "channels", "Ljava/util/List;");
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/List", "get", "(I)Ljava/lang/Object;", true);
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue", "()I", false);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/data/Page",
                    "getBlock",
                    "(I)Lorg/elasticsearch/compute/data/Block;",
                    false
                );
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, blockType);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 3); // vBlock

                // LongVector vVector = vBlock.asVector();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // vBlock
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "asVector", "()L" + vectorType + ";", true);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 4); // vVector

                // if (vVector == null) { ... return BlockAddInput ... } else { ... return VectorAddInput ... }
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // vVector
                Label vectorNotNull = new Label();
                methodVisitor.visitJumpInsn(Opcodes.IFNONNULL, vectorNotNull);

                // vVector is null - use block path
                // maybeEnableGroupIdTracking(seenGroupIds, vBlock);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // seenGroupIds
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // vBlock
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    className,
                    "maybeEnableGroupIdTracking",
                    "(Lorg/elasticsearch/compute/aggregation/SeenGroupIds;L" + blockType + ";)V",
                    false
                );

                // return BLOCK_ADD_INPUT_CTOR.newInstance(this, vBlock);
                methodVisitor.visitFieldInsn(Opcodes.GETSTATIC, className, "BLOCK_ADD_INPUT_CTOR", "Ljava/lang/reflect/Constructor;");
                methodVisitor.visitInsn(Opcodes.ICONST_2);
                methodVisitor.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/Object");
                methodVisitor.visitInsn(Opcodes.DUP);
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitInsn(Opcodes.AASTORE);
                methodVisitor.visitInsn(Opcodes.DUP);
                methodVisitor.visitInsn(Opcodes.ICONST_1);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // vBlock
                methodVisitor.visitInsn(Opcodes.AASTORE);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "java/lang/reflect/Constructor",
                    "newInstance",
                    "([Ljava/lang/Object;)Ljava/lang/Object;",
                    false
                );
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, "org/elasticsearch/compute/aggregation/GroupingAggregatorFunction$AddInput");
                methodVisitor.visitInsn(Opcodes.ARETURN);

                // vVector is not null - use vector path
                methodVisitor.visitLabel(vectorNotNull);

                // return VECTOR_ADD_INPUT_CTOR.newInstance(this, vVector);
                methodVisitor.visitFieldInsn(Opcodes.GETSTATIC, className, "VECTOR_ADD_INPUT_CTOR", "Ljava/lang/reflect/Constructor;");
                methodVisitor.visitInsn(Opcodes.ICONST_2);
                methodVisitor.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/Object");
                methodVisitor.visitInsn(Opcodes.DUP);
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitInsn(Opcodes.AASTORE);
                methodVisitor.visitInsn(Opcodes.DUP);
                methodVisitor.visitInsn(Opcodes.ICONST_1);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // vVector
                methodVisitor.visitInsn(Opcodes.AASTORE);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "java/lang/reflect/Constructor",
                    "newInstance",
                    "([Ljava/lang/Object;)Ljava/lang/Object;",
                    false
                );
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, "org/elasticsearch/compute/aggregation/GroupingAggregatorFunction$AddInput");
                methodVisitor.visitInsn(Opcodes.ARETURN);

                return new ByteCodeAppender.Size(6, 5);
            };
        }
    }

    private static class GroupingAddRawInputBlockImplementation implements Implementation {
        private final AggregatorSpec spec;
        private final Class<?> groupsType;

        GroupingAddRawInputBlockImplementation(AggregatorSpec spec, Class<?> groupsType) {
            this.spec = spec;
            this.groupsType = groupsType;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                String stateWrapperInternal = spec.arrayStateWrapperClassName().replace('.', '/');
                String declaringClassInternal = spec.declaringClassInternalName();
                Class<?> valueType = spec.valueType();
                Class<?> stateType = spec.stateType();
                String blockType = getBlockTypeInternal(valueType);
                String groupsTypeInternal = groupsType.getName().replace('.', '/');
                boolean groupsIsBlock = groupsType.getSimpleName().endsWith("Block");
                boolean needsScratch = requiresScratch(valueType);

                // For BytesRef: BytesRef scratch = new BytesRef();
                int scratchSlot = -1;
                if (needsScratch) {
                    scratchSlot = 4;
                    methodVisitor.visitTypeInsn(Opcodes.NEW, Type.getInternalName(BytesRef.class));
                    methodVisitor.visitInsn(Opcodes.DUP);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, Type.getInternalName(BytesRef.class), "<init>", "()V", false);
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, scratchSlot);
                }

                // for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++)
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                int groupPositionSlot = needsScratch ? 5 : 4;
                methodVisitor.visitVarInsn(Opcodes.ISTORE, groupPositionSlot); // groupPosition

                Label loopStart = new Label();
                Label loopEnd = new Label();

                methodVisitor.visitLabel(loopStart);
                if (needsScratch) {
                    methodVisitor.visitFrame(
                        Opcodes.F_APPEND,
                        2,
                        new Object[] { Type.getInternalName(BytesRef.class), Opcodes.INTEGER },
                        0,
                        null
                    );
                } else {
                    methodVisitor.visitFrame(Opcodes.F_APPEND, 1, new Object[] { Opcodes.INTEGER }, 0, null);
                }

                // groupPosition < groups.getPositionCount()
                methodVisitor.visitVarInsn(Opcodes.ILOAD, groupPositionSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "getPositionCount", "()I", true);
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, loopEnd);

                Label continueLabel = new Label();

                if (groupsIsBlock) {
                    // if (groups.isNull(groupPosition)) continue;
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, groupPositionSlot); // groupPosition
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "isNull", "(I)Z", true);
                    methodVisitor.visitJumpInsn(Opcodes.IFNE, continueLabel);
                }

                // int valuesPosition = groupPosition + positionOffset;
                methodVisitor.visitVarInsn(Opcodes.ILOAD, groupPositionSlot); // groupPosition
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 1); // positionOffset
                methodVisitor.visitInsn(Opcodes.IADD);
                int valuesPositionSlot = groupPositionSlot + 1;
                methodVisitor.visitVarInsn(Opcodes.ISTORE, valuesPositionSlot); // valuesPosition

                // if (vBlock.isNull(valuesPosition)) continue;
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // vBlock
                methodVisitor.visitVarInsn(Opcodes.ILOAD, valuesPositionSlot); // valuesPosition
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "isNull", "(I)Z", true);
                methodVisitor.visitJumpInsn(Opcodes.IFNE, continueLabel);

                if (groupsIsBlock) {
                    // int groupStart = groups.getFirstValueIndex(groupPosition);
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, groupPositionSlot); // groupPosition
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "getFirstValueIndex", "(I)I", true);
                    int groupStartSlot = valuesPositionSlot + 1;
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, groupStartSlot); // groupStart

                    // int groupEnd = groupStart + groups.getValueCount(groupPosition);
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, groupStartSlot); // groupStart
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, groupPositionSlot); // groupPosition
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "getValueCount", "(I)I", true);
                    methodVisitor.visitInsn(Opcodes.IADD);
                    int groupEndSlot = groupStartSlot + 1;
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, groupEndSlot); // groupEnd

                    // for (int g = groupStart; g < groupEnd; g++)
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, groupStartSlot); // groupStart
                    int gSlot = groupEndSlot + 1;
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, gSlot); // g

                    Label innerLoopStart = new Label();
                    Label innerLoopEnd = new Label();

                    methodVisitor.visitLabel(innerLoopStart);
                    methodVisitor.visitFrame(
                        Opcodes.F_APPEND,
                        4,
                        new Object[] { Opcodes.INTEGER, Opcodes.INTEGER, Opcodes.INTEGER, Opcodes.INTEGER },
                        0,
                        null
                    );

                    methodVisitor.visitVarInsn(Opcodes.ILOAD, gSlot); // g
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, groupEndSlot); // groupEnd
                    methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, innerLoopEnd);

                    // int groupId = groups.getInt(g);
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, gSlot); // g
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "getInt", "(I)I", true);
                    int groupIdSlot = gSlot + 1;
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, groupIdSlot); // groupId

                    // Process all values in the block for this position
                    generateCombineForGroupingBlock(
                        methodVisitor,
                        className,
                        stateWrapperInternal,
                        declaringClassInternal,
                        blockType,
                        valueType,
                        stateType,
                        valuesPositionSlot,
                        groupIdSlot,
                        groupIdSlot + 1,
                        scratchSlot
                    );

                    // g++
                    methodVisitor.visitIincInsn(gSlot, 1);
                    methodVisitor.visitJumpInsn(Opcodes.GOTO, innerLoopStart);

                    methodVisitor.visitLabel(innerLoopEnd);
                    methodVisitor.visitFrame(Opcodes.F_CHOP, 4, null, 0, null);
                } else {
                    // IntVector case - simpler, no group inner loop
                    // int groupId = groups.getInt(groupPosition);
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, groupPositionSlot); // groupPosition
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "getInt", "(I)I", true);
                    int groupIdSlot = valuesPositionSlot + 1;
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, groupIdSlot); // groupId

                    // Process all values in the block for this position
                    generateCombineForGroupingBlock(
                        methodVisitor,
                        className,
                        stateWrapperInternal,
                        declaringClassInternal,
                        blockType,
                        valueType,
                        stateType,
                        valuesPositionSlot,
                        groupIdSlot,
                        groupIdSlot + 1,
                        scratchSlot
                    );
                }

                // continue label and increment
                methodVisitor.visitLabel(continueLabel);
                methodVisitor.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
                methodVisitor.visitIincInsn(groupPositionSlot, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, loopStart);

                methodVisitor.visitLabel(loopEnd);
                if (needsScratch) {
                    methodVisitor.visitFrame(Opcodes.F_CHOP, 2, null, 0, null);
                } else {
                    methodVisitor.visitFrame(Opcodes.F_CHOP, 1, null, 0, null);
                }
                methodVisitor.visitInsn(Opcodes.RETURN);

                int maxStack = 6;
                int maxLocals = groupsIsBlock ? (needsScratch ? 16 : 14) : (needsScratch ? 13 : 11);
                return new ByteCodeAppender.Size(maxStack, maxLocals);
            };
        }

        private void generateCombineForGroupingBlock(
            MethodVisitor methodVisitor,
            String className,
            String stateWrapperInternal,
            String declaringClassInternal,
            String blockType,
            Class<?> valueType,
            Class<?> stateType,
            int valuesPositionSlot,
            int groupIdSlot,
            int startLocalSlot,
            int scratchSlot
        ) {
            boolean needsScratch = requiresScratch(valueType);

            // int vStart = vBlock.getFirstValueIndex(valuesPosition);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // vBlock
            methodVisitor.visitVarInsn(Opcodes.ILOAD, valuesPositionSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "getFirstValueIndex", "(I)I", true);
            methodVisitor.visitVarInsn(Opcodes.ISTORE, startLocalSlot); // vStart

            // int vEnd = vStart + vBlock.getValueCount(valuesPosition);
            methodVisitor.visitVarInsn(Opcodes.ILOAD, startLocalSlot); // vStart
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // vBlock
            methodVisitor.visitVarInsn(Opcodes.ILOAD, valuesPositionSlot);
            methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "getValueCount", "(I)I", true);
            methodVisitor.visitInsn(Opcodes.IADD);
            methodVisitor.visitVarInsn(Opcodes.ISTORE, startLocalSlot + 1); // vEnd

            // for (int vOffset = vStart; vOffset < vEnd; vOffset++)
            methodVisitor.visitVarInsn(Opcodes.ILOAD, startLocalSlot); // vStart
            methodVisitor.visitVarInsn(Opcodes.ISTORE, startLocalSlot + 2); // vOffset

            Label valueLoopStart = new Label();
            Label valueLoopEnd = new Label();

            methodVisitor.visitLabel(valueLoopStart);
            methodVisitor.visitFrame(Opcodes.F_APPEND, 3, new Object[] { Opcodes.INTEGER, Opcodes.INTEGER, Opcodes.INTEGER }, 0, null);

            methodVisitor.visitVarInsn(Opcodes.ILOAD, startLocalSlot + 2); // vOffset
            methodVisitor.visitVarInsn(Opcodes.ILOAD, startLocalSlot + 1); // vEnd
            methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, valueLoopEnd);

            // Get value from block
            int valueSlot = startLocalSlot + 3;
            if (needsScratch) {
                // BytesRef vValue = vBlock.getBytesRef(vOffset, scratch);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // vBlock
                methodVisitor.visitVarInsn(Opcodes.ILOAD, startLocalSlot + 2); // vOffset
                methodVisitor.visitVarInsn(Opcodes.ALOAD, scratchSlot); // scratch
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    blockType,
                    "getBytesRef",
                    "(I" + Type.getDescriptor(BytesRef.class) + ")" + Type.getDescriptor(BytesRef.class),
                    true
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, valueSlot);
            } else {
                // vValue = vBlock.getLong(vOffset)
                String valueAccessor = getBlockAccessorNameForGrouping(valueType);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // vBlock
                methodVisitor.visitVarInsn(Opcodes.ILOAD, startLocalSlot + 2); // vOffset
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    blockType,
                    valueAccessor,
                    "(I)" + Type.getDescriptor(valueType),
                    true
                );
                if (valueType == long.class || valueType == double.class) {
                    methodVisitor.visitVarInsn(valueType == long.class ? Opcodes.LSTORE : Opcodes.DSTORE, valueSlot);
                } else {
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, valueSlot);
                }
            }

            if (spec.hasCustomGroupingState()) {
                // Custom grouping state: DeclaringClass.combine(state, groupId, value) - void return
                generateCustomStateGroupingCombineForBlock(
                    methodVisitor,
                    className,
                    stateWrapperInternal,
                    declaringClassInternal,
                    valueType,
                    valueSlot,
                    groupIdSlot
                );
            } else {
                // Primitive state: state.set(groupId, DeclaringClass.combine(state.getOrDefault(groupId), vValue))
                generatePrimitiveStateGroupingCombineForBlock(
                    methodVisitor,
                    className,
                    stateWrapperInternal,
                    declaringClassInternal,
                    valueType,
                    stateType,
                    valueSlot,
                    groupIdSlot,
                    needsScratch
                );
            }

            // vOffset++
            methodVisitor.visitIincInsn(startLocalSlot + 2, 1);
            methodVisitor.visitJumpInsn(Opcodes.GOTO, valueLoopStart);

            methodVisitor.visitLabel(valueLoopEnd);
            methodVisitor.visitFrame(Opcodes.F_CHOP, 3, null, 0, null);
        }

        private void generateCustomStateGroupingCombineForBlock(
            MethodVisitor methodVisitor,
            String className,
            String stateWrapperInternal,
            String declaringClassInternal,
            Class<?> valueType,
            int valueSlot,
            int groupIdSlot
        ) {
            // DeclaringClass.combine(state, groupId, value)
            // Load state
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
            methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");

            // Load groupId
            methodVisitor.visitVarInsn(Opcodes.ILOAD, groupIdSlot);

            // Load value
            if (requiresScratch(valueType)) {
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valueSlot);
            } else if (valueType == long.class || valueType == double.class) {
                methodVisitor.visitVarInsn(valueType == long.class ? Opcodes.LLOAD : Opcodes.DLOAD, valueSlot);
            } else {
                methodVisitor.visitVarInsn(Opcodes.ILOAD, valueSlot);
            }

            // Call combine(GroupingState, int, value) - void return
            Class<?> groupingStateType = spec.groupingStateType();
            String combineDesc = "(" + Type.getDescriptor(groupingStateType) + "I" + Type.getDescriptor(valueType) + ")V";
            methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "combine", combineDesc, false);
        }

        private void generatePrimitiveStateGroupingCombineForBlock(
            MethodVisitor methodVisitor,
            String className,
            String stateWrapperInternal,
            String declaringClassInternal,
            Class<?> valueType,
            Class<?> stateType,
            int valueSlot,
            int groupIdSlot,
            boolean needsScratch
        ) {
            // state.set(groupId, DeclaringClass.combine(state.getOrDefault(groupId), vValue))
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
            methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
            methodVisitor.visitVarInsn(Opcodes.ILOAD, groupIdSlot); // groupId

            // state.getOrDefault(groupId)
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
            methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
            methodVisitor.visitVarInsn(Opcodes.ILOAD, groupIdSlot); // groupId
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                stateWrapperInternal,
                "getOrDefault",
                "(I)" + Type.getDescriptor(stateType),
                false
            );

            // Load vValue
            if (needsScratch) {
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valueSlot);
            } else if (valueType == long.class || valueType == double.class) {
                methodVisitor.visitVarInsn(valueType == long.class ? Opcodes.LLOAD : Opcodes.DLOAD, valueSlot);
            } else {
                methodVisitor.visitVarInsn(Opcodes.ILOAD, valueSlot);
            }

            // DeclaringClass.combine(state, value)
            String combineDesc = "(" + Type.getDescriptor(stateType) + Type.getDescriptor(valueType) + ")" + Type.getDescriptor(stateType);
            methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "combine", combineDesc, false);

            // state.set(groupId, result)
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                stateWrapperInternal,
                "set",
                "(I" + Type.getDescriptor(stateType) + ")V",
                false
            );
        }

        private String getBlockAccessorNameForGrouping(Class<?> type) {
            if (type == long.class) return "getLong";
            if (type == int.class) return "getInt";
            if (type == double.class) return "getDouble";
            if (type == boolean.class) return "getBoolean";
            if (type == BytesRef.class) return "getBytesRef";
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    private static class GroupingAddRawInputVectorImplementation implements Implementation {
        private final AggregatorSpec spec;
        private final Class<?> groupsType;

        GroupingAddRawInputVectorImplementation(AggregatorSpec spec, Class<?> groupsType) {
            this.spec = spec;
            this.groupsType = groupsType;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                String stateWrapperInternal = spec.arrayStateWrapperClassName().replace('.', '/');
                String declaringClassInternal = spec.declaringClassInternalName();
                Class<?> valueType = spec.valueType();
                Class<?> stateType = spec.stateType();
                String vectorType = getVectorTypeInternal(valueType);
                String groupsTypeInternal = groupsType.getName().replace('.', '/');
                boolean groupsIsBlock = groupsType.getSimpleName().endsWith("Block");
                boolean needsScratch = requiresScratch(valueType);

                // For BytesRef: BytesRef scratch = new BytesRef();
                int scratchSlot = -1;
                if (needsScratch) {
                    scratchSlot = 4;
                    methodVisitor.visitTypeInsn(Opcodes.NEW, Type.getInternalName(BytesRef.class));
                    methodVisitor.visitInsn(Opcodes.DUP);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, Type.getInternalName(BytesRef.class), "<init>", "()V", false);
                    methodVisitor.visitVarInsn(Opcodes.ASTORE, scratchSlot);
                }

                // for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++)
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                int groupPositionSlot = needsScratch ? 5 : 4;
                methodVisitor.visitVarInsn(Opcodes.ISTORE, groupPositionSlot); // groupPosition

                Label loopStart = new Label();
                Label loopEnd = new Label();

                methodVisitor.visitLabel(loopStart);
                if (needsScratch) {
                    methodVisitor.visitFrame(
                        Opcodes.F_APPEND,
                        2,
                        new Object[] { Type.getInternalName(BytesRef.class), Opcodes.INTEGER },
                        0,
                        null
                    );
                } else {
                    methodVisitor.visitFrame(Opcodes.F_APPEND, 1, new Object[] { Opcodes.INTEGER }, 0, null);
                }

                // groupPosition < groups.getPositionCount()
                methodVisitor.visitVarInsn(Opcodes.ILOAD, groupPositionSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "getPositionCount", "()I", true);
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, loopEnd);

                if (groupsIsBlock) {
                    // if (groups.isNull(groupPosition)) continue;
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, groupPositionSlot); // groupPosition
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "isNull", "(I)Z", true);
                    Label notNull = new Label();
                    methodVisitor.visitJumpInsn(Opcodes.IFEQ, notNull);
                    // continue - increment and jump to loop start
                    methodVisitor.visitIincInsn(groupPositionSlot, 1);
                    methodVisitor.visitJumpInsn(Opcodes.GOTO, loopStart);
                    methodVisitor.visitLabel(notNull);
                    methodVisitor.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
                }

                // int valuesPosition = groupPosition + positionOffset;
                methodVisitor.visitVarInsn(Opcodes.ILOAD, groupPositionSlot); // groupPosition
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 1); // positionOffset
                methodVisitor.visitInsn(Opcodes.IADD);
                int valuesPositionSlot = groupPositionSlot + 1;
                methodVisitor.visitVarInsn(Opcodes.ISTORE, valuesPositionSlot); // valuesPosition

                if (groupsIsBlock) {
                    // int groupStart = groups.getFirstValueIndex(groupPosition);
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, groupPositionSlot); // groupPosition
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "getFirstValueIndex", "(I)I", true);
                    int groupStartSlot = valuesPositionSlot + 1;
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, groupStartSlot); // groupStart

                    // int groupEnd = groupStart + groups.getValueCount(groupPosition);
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, groupStartSlot); // groupStart
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, groupPositionSlot); // groupPosition
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "getValueCount", "(I)I", true);
                    methodVisitor.visitInsn(Opcodes.IADD);
                    int groupEndSlot = groupStartSlot + 1;
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, groupEndSlot); // groupEnd

                    // for (int g = groupStart; g < groupEnd; g++)
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, groupStartSlot); // groupStart
                    int gSlot = groupEndSlot + 1;
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, gSlot); // g

                    Label innerLoopStart = new Label();
                    Label innerLoopEnd = new Label();

                    methodVisitor.visitLabel(innerLoopStart);
                    methodVisitor.visitFrame(
                        Opcodes.F_APPEND,
                        4,
                        new Object[] { Opcodes.INTEGER, Opcodes.INTEGER, Opcodes.INTEGER, Opcodes.INTEGER },
                        0,
                        null
                    );

                    methodVisitor.visitVarInsn(Opcodes.ILOAD, gSlot); // g
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, groupEndSlot); // groupEnd
                    methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, innerLoopEnd);

                    // int groupId = groups.getInt(g);
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, gSlot); // g
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "getInt", "(I)I", true);
                    int groupIdSlot = gSlot + 1;
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, groupIdSlot); // groupId

                    // Get value from vector and combine
                    generateCombineForGroupingVector(
                        methodVisitor,
                        className,
                        stateWrapperInternal,
                        declaringClassInternal,
                        vectorType,
                        valueType,
                        stateType,
                        valuesPositionSlot,
                        groupIdSlot,
                        scratchSlot
                    );

                    // g++
                    methodVisitor.visitIincInsn(gSlot, 1);
                    methodVisitor.visitJumpInsn(Opcodes.GOTO, innerLoopStart);

                    methodVisitor.visitLabel(innerLoopEnd);
                    methodVisitor.visitFrame(Opcodes.F_CHOP, 4, null, 0, null);
                } else {
                    // IntVector case - simpler, no inner loop
                    // int groupId = groups.getInt(groupPosition);
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, groupPositionSlot); // groupPosition
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "getInt", "(I)I", true);
                    int groupIdSlot = valuesPositionSlot + 1;
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, groupIdSlot); // groupId

                    // Get value from vector and combine
                    generateCombineForGroupingVector(
                        methodVisitor,
                        className,
                        stateWrapperInternal,
                        declaringClassInternal,
                        vectorType,
                        valueType,
                        stateType,
                        valuesPositionSlot,
                        groupIdSlot,
                        scratchSlot
                    );
                }

                // groupPosition++
                methodVisitor.visitIincInsn(groupPositionSlot, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, loopStart);

                methodVisitor.visitLabel(loopEnd);
                if (needsScratch) {
                    methodVisitor.visitFrame(Opcodes.F_CHOP, 2, null, 0, null);
                } else {
                    methodVisitor.visitFrame(Opcodes.F_CHOP, 1, null, 0, null);
                }
                methodVisitor.visitInsn(Opcodes.RETURN);

                int maxStack = groupsIsBlock ? 6 : 5;
                int maxLocals = groupsIsBlock ? (needsScratch ? 12 : 10) : (needsScratch ? 9 : 7);
                return new ByteCodeAppender.Size(maxStack, maxLocals);
            };
        }

        private void generateCombineForGroupingVector(
            MethodVisitor methodVisitor,
            String className,
            String stateWrapperInternal,
            String declaringClassInternal,
            String vectorType,
            Class<?> valueType,
            Class<?> stateType,
            int valuesPositionSlot,
            int groupIdSlot,
            int scratchSlot
        ) {
            boolean needsScratch = requiresScratch(valueType);
            int valueSlot = groupIdSlot + 1;

            if (needsScratch) {
                // BytesRef vValue = vVector.getBytesRef(valuesPosition, scratch);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // vVector
                methodVisitor.visitVarInsn(Opcodes.ILOAD, valuesPositionSlot);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, scratchSlot); // scratch
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    vectorType,
                    "getBytesRef",
                    "(I" + Type.getDescriptor(BytesRef.class) + ")" + Type.getDescriptor(BytesRef.class),
                    true
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, valueSlot);
            } else {
                String valueAccessor = getVectorAccessorNameForGrouping(valueType);
                // vValue = vVector.getLong(valuesPosition)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // vVector
                methodVisitor.visitVarInsn(Opcodes.ILOAD, valuesPositionSlot);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    vectorType,
                    valueAccessor,
                    "(I)" + Type.getDescriptor(valueType),
                    true
                );
                // Store value in appropriate slot
                if (valueType == long.class || valueType == double.class) {
                    methodVisitor.visitVarInsn(valueType == long.class ? Opcodes.LSTORE : Opcodes.DSTORE, valueSlot);
                } else {
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, valueSlot);
                }
            }

            if (spec.hasCustomGroupingState()) {
                // Custom grouping state: DeclaringClass.combine(state, groupId, value) - void return
                generateCustomStateGroupingCombine(
                    methodVisitor,
                    className,
                    stateWrapperInternal,
                    declaringClassInternal,
                    valueType,
                    valueSlot,
                    groupIdSlot
                );
            } else {
                // Primitive state: state.set(groupId, DeclaringClass.combine(state.getOrDefault(groupId), vValue))
                generatePrimitiveStateGroupingCombine(
                    methodVisitor,
                    className,
                    stateWrapperInternal,
                    declaringClassInternal,
                    valueType,
                    stateType,
                    valueSlot,
                    groupIdSlot,
                    needsScratch
                );
            }
        }

        private void generateCustomStateGroupingCombine(
            MethodVisitor methodVisitor,
            String className,
            String stateWrapperInternal,
            String declaringClassInternal,
            Class<?> valueType,
            int valueSlot,
            int groupIdSlot
        ) {
            // DeclaringClass.combine(state, groupId, value)
            // Load state
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
            methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");

            // Load groupId
            methodVisitor.visitVarInsn(Opcodes.ILOAD, groupIdSlot);

            // Load value
            if (requiresScratch(valueType)) {
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valueSlot);
            } else if (valueType == long.class || valueType == double.class) {
                methodVisitor.visitVarInsn(valueType == long.class ? Opcodes.LLOAD : Opcodes.DLOAD, valueSlot);
            } else {
                methodVisitor.visitVarInsn(Opcodes.ILOAD, valueSlot);
            }

            // Call combine(GroupingState, int, value) - void return
            Class<?> groupingStateType = spec.groupingStateType();
            String combineDesc = "(" + Type.getDescriptor(groupingStateType) + "I" + Type.getDescriptor(valueType) + ")V";
            methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "combine", combineDesc, false);
        }

        private void generatePrimitiveStateGroupingCombine(
            MethodVisitor methodVisitor,
            String className,
            String stateWrapperInternal,
            String declaringClassInternal,
            Class<?> valueType,
            Class<?> stateType,
            int valueSlot,
            int groupIdSlot,
            boolean needsScratch
        ) {
            // state.set(groupId, DeclaringClass.combine(state.getOrDefault(groupId), vValue))
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
            methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
            methodVisitor.visitVarInsn(Opcodes.ILOAD, groupIdSlot); // groupId

            // state.getOrDefault(groupId)
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
            methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
            methodVisitor.visitVarInsn(Opcodes.ILOAD, groupIdSlot); // groupId
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                stateWrapperInternal,
                "getOrDefault",
                "(I)" + Type.getDescriptor(stateType),
                false
            );

            // Load vValue
            if (needsScratch) {
                methodVisitor.visitVarInsn(Opcodes.ALOAD, valueSlot);
            } else if (valueType == long.class || valueType == double.class) {
                methodVisitor.visitVarInsn(valueType == long.class ? Opcodes.LLOAD : Opcodes.DLOAD, valueSlot);
            } else {
                methodVisitor.visitVarInsn(Opcodes.ILOAD, valueSlot);
            }

            // DeclaringClass.combine(state, value)
            String combineDesc = "(" + Type.getDescriptor(stateType) + Type.getDescriptor(valueType) + ")" + Type.getDescriptor(stateType);
            methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "combine", combineDesc, false);

            // state.set(groupId, result)
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                stateWrapperInternal,
                "set",
                "(I" + Type.getDescriptor(stateType) + ")V",
                false
            );
        }

        private String getVectorAccessorNameForGrouping(Class<?> type) {
            if (type == long.class) return "getLong";
            if (type == int.class) return "getInt";
            if (type == double.class) return "getDouble";
            if (type == boolean.class) return "getBoolean";
            if (type == BytesRef.class) return "getBytesRef";
            throw new IllegalArgumentException("Unsupported type: " + type);
        }

        private String getStateAccessorName(Class<?> type) {
            if (type == long.class) return "longValue";
            if (type == int.class) return "intValue";
            if (type == double.class) return "doubleValue";
            if (type == boolean.class) return "booleanValue";
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    private static class GroupingAddIntermediateInputImplementation implements Implementation {
        private final AggregatorSpec spec;
        private final Class<?> groupsType;

        GroupingAddIntermediateInputImplementation(AggregatorSpec spec, Class<?> groupsType) {
            this.spec = spec;
            this.groupsType = groupsType;
        }

        @Override
        public InstrumentedType prepare(InstrumentedType instrumentedType) {
            return instrumentedType;
        }

        @Override
        public ByteCodeAppender appender(Target implementationTarget) {
            return (methodVisitor, implementationContext, instrumentedMethod) -> {
                String className = implementationContext.getInstrumentedType().getInternalName();
                String stateWrapperInternal = spec.arrayStateWrapperClassName().replace('.', '/');
                String declaringClassInternal = spec.declaringClassInternalName();
                Class<?> stateType = spec.stateType();
                String groupsTypeInternal = groupsType.getName().replace('.', '/');
                boolean groupsIsBlock = groupsType.getSimpleName().endsWith("Block");

                // state.enableGroupIdTracking(new SeenGroupIds.Empty())
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                methodVisitor.visitTypeInsn(Opcodes.NEW, "org/elasticsearch/compute/aggregation/SeenGroupIds$Empty");
                methodVisitor.visitInsn(Opcodes.DUP);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKESPECIAL,
                    "org/elasticsearch/compute/aggregation/SeenGroupIds$Empty",
                    "<init>",
                    "()V",
                    false
                );
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    stateWrapperInternal,
                    "enableGroupIdTracking",
                    "(Lorg/elasticsearch/compute/aggregation/SeenGroupIds;)V",
                    false
                );

                // Block sumUncast = page.getBlock(channels.get(0));
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // page
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "channels", "Ljava/util/List;");
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/List", "get", "(I)Ljava/lang/Object;", true);
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue", "()I", false);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/data/Page",
                    "getBlock",
                    "(I)Lorg/elasticsearch/compute/data/Block;",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 4); // sumUncast

                // if (sumUncast.areAllValuesNull()) return;
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    "org/elasticsearch/compute/data/Block",
                    "areAllValuesNull",
                    "()Z",
                    true
                );
                Label notAllNull1 = new Label();
                methodVisitor.visitJumpInsn(Opcodes.IFEQ, notAllNull1);
                methodVisitor.visitInsn(Opcodes.RETURN);
                methodVisitor.visitLabel(notAllNull1);
                methodVisitor.visitFrame(Opcodes.F_APPEND, 1, new Object[] { "org/elasticsearch/compute/data/Block" }, 0, null);

                // LongVector sum = ((LongBlock) sumUncast).asVector();
                // For custom state with BytesRef, use BytesRefBlock/BytesRefVector
                String stateBlockType;
                String stateVectorType;
                if (spec.hasCustomGroupingState()) {
                    // For custom state, the intermediate state type is determined by the first intermediate state
                    String intermediateType = spec.intermediateState().get(0).elementType();
                    stateBlockType = getBlockTypeInternalForElementType(intermediateType);
                    stateVectorType = getVectorTypeInternalForElementType(intermediateType);
                } else {
                    stateBlockType = getBlockTypeInternal(stateType);
                    stateVectorType = getVectorTypeInternal(stateType);
                }
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 4);
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, stateBlockType);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, stateBlockType, "asVector", "()L" + stateVectorType + ";", true);
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 5); // sum

                // Block seenUncast = page.getBlock(channels.get(1));
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // page
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "channels", "Ljava/util/List;");
                methodVisitor.visitInsn(Opcodes.ICONST_1);
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/List", "get", "(I)Ljava/lang/Object;", true);
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue", "()I", false);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    "org/elasticsearch/compute/data/Page",
                    "getBlock",
                    "(I)Lorg/elasticsearch/compute/data/Block;",
                    false
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 6); // seenUncast

                // if (seenUncast.areAllValuesNull()) return;
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 6);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    "org/elasticsearch/compute/data/Block",
                    "areAllValuesNull",
                    "()Z",
                    true
                );
                Label notAllNull2 = new Label();
                methodVisitor.visitJumpInsn(Opcodes.IFEQ, notAllNull2);
                methodVisitor.visitInsn(Opcodes.RETURN);
                methodVisitor.visitLabel(notAllNull2);
                methodVisitor.visitFrame(
                    Opcodes.F_APPEND,
                    2,
                    new Object[] { stateVectorType, "org/elasticsearch/compute/data/Block" },
                    0,
                    null
                );

                // BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 6);
                methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, "org/elasticsearch/compute/data/BooleanBlock");
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEINTERFACE,
                    "org/elasticsearch/compute/data/BooleanBlock",
                    "asVector",
                    "()Lorg/elasticsearch/compute/data/BooleanVector;",
                    true
                );
                methodVisitor.visitVarInsn(Opcodes.ASTORE, 7); // seen

                // for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++)
                methodVisitor.visitInsn(Opcodes.ICONST_0);
                methodVisitor.visitVarInsn(Opcodes.ISTORE, 8); // groupPosition

                Label loopStart = new Label();
                Label loopEnd = new Label();

                methodVisitor.visitLabel(loopStart);
                methodVisitor.visitFrame(
                    Opcodes.F_APPEND,
                    2,
                    new Object[] { "org/elasticsearch/compute/data/BooleanVector", Opcodes.INTEGER },
                    0,
                    null
                );

                methodVisitor.visitVarInsn(Opcodes.ILOAD, 8);
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "getPositionCount", "()I", true);
                methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, loopEnd);

                Label continueLabel = new Label();

                if (groupsIsBlock) {
                    // if (groups.isNull(groupPosition)) continue;
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, 8); // groupPosition
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "isNull", "(I)Z", true);
                    methodVisitor.visitJumpInsn(Opcodes.IFNE, continueLabel);

                    // int groupStart = groups.getFirstValueIndex(groupPosition);
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, 8); // groupPosition
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "getFirstValueIndex", "(I)I", true);
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, 9); // groupStart

                    // int groupEnd = groupStart + groups.getValueCount(groupPosition);
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, 9); // groupStart
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, 8); // groupPosition
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "getValueCount", "(I)I", true);
                    methodVisitor.visitInsn(Opcodes.IADD);
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, 10); // groupEnd

                    // for (int g = groupStart; g < groupEnd; g++)
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, 9); // groupStart
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, 11); // g

                    Label innerLoopStart = new Label();
                    Label innerLoopEnd = new Label();

                    methodVisitor.visitLabel(innerLoopStart);
                    methodVisitor.visitFrame(
                        Opcodes.F_APPEND,
                        3,
                        new Object[] { Opcodes.INTEGER, Opcodes.INTEGER, Opcodes.INTEGER },
                        0,
                        null
                    );

                    methodVisitor.visitVarInsn(Opcodes.ILOAD, 11); // g
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, 10); // groupEnd
                    methodVisitor.visitJumpInsn(Opcodes.IF_ICMPGE, innerLoopEnd);

                    // int groupId = groups.getInt(g);
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, 11); // g
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "getInt", "(I)I", true);
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, 12); // groupId

                    // int valuesPosition = groupPosition + positionOffset;
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, 8); // groupPosition
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, 1); // positionOffset
                    methodVisitor.visitInsn(Opcodes.IADD);
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, 13); // valuesPosition

                    // if (seen.getBoolean(valuesPosition)) { combine }
                    generateIntermediateCombine(
                        methodVisitor,
                        className,
                        stateWrapperInternal,
                        declaringClassInternal,
                        stateVectorType,
                        stateType,
                        12,
                        13
                    ); // groupId=12, valuesPosition=13

                    // g++
                    methodVisitor.visitIincInsn(11, 1);
                    methodVisitor.visitJumpInsn(Opcodes.GOTO, innerLoopStart);

                    methodVisitor.visitLabel(innerLoopEnd);
                    methodVisitor.visitFrame(Opcodes.F_CHOP, 3, null, 0, null);
                } else {
                    // IntVector case - simpler
                    // int groupId = groups.getInt(groupPosition);
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // groups
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, 8); // groupPosition
                    methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, groupsTypeInternal, "getInt", "(I)I", true);
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, 9); // groupId

                    // int valuesPosition = groupPosition + positionOffset;
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, 8); // groupPosition
                    methodVisitor.visitVarInsn(Opcodes.ILOAD, 1); // positionOffset
                    methodVisitor.visitInsn(Opcodes.IADD);
                    methodVisitor.visitVarInsn(Opcodes.ISTORE, 10); // valuesPosition

                    // if (seen.getBoolean(valuesPosition)) { combine }
                    generateIntermediateCombine(
                        methodVisitor,
                        className,
                        stateWrapperInternal,
                        declaringClassInternal,
                        stateVectorType,
                        stateType,
                        9,
                        10
                    ); // groupId=9, valuesPosition=10
                }

                // continue label and increment
                methodVisitor.visitLabel(continueLabel);
                methodVisitor.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
                methodVisitor.visitIincInsn(8, 1);
                methodVisitor.visitJumpInsn(Opcodes.GOTO, loopStart);

                methodVisitor.visitLabel(loopEnd);
                methodVisitor.visitFrame(Opcodes.F_CHOP, 1, null, 0, null);
                methodVisitor.visitInsn(Opcodes.RETURN);

                int maxStack = 6;
                int maxLocals = groupsIsBlock ? 14 : 11;
                return new ByteCodeAppender.Size(maxStack, maxLocals);
            };
        }

        private void generateIntermediateCombine(
            MethodVisitor methodVisitor,
            String className,
            String stateWrapperInternal,
            String declaringClassInternal,
            String stateVectorType,
            Class<?> stateType,
            int groupIdSlot,
            int valuesPositionSlot
        ) {
            if (spec.hasCustomGroupingState()) {
                generateCustomStateIntermediateCombine(
                    methodVisitor,
                    className,
                    stateWrapperInternal,
                    declaringClassInternal,
                    groupIdSlot,
                    valuesPositionSlot
                );
            } else {
                generatePrimitiveStateIntermediateCombine(
                    methodVisitor,
                    className,
                    stateWrapperInternal,
                    declaringClassInternal,
                    stateVectorType,
                    stateType,
                    groupIdSlot,
                    valuesPositionSlot
                );
            }
        }

        private void generateCustomStateIntermediateCombine(
            MethodVisitor methodVisitor,
            String className,
            String stateWrapperInternal,
            String declaringClassInternal,
            int groupIdSlot,
            int valuesPositionSlot
        ) {
            // DeclaringClass.combineIntermediate(state, groupId, value.getBytesRef(valuesPosition, scratch),
            // seen.getBoolean(valuesPosition))
            // Load state
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
            methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");

            // Load groupId
            methodVisitor.visitVarInsn(Opcodes.ILOAD, groupIdSlot);

            // Load value: sum.getBytesRef(valuesPosition, scratch)
            // Note: For custom state with BytesRef, we need a scratch object
            // We'll create it inline since we don't have a dedicated slot
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 5); // sum (BytesRefVector)
            methodVisitor.visitVarInsn(Opcodes.ILOAD, valuesPositionSlot);
            methodVisitor.visitTypeInsn(Opcodes.NEW, Type.getInternalName(BytesRef.class));
            methodVisitor.visitInsn(Opcodes.DUP);
            methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, Type.getInternalName(BytesRef.class), "<init>", "()V", false);
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEINTERFACE,
                "org/elasticsearch/compute/data/BytesRefVector",
                "getBytesRef",
                "(I" + Type.getDescriptor(BytesRef.class) + ")" + Type.getDescriptor(BytesRef.class),
                true
            );

            // Load seen: seen.getBoolean(valuesPosition)
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 7); // seen
            methodVisitor.visitVarInsn(Opcodes.ILOAD, valuesPositionSlot);
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEINTERFACE,
                "org/elasticsearch/compute/data/BooleanVector",
                "getBoolean",
                "(I)Z",
                true
            );

            // Call combineIntermediate(GroupingState, int, BytesRef, boolean)
            Class<?> groupingStateType = spec.groupingStateType();
            String combineIntermediateDesc = "(" + Type.getDescriptor(groupingStateType) + "I" + Type.getDescriptor(BytesRef.class) + "Z)V";
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKESTATIC,
                declaringClassInternal,
                "combineIntermediate",
                combineIntermediateDesc,
                false
            );
        }

        private void generatePrimitiveStateIntermediateCombine(
            MethodVisitor methodVisitor,
            String className,
            String stateWrapperInternal,
            String declaringClassInternal,
            String stateVectorType,
            Class<?> stateType,
            int groupIdSlot,
            int valuesPositionSlot
        ) {
            // if (seen.getBoolean(valuesPosition))
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 7); // seen
            methodVisitor.visitVarInsn(Opcodes.ILOAD, valuesPositionSlot);
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEINTERFACE,
                "org/elasticsearch/compute/data/BooleanVector",
                "getBoolean",
                "(I)Z",
                true
            );
            Label skipCombine = new Label();
            methodVisitor.visitJumpInsn(Opcodes.IFEQ, skipCombine);

            // state.set(groupId, DeclaringClass.combine(state.getOrDefault(groupId), sum.getLong(valuesPosition)))
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
            methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
            methodVisitor.visitVarInsn(Opcodes.ILOAD, groupIdSlot);

            // state.getOrDefault(groupId)
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
            methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
            methodVisitor.visitVarInsn(Opcodes.ILOAD, groupIdSlot);
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                stateWrapperInternal,
                "getOrDefault",
                "(I)" + Type.getDescriptor(stateType),
                false
            );

            // sum.getLong(valuesPosition)
            String stateAccessor = getStateVectorAccessorName(stateType);
            methodVisitor.visitVarInsn(Opcodes.ALOAD, 5); // sum
            methodVisitor.visitVarInsn(Opcodes.ILOAD, valuesPositionSlot);
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEINTERFACE,
                stateVectorType,
                stateAccessor,
                "(I)" + Type.getDescriptor(stateType),
                true
            );

            // DeclaringClass.combine(state, state)
            String combineDesc = "(" + Type.getDescriptor(stateType) + Type.getDescriptor(stateType) + ")" + Type.getDescriptor(stateType);
            methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "combine", combineDesc, false);

            // state.set(groupId, result)
            methodVisitor.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                stateWrapperInternal,
                "set",
                "(I" + Type.getDescriptor(stateType) + ")V",
                false
            );

            methodVisitor.visitLabel(skipCombine);
            methodVisitor.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
        }

        private String getStateVectorAccessorName(Class<?> type) {
            if (type == long.class) return "getLong";
            if (type == int.class) return "getInt";
            if (type == double.class) return "getDouble";
            if (type == boolean.class) return "getBoolean";
            throw new IllegalArgumentException("Unsupported type: " + type);
        }

        private String getBlockTypeInternalForElementType(String elementType) {
            return switch (elementType) {
                case "LONG" -> "org/elasticsearch/compute/data/LongBlock";
                case "INT" -> "org/elasticsearch/compute/data/IntBlock";
                case "DOUBLE" -> "org/elasticsearch/compute/data/DoubleBlock";
                case "BOOLEAN" -> "org/elasticsearch/compute/data/BooleanBlock";
                case "BYTES_REF" -> "org/elasticsearch/compute/data/BytesRefBlock";
                default -> throw new IllegalArgumentException("Unsupported element type: " + elementType);
            };
        }

        private String getVectorTypeInternalForElementType(String elementType) {
            return switch (elementType) {
                case "LONG" -> "org/elasticsearch/compute/data/LongVector";
                case "INT" -> "org/elasticsearch/compute/data/IntVector";
                case "DOUBLE" -> "org/elasticsearch/compute/data/DoubleVector";
                case "BOOLEAN" -> "org/elasticsearch/compute/data/BooleanVector";
                case "BYTES_REF" -> "org/elasticsearch/compute/data/BytesRefVector";
                default -> throw new IllegalArgumentException("Unsupported element type: " + elementType);
            };
        }
    }

    private static class MaybeEnableGroupIdTrackingImplementation implements Implementation {
        private final AggregatorSpec spec;

        MaybeEnableGroupIdTrackingImplementation(AggregatorSpec spec) {
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
                String stateWrapperInternal = spec.arrayStateWrapperClassName().replace('.', '/');
                Class<?> valueType = spec.valueType();
                String blockType = getBlockTypeInternal(valueType);

                // if (vBlock.mayHaveNulls()) { state.enableGroupIdTracking(seenGroupIds); }
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 2); // vBlock
                methodVisitor.visitMethodInsn(Opcodes.INVOKEINTERFACE, blockType, "mayHaveNulls", "()Z", true);

                Label endLabel = new Label();
                methodVisitor.visitJumpInsn(Opcodes.IFEQ, endLabel);

                // state.enableGroupIdTracking(seenGroupIds)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0); // this
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // seenGroupIds
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    stateWrapperInternal,
                    "enableGroupIdTracking",
                    "(Lorg/elasticsearch/compute/aggregation/SeenGroupIds;)V",
                    false
                );

                methodVisitor.visitLabel(endLabel);
                methodVisitor.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(2, 3);
            };
        }
    }

    private static class SelectedMayContainUnseenGroupsImplementation implements Implementation {
        private final AggregatorSpec spec;

        SelectedMayContainUnseenGroupsImplementation(AggregatorSpec spec) {
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
                String stateWrapperInternal = spec.arrayStateWrapperClassName().replace('.', '/');

                // state.enableGroupIdTracking(seenGroupIds)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1);
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    stateWrapperInternal,
                    "enableGroupIdTracking",
                    "(Lorg/elasticsearch/compute/aggregation/SeenGroupIds;)V",
                    false
                );

                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(2, 2);
            };
        }
    }

    private static class GroupingEvaluateIntermediateImplementation implements Implementation {
        private final AggregatorSpec spec;

        GroupingEvaluateIntermediateImplementation(AggregatorSpec spec) {
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
                String stateWrapperInternal = spec.arrayStateWrapperClassName().replace('.', '/');

                // state.toIntermediate(blocks, offset, selected, driverContext)
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // blocks
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 2); // offset
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // selected
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "driverContext", Type.getDescriptor(DriverContext.class));
                methodVisitor.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    stateWrapperInternal,
                    "toIntermediate",
                    "([L"
                        + Type.getInternalName(Block.class)
                        + ";I"
                        + Type.getDescriptor(IntVector.class)
                        + Type.getDescriptor(DriverContext.class)
                        + ")V",
                    false
                );

                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(5, 4);
            };
        }
    }

    private static class GroupingEvaluateFinalImplementation implements Implementation {
        private final AggregatorSpec spec;

        GroupingEvaluateFinalImplementation(AggregatorSpec spec) {
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
                String stateWrapperInternal = spec.arrayStateWrapperClassName().replace('.', '/');
                String declaringClassInternal = spec.declaringClassInternalName();

                // blocks[offset] = ...
                methodVisitor.visitVarInsn(Opcodes.ALOAD, 1); // blocks
                methodVisitor.visitVarInsn(Opcodes.ILOAD, 2); // offset

                if (spec.hasCustomGroupingState()) {
                    // Custom state: blocks[offset] = DeclaringClass.evaluateFinal(state, selected, ctx)
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // selected
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // ctx

                    // DeclaringClass.evaluateFinal(GroupingState, IntVector, GroupingAggregatorEvaluationContext)
                    Class<?> groupingStateType = spec.groupingStateType();
                    String evalFinalDesc = "("
                        + Type.getDescriptor(groupingStateType)
                        + Type.getDescriptor(IntVector.class)
                        + "Lorg/elasticsearch/compute/aggregation/GroupingAggregatorEvaluationContext;)"
                        + Type.getDescriptor(Block.class);
                    methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, declaringClassInternal, "evaluateFinal", evalFinalDesc, false);
                } else {
                    // Primitive state: blocks[offset] = state.toValuesBlock(selected, ctx.driverContext())
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
                    methodVisitor.visitFieldInsn(Opcodes.GETFIELD, className, "state", "L" + stateWrapperInternal + ";");
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 3); // selected

                    // ctx.driverContext()
                    methodVisitor.visitVarInsn(Opcodes.ALOAD, 4); // ctx
                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        "org/elasticsearch/compute/aggregation/GroupingAggregatorEvaluationContext",
                        "driverContext",
                        "()" + Type.getDescriptor(DriverContext.class),
                        false
                    );

                    methodVisitor.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        stateWrapperInternal,
                        "toValuesBlock",
                        "("
                            + Type.getDescriptor(IntVector.class)
                            + Type.getDescriptor(DriverContext.class)
                            + ")L"
                            + Type.getInternalName(Block.class)
                            + ";",
                        false
                    );
                }

                methodVisitor.visitInsn(Opcodes.AASTORE);
                methodVisitor.visitInsn(Opcodes.RETURN);
                return new ByteCodeAppender.Size(5, 5);
            };
        }
    }

    // ============================================================================================
    // ADD INPUT CLASS IMPLEMENTATIONS
    // ============================================================================================
    /**
     * ASM visitor wrapper that enables automatic frame computation.
     * This removes the need for manual frame declarations in the bytecode generation.
     */
    private static class FrameComputingVisitorWrapper implements AsmVisitorWrapper {
        @Override
        public int mergeWriter(int flags) {
            return flags | ClassWriter.COMPUTE_FRAMES;
        }

        @Override
        public int mergeReader(int flags) {
            return flags;
        }

        @Override
        public ClassVisitor wrap(
            TypeDescription instrumentedType,
            ClassVisitor classVisitor,
            Implementation.Context implementationContext,
            TypePool typePool,
            FieldList<FieldDescription.InDefinedShape> fields,
            MethodList<?> methods,
            int writerFlags,
            int readerFlags
        ) {
            return classVisitor;
        }
    }
}

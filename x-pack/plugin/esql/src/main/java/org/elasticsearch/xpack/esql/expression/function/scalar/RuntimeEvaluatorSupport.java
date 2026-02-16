/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.compute.ann.RuntimeEvaluator;
import org.elasticsearch.compute.ann.RuntimeMvEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.runtime.RuntimeEvaluatorException;
import org.elasticsearch.compute.runtime.RuntimeEvaluatorGenerator;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Support utilities for runtime evaluator generation.
 * <p>
 * This class provides helper methods for creating runtime-generated evaluators
 * from methods annotated with {@link RuntimeEvaluator}.
 */
public final class RuntimeEvaluatorSupport {

    private RuntimeEvaluatorSupport() {
        // Utility class
    }

    /**
     * Creates a factory for a runtime-generated unary evaluator.
     * <p>
     * This method:
     * <ol>
     *   <li>Finds the appropriate {@code @RuntimeEvaluator} method for the given data type</li>
     *   <li>Generates (or retrieves from cache) the evaluator class</li>
     *   <li>Returns a factory that can instantiate the evaluator</li>
     * </ol>
     *
     * @param functionClass the class containing {@code @RuntimeEvaluator} methods
     * @param dataType the ES|QL data type to process
     * @param source the source location for error reporting
     * @param fieldFactory the factory for the input field evaluator
     * @return a factory that creates runtime-generated evaluator instances
     * @throws IllegalArgumentException if no suitable method is found for the data type
     */
    public static EvalOperator.ExpressionEvaluator.Factory createFactory(
        Class<?> functionClass,
        DataType dataType,
        Source source,
        EvalOperator.ExpressionEvaluator.Factory fieldFactory
    ) {
        return createFactory(functionClass, dataType, source, List.of(fieldFactory));
    }

    /**
     * Creates a factory for a runtime-generated N-ary evaluator.
     * <p>
     * This method supports functions with any number of parameters.
     *
     * @param functionClass the class containing {@code @RuntimeEvaluator} methods
     * @param dataType the ES|QL data type to process (used to select the right method variant)
     * @param source the source location for error reporting
     * @param fieldFactories the factories for the input field evaluators (one per parameter)
     * @return a factory that creates runtime-generated evaluator instances
     * @throws IllegalArgumentException if no suitable method is found for the data type
     */
    public static EvalOperator.ExpressionEvaluator.Factory createFactory(
        Class<?> functionClass,
        DataType dataType,
        Source source,
        List<EvalOperator.ExpressionEvaluator.Factory> fieldFactories
    ) {
        // Find the method annotated with @RuntimeEvaluator for this data type
        Method processMethod = findMethodForDataType(functionClass, dataType);

        // Validate parameter count matches (for non-fixed parameters)
        // Note: This validation doesn't account for @RuntimeFixed parameters
        // Use createFactoryWithFixed for methods with fixed parameters
        if (processMethod.getParameterCount() != fieldFactories.size()) {
            throw new IllegalArgumentException(
                "Method "
                    + processMethod.getName()
                    + " expects "
                    + processMethod.getParameterCount()
                    + " parameters but "
                    + fieldFactories.size()
                    + " field factories were provided. "
                    + "If the method has @RuntimeFixed parameters, use createFactoryWithFixed instead."
            );
        }

        // Generate or retrieve the evaluator class
        // Use the function class's classloader so generated code can access the function
        try {
            RuntimeEvaluatorGenerator generator = RuntimeEvaluatorGenerator.getInstance(functionClass.getClassLoader());
            Class<?> evaluatorClass = generator.getOrGenerateEvaluator(processMethod);

            // Return a factory that instantiates the generated evaluator
            return new RuntimeEvaluatorFactory(source, fieldFactories, List.of(), evaluatorClass, processMethod);
        } catch (RuntimeEvaluatorException e) {
            // Propagate with user-friendly message
            throw new IllegalArgumentException(e.getUserMessage(), e);
        } catch (LinkageError e) {
            // Catches VerifyError, ClassFormatError, and other bytecode-related errors
            throw new IllegalArgumentException(
                "Failed to generate evaluator bytecode for "
                    + functionClass.getSimpleName()
                    + " (method "
                    + processMethod.getName()
                    + "): "
                    + e.getMessage(),
                e
            );
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Failed to create evaluator for " + functionClass.getSimpleName() + ": " + e.getMessage(),
                e
            );
        }
    }

    /**
     * Creates a factory for a runtime-generated evaluator with @RuntimeFixed parameters.
     * <p>
     * This method supports functions that have a mix of evaluated parameters (from expressions)
     * and fixed parameters (computed once at factory creation time).
     *
     * @param functionClass the class containing {@code @RuntimeEvaluator} methods
     * @param dataType the ES|QL data type to process (used to select the right method variant)
     * @param source the source location for error reporting
     * @param fieldFactories the factories for the evaluated input parameters
     * @param fixedValues the values for @RuntimeFixed parameters, in the order they appear in the method signature
     * @return a factory that creates runtime-generated evaluator instances
     * @throws IllegalArgumentException if no suitable method is found for the data type
     */
    public static EvalOperator.ExpressionEvaluator.Factory createFactoryWithFixed(
        Class<?> functionClass,
        DataType dataType,
        Source source,
        List<EvalOperator.ExpressionEvaluator.Factory> fieldFactories,
        List<Object> fixedValues
    ) {
        // Find the method annotated with @RuntimeEvaluator for this data type
        Method processMethod = findMethodForDataType(functionClass, dataType);

        // Generate or retrieve the evaluator class
        // Use the function class's classloader so generated code can access the function
        try {
            RuntimeEvaluatorGenerator generator = RuntimeEvaluatorGenerator.getInstance(functionClass.getClassLoader());
            Class<?> evaluatorClass = generator.getOrGenerateEvaluator(processMethod);

            // Return a factory that instantiates the generated evaluator with fixed values
            return new RuntimeEvaluatorFactory(source, fieldFactories, fixedValues, evaluatorClass, processMethod);
        } catch (RuntimeEvaluatorException e) {
            throw new IllegalArgumentException(e.getUserMessage(), e);
        } catch (LinkageError e) {
            // Catches VerifyError, ClassFormatError, and other bytecode-related errors
            throw new IllegalArgumentException(
                "Failed to generate evaluator bytecode for "
                    + functionClass.getSimpleName()
                    + " (method "
                    + processMethod.getName()
                    + "): "
                    + e.getMessage(),
                e
            );
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Failed to create evaluator for " + functionClass.getSimpleName() + ": " + e.getMessage(),
                e
            );
        }
    }

    /**
     * Creates a factory for a runtime-generated variadic evaluator.
     * <p>
     * Variadic functions take a variable number of arguments (like GREATEST, LEAST, COALESCE).
     * The process method takes an array parameter: {@code process(int[] values)}.
     *
     * @param functionClass the class containing {@code @RuntimeEvaluator} methods
     * @param dataType the ES|QL data type to process (used to select the right method variant)
     * @param source the source location for error reporting
     * @param fieldFactories the factories for the input field evaluators (variable number)
     * @return a factory that creates runtime-generated evaluator instances
     * @throws IllegalArgumentException if no suitable method is found for the data type
     */
    public static EvalOperator.ExpressionEvaluator.Factory createVariadicFactory(
        Class<?> functionClass,
        DataType dataType,
        Source source,
        List<EvalOperator.ExpressionEvaluator.Factory> fieldFactories
    ) {
        // Find the method annotated with @RuntimeEvaluator for this data type
        Method processMethod = findMethodForDataType(functionClass, dataType);

        // Generate or retrieve the evaluator class
        try {
            RuntimeEvaluatorGenerator generator = RuntimeEvaluatorGenerator.getInstance(functionClass.getClassLoader());
            Class<?> evaluatorClass = generator.getOrGenerateEvaluator(processMethod);

            // Return a factory that instantiates the generated evaluator with array of evaluators
            return new VariadicEvaluatorFactory(source, fieldFactories, evaluatorClass);
        } catch (RuntimeEvaluatorException e) {
            throw new IllegalArgumentException(e.getUserMessage(), e);
        } catch (LinkageError e) {
            throw new IllegalArgumentException(
                "Failed to generate evaluator bytecode for "
                    + functionClass.getSimpleName()
                    + " (method "
                    + processMethod.getName()
                    + "): "
                    + e.getMessage(),
                e
            );
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Failed to create evaluator for " + functionClass.getSimpleName() + ": " + e.getMessage(),
                e
            );
        }
    }

    /**
     * Creates a factory for a runtime-generated MvEvaluator.
     * <p>
     * MvEvaluators reduce multivalued fields to a single value using pairwise processing.
     * The process method has signature: {@code process(T current, T next)}.
     *
     * @param functionClass the class containing {@code @RuntimeMvEvaluator} methods
     * @param dataType the ES|QL data type to process (used to select the right method variant)
     * @param source the source location for error reporting
     * @param fieldFactory the factory for the input field evaluator
     * @return a factory that creates runtime-generated MvEvaluator instances
     * @throws IllegalArgumentException if no suitable method is found for the data type
     */
    public static EvalOperator.ExpressionEvaluator.Factory createMvFactory(
        Class<?> functionClass,
        DataType dataType,
        Source source,
        EvalOperator.ExpressionEvaluator.Factory fieldFactory
    ) {
        // Find the method annotated with @RuntimeMvEvaluator for this data type
        Method processMethod = findMvMethodForDataType(functionClass, dataType);

        // Generate or retrieve the evaluator class
        try {
            RuntimeEvaluatorGenerator generator = RuntimeEvaluatorGenerator.getInstance(functionClass.getClassLoader());
            Class<?> evaluatorClass = generator.getOrGenerateMvEvaluator(processMethod);

            // Return a factory that instantiates the generated MvEvaluator
            return new MvEvaluatorFactory(source, fieldFactory, evaluatorClass, processMethod);
        } catch (RuntimeEvaluatorException e) {
            throw new IllegalArgumentException(e.getUserMessage(), e);
        } catch (LinkageError e) {
            throw new IllegalArgumentException(
                "Failed to generate MvEvaluator bytecode for "
                    + functionClass.getSimpleName()
                    + " (method "
                    + processMethod.getName()
                    + "): "
                    + e.getMessage(),
                e
            );
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Failed to create MvEvaluator for " + functionClass.getSimpleName() + ": " + e.getMessage(),
                e
            );
        }
    }

    /**
     * Finds the {@code @RuntimeMvEvaluator} method for the given data type.
     *
     * @param functionClass the class containing {@code @RuntimeMvEvaluator} methods
     * @param dataType the ES|QL data type to find a method for
     * @return the method that processes the given data type
     * @throws IllegalArgumentException if no suitable method is found
     */
    public static Method findMvMethodForDataType(Class<?> functionClass, DataType dataType) {
        // Map DataType to the expected extraName
        String expectedExtraName = getExtraNameForDataType(dataType);

        // Find all methods with @RuntimeMvEvaluator annotation
        Method[] methods = functionClass.getMethods();
        for (Method method : methods) {
            RuntimeMvEvaluator annotation = method.getAnnotation(RuntimeMvEvaluator.class);
            if (annotation != null && annotation.extraName().equals(expectedExtraName)) {
                return method;
            }
        }

        // No matching method found
        throw new IllegalArgumentException(
            "No @RuntimeMvEvaluator method found for data type "
                + dataType
                + " (expected extraName='"
                + expectedExtraName
                + "') in class "
                + functionClass.getName()
        );
    }

    /**
     * Finds the {@code @RuntimeEvaluator} method for the given data type.
     * <p>
     * The method is selected based on the {@code extraName} attribute of the
     * {@link RuntimeEvaluator} annotation, which should match the data type:
     * <ul>
     *   <li>"Double" → {@link DataType#DOUBLE}</li>
     *   <li>"Long" → {@link DataType#LONG}</li>
     *   <li>"Int" → {@link DataType#INTEGER}</li>
     *   <li>"UnsignedLong" → {@link DataType#UNSIGNED_LONG}</li>
     * </ul>
     *
     * @param functionClass the class containing {@code @RuntimeEvaluator} methods
     * @param dataType the ES|QL data type to find a method for
     * @return the method that processes the given data type
     * @throws IllegalArgumentException if no suitable method is found
     */
    public static Method findMethodForDataType(Class<?> functionClass, DataType dataType) {
        // Map DataType to the expected extraName
        String expectedExtraName = getExtraNameForDataType(dataType);

        // Find all methods with @RuntimeEvaluator annotation
        Method[] methods = functionClass.getMethods();
        for (Method method : methods) {
            RuntimeEvaluator annotation = method.getAnnotation(RuntimeEvaluator.class);
            if (annotation != null && annotation.extraName().equals(expectedExtraName)) {
                return method;
            }
        }

        // No matching method found
        throw new IllegalArgumentException(
            "No @RuntimeEvaluator method found for data type "
                + dataType
                + " (expected extraName='"
                + expectedExtraName
                + "') in class "
                + functionClass.getName()
        );
    }

    /**
     * Maps an ES|QL data type to the expected {@code extraName} attribute.
     */
    private static String getExtraNameForDataType(DataType dataType) {
        if (dataType == DataType.DOUBLE) {
            return "Double";
        } else if (dataType == DataType.LONG) {
            return "Long";
        } else if (dataType == DataType.INTEGER) {
            return "Int";
        } else if (dataType == DataType.UNSIGNED_LONG) {
            return "UnsignedLong";
        } else if (dataType == DataType.KEYWORD || dataType == DataType.TEXT) {
            return "";  // BytesRef methods typically have empty extraName
        } else {
            throw new IllegalArgumentException("Unsupported data type for runtime evaluation: " + dataType);
        }
    }

    /**
     * Factory that creates instances of runtime-generated evaluators.
     * Supports N-ary functions with any number of field factories and @RuntimeFixed parameters.
     */
    private static class RuntimeEvaluatorFactory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;
        private final List<EvalOperator.ExpressionEvaluator.Factory> fieldFactories;
        private final List<Object> fixedValues;
        private final Class<?> evaluatorClass;
        private final Method processMethod;

        RuntimeEvaluatorFactory(
            Source source,
            List<EvalOperator.ExpressionEvaluator.Factory> fieldFactories,
            List<Object> fixedValues,
            Class<?> evaluatorClass,
            Method processMethod
        ) {
            this.source = source;
            this.fieldFactories = fieldFactories;
            this.fixedValues = fixedValues;
            this.evaluatorClass = evaluatorClass;
            this.processMethod = processMethod;
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            try {
                // Determine which parameters are fixed by checking for @RuntimeFixed annotation
                java.lang.reflect.Parameter[] params = processMethod.getParameters();
                int totalParamCount = params.length;

                // Build constructor parameter types: (Source, [params...], DriverContext)
                // For evaluated params: ExpressionEvaluator
                // For fixed params: their actual type
                Class<?>[] constructorParamTypes = new Class<?>[totalParamCount + 2];
                constructorParamTypes[0] = Source.class;

                int evalIdx = 0;
                int fixedIdx = 0;
                for (int i = 0; i < totalParamCount; i++) {
                    boolean isFixed = params[i].getAnnotation(org.elasticsearch.compute.ann.RuntimeFixed.class) != null;
                    if (isFixed) {
                        constructorParamTypes[i + 1] = params[i].getType();
                    } else {
                        constructorParamTypes[i + 1] = EvalOperator.ExpressionEvaluator.class;
                    }
                }
                constructorParamTypes[totalParamCount + 1] = DriverContext.class;

                // Build constructor arguments
                Object[] constructorArgs = new Object[totalParamCount + 2];
                constructorArgs[0] = source;

                evalIdx = 0;
                fixedIdx = 0;
                for (int i = 0; i < totalParamCount; i++) {
                    boolean isFixed = params[i].getAnnotation(org.elasticsearch.compute.ann.RuntimeFixed.class) != null;
                    if (isFixed) {
                        constructorArgs[i + 1] = fixedValues.get(fixedIdx++);
                    } else {
                        constructorArgs[i + 1] = fieldFactories.get(evalIdx++).get(context);
                    }
                }
                constructorArgs[totalParamCount + 1] = context;

                // Instantiate the generated evaluator using reflection
                Constructor<?> ctor = evaluatorClass.getConstructor(constructorParamTypes);
                return (EvalOperator.ExpressionEvaluator) ctor.newInstance(constructorArgs);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to instantiate runtime evaluator: " + evaluatorClass.getName(), e);
            }
        }

        @Override
        public String toString() {
            // Build toString to match the evaluator's toString format: "EvaluatorName[field0=..., field1=...]"
            StringBuilder sb = new StringBuilder(evaluatorClass.getSimpleName()).append("[");
            int evalIdx = 0;
            int fixedIdx = 0;
            java.lang.reflect.Parameter[] params = processMethod.getParameters();
            for (int i = 0; i < params.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                boolean isFixed = params[i].getAnnotation(org.elasticsearch.compute.ann.RuntimeFixed.class) != null;
                if (isFixed) {
                    sb.append("fixed").append(fixedIdx).append("=").append(fixedValues.get(fixedIdx));
                    fixedIdx++;
                } else {
                    sb.append("field").append(evalIdx).append("=").append(fieldFactories.get(evalIdx));
                    evalIdx++;
                }
            }
            sb.append("]");
            return sb.toString();
        }
    }

    /**
     * Factory that creates instances of runtime-generated variadic evaluators.
     * Variadic evaluators take an array of ExpressionEvaluators.
     */
    private static class VariadicEvaluatorFactory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;
        private final List<EvalOperator.ExpressionEvaluator.Factory> fieldFactories;
        private final Class<?> evaluatorClass;

        VariadicEvaluatorFactory(Source source, List<EvalOperator.ExpressionEvaluator.Factory> fieldFactories, Class<?> evaluatorClass) {
            this.source = source;
            this.fieldFactories = fieldFactories;
            this.evaluatorClass = evaluatorClass;
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            try {
                // Build array of evaluators
                EvalOperator.ExpressionEvaluator[] evaluators = fieldFactories.stream()
                    .map(f -> f.get(context))
                    .toArray(EvalOperator.ExpressionEvaluator[]::new);

                // Constructor: (Source, ExpressionEvaluator[], DriverContext)
                Constructor<?> ctor = evaluatorClass.getConstructor(
                    Source.class,
                    EvalOperator.ExpressionEvaluator[].class,
                    DriverContext.class
                );
                return (EvalOperator.ExpressionEvaluator) ctor.newInstance(source, evaluators, context);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to instantiate variadic runtime evaluator: " + evaluatorClass.getName(), e);
            }
        }

        @Override
        public String toString() {
            String fields = fieldFactories.stream().map(Object::toString).collect(Collectors.joining(", "));
            return evaluatorClass.getSimpleName() + "[values=" + fields + "]";
        }
    }

    /**
     * Factory that creates instances of runtime-generated MvEvaluators.
     * MvEvaluators reduce multivalued fields to a single value.
     */
    private static class MvEvaluatorFactory implements EvalOperator.ExpressionEvaluator.Factory {
        private final Source source;
        private final EvalOperator.ExpressionEvaluator.Factory fieldFactory;
        private final Class<?> evaluatorClass;
        private final Method processMethod;

        MvEvaluatorFactory(
            Source source,
            EvalOperator.ExpressionEvaluator.Factory fieldFactory,
            Class<?> evaluatorClass,
            Method processMethod
        ) {
            this.source = source;
            this.fieldFactory = fieldFactory;
            this.evaluatorClass = evaluatorClass;
            this.processMethod = processMethod;
        }

        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            try {
                // Check if the method has warnExceptions
                RuntimeMvEvaluator annotation = processMethod.getAnnotation(RuntimeMvEvaluator.class);
                boolean hasWarnExceptions = annotation != null && annotation.warnExceptions().length > 0;

                if (hasWarnExceptions) {
                    // Constructor: (Source, ExpressionEvaluator, DriverContext)
                    Constructor<?> ctor = evaluatorClass.getConstructor(
                        Source.class,
                        EvalOperator.ExpressionEvaluator.class,
                        DriverContext.class
                    );
                    return (EvalOperator.ExpressionEvaluator) ctor.newInstance(source, fieldFactory.get(context), context);
                } else {
                    // Constructor: (ExpressionEvaluator, DriverContext)
                    Constructor<?> ctor = evaluatorClass.getConstructor(EvalOperator.ExpressionEvaluator.class, DriverContext.class);
                    return (EvalOperator.ExpressionEvaluator) ctor.newInstance(fieldFactory.get(context), context);
                }
            } catch (Exception e) {
                throw new IllegalStateException("Failed to instantiate MvEvaluator: " + evaluatorClass.getName(), e);
            }
        }

        @Override
        public String toString() {
            return evaluatorClass.getSimpleName() + "[field=" + fieldFactory + "]";
        }
    }
}

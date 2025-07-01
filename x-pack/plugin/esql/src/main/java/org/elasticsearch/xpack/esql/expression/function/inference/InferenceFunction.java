/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Base class for ESQL functions that perform inference using an `inference_id` and optional parameters.
 */
public abstract class InferenceFunction extends Function implements OptionalArgument {
    public static final String INFERENCE_ID_OPTION_NAME = "inference_id";

    public static final List<OptionalArgumentsValidator> DEFAULT_OPTIONAL_ARGUMENTS_VALIDATORS = List.of(
        new InferenceIdOptionalArgumentsValidator()
    );

    private final Expression inferenceId;
    private final Expression options;

    @SuppressWarnings("this-escape")
    protected InferenceFunction(Source source, List<Expression> children, Expression options) {
        super(source, Stream.concat(children.stream(), Stream.of(options)).toList());
        this.inferenceId = parseInferenceId(options, this::defaultInferenceId);
        this.options = options;
    }

    /**
     * Returns the expression representing the {@code inference_id} used by the function.
     *
     * @return the inference ID expression
     */
    public Expression inferenceId() {
        return inferenceId;
    }

    /**
     * Returns the expression representing the options passed to the function.
     *
     * @return the options expression
     */
    public Expression options() {
        return options;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return resolveParams().and(resolveOptions());
    }

    /**
     * Returns the default inference ID expression to use when no {@code inference_id}
     * is specified in the options.
     *
     * @return the default inference ID expression
     */
    protected abstract Expression defaultInferenceId();

    /**
     * When an inference function is resolved it is replaced with a temporary attributes that in an ad-hoc inference command.
     * These attributes need to be cleansed once they are not used anymore.
     *
     * @return the list of temporary attributes
     */
    public abstract List<Attribute> temporaryAttributes();

    /**
     * Resolves the types of the core parameters passed to this function.
     *
     * @return the result of parameter type resolution
     */
    protected abstract TypeResolution resolveParams();

    /**
     * Return the param ordinal of the optional arguments parameters.
     *
     * @return the result of option type resolution
     */
    protected abstract TypeResolutions.ParamOrdinal optionsParamsOrdinal();

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        InferenceFunction that = (InferenceFunction) o;
        return Objects.equals(options, that.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), options);
    }

    protected TypeResolution resolveOptions() {
        TypeResolution resolution = TypeResolutions.isMapExpression(options(), sourceText(), optionsParamsOrdinal());
        if (resolution.unresolved()) {
            return resolution;
        }

        MapExpression options = (MapExpression) options();
        for (Map.Entry<String, Expression> optionEntry : options.keyFoldedMap().entrySet()) {
            for (OptionalArgumentsValidator validator : optionalArgumentsValidators()) {
                if (validator.applyTo(optionEntry.getKey(), optionEntry.getValue())) {
                    TypeResolution optionResolution = validator.resolveOptionValue(
                        optionEntry.getKey(),
                        optionEntry.getValue(),
                        optionsParamsOrdinal()
                    );
                    if (optionResolution.unresolved()) {
                        return optionResolution;
                    }
                    break;
                }
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    protected List<OptionalArgumentsValidator> optionalArgumentsValidators() {
        return DEFAULT_OPTIONAL_ARGUMENTS_VALIDATORS;
    }

    /**
     * Extracts the {@code inference_id} expression from the options.
     * Fallback to the provided inference id if the option is missing.
     *
     * @param options                    the options map expression
     * @param defaultInferenceIdSupplier the supplier for the default inference ID
     * @return the resolved inference ID expression
     */
    private static Expression parseInferenceId(Expression options, Supplier<Expression> defaultInferenceIdSupplier) {
        return readOption("inference_id", options, defaultInferenceIdSupplier);
    }

    /**
     * Reads an option value from a map expression with a fallback to a default value.
     *
     * @param optionName           the name of the option to retrieve
     * @param options              the map expression containing options
     * @param defaultValueSupplier the supplier of the default value
     * @return the option value as an expression or the default if not present
     */
    private static Expression readOption(String optionName, Expression options, Supplier<Expression> defaultValueSupplier) {
        if (options != null && options.dataType() != DataType.NULL && options instanceof MapExpression mapOptions) {
            return mapOptions.getOrDefault(optionName, defaultValueSupplier.get());
        }

        return defaultValueSupplier.get();
    }

    public interface OptionalArgumentsValidator {
        boolean applyTo(String optionName, Expression optionValue);

        TypeResolution resolveOptionValue(String optionName, Expression optionValue, TypeResolutions.ParamOrdinal paramOrdinal);
    }

    public static class InferenceIdOptionalArgumentsValidator implements OptionalArgumentsValidator {
        private InferenceIdOptionalArgumentsValidator() {}

        public boolean applyTo(String optionName, Expression optionValue) {
            return optionName.equals(INFERENCE_ID_OPTION_NAME);
        }

        public TypeResolution resolveOptionValue(String optionName, Expression optionValue, TypeResolutions.ParamOrdinal paramOrdinal) {
            return TypeResolutions.isString(optionValue, optionName, paramOrdinal)
                .and(TypeResolutions.isNotNull(optionValue, optionName, paramOrdinal))
                .and(TypeResolutions.isFoldable(optionValue, optionName, paramOrdinal));
        }
    }
}

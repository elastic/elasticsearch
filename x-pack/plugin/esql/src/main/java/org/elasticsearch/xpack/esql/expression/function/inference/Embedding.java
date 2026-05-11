/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.TIMEOUT_NOT_DETERMINED;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * EMBEDDING function converts input to dense vector embeddings using an inference endpoint with the {@code embedding} task type.
 * It can manage multiple data types and formats (e.g. text, images) based on the options provided
 */
public class Embedding extends InferenceFunction<Embedding> implements OptionalArgument, PostOptimizationVerificationAware {

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Embedding.class).ternary(Embedding::new).name("embedding");

    private static final String OPTION_TYPE = "type";
    private static final String OPTION_TIMEOUT = "timeout";
    private static final Map<String, DataType> ALLOWED_OPTIONS = Map.of(OPTION_TYPE, DataType.KEYWORD, OPTION_TIMEOUT, DataType.KEYWORD);

    private final Expression inferenceId;
    private final Expression inputValue;
    private final Expression inputOptions;

    @FunctionInfo(
        returnType = "dense_vector",
        description = "Generates dense vector embeddings from multimodal input using a specified "
            + "[inference endpoint](docs-content://explore-analyze/elastic-inference/inference-api.md) "
            + "with the {@code embedding} task type. "
            + "Use this function to generate query vectors for KNN searches from multimodal inputs against your vectorized data "
            + "or other dense vector based operations.",
        appliesTo = { @FunctionAppliesTo(version = "9.5.0", lifeCycle = FunctionAppliesToLifecycle.PREVIEW), },
        examples = {
            @Example(
                description = "Generate embeddings using the 'test_dense_inference' inference endpoint, "
                    + "using the default type and format (text):",
                file = "embedding",
                tag = "embedding-knn"
            ),
            @Example(
                description = "Generate embeddings using an inference endpoint, specifying the data type:",
                file = "embedding",
                tag = "embedding-image"
            ) }
    )
    public Embedding(
        Source source,
        @Param(
            name = "value",
            type = { "keyword" },
            description = "Value to generate embeddings from. Must be a non-null literal string value. "
                + "Use data type and format options to specify the content type and format "
                + "(e.g. plain text, base64-encoded images) of the input."
        ) Expression inputValue,
        @Param(
            name = InferenceFunction.INFERENCE_ID_PARAMETER_NAME,
            type = { "keyword" },
            description = "Identifier of an existing inference endpoint that will generate the embeddings. "
                + "The inference endpoint must have the `embedding` task type and should use the same model "
                + "that was used to embed your indexed data.",
            hint = @Param.Hint(
                kind = Param.Hint.Kind.ENTITY,
                entityType = Param.Hint.ENTITY_TYPE.INFERENCE_ENDPOINT,
                constraints = { @Param.Hint.Constraint(name = "task_type", value = "embedding") }
            )
        ) Expression inferenceId,
        @MapParam(
            name = "options",
            description = "(Optional) Options for the input value.",
            params = {
                @MapParam.MapParamEntry(
                    name = "type",
                    type = { "keyword" },
                    description = "Content type of the input (e.g. \"text\", \"image\")."
                ),
                @MapParam.MapParamEntry(
                    name = "timeout",
                    type = { "keyword" },
                    description = "Timeout for the inference request (e.g. \"30s\", \"1m\")."
                ) },
            optional = true
        ) Expression inputOptions
    ) {
        super(source, inputOptions == null ? List.of(inputValue, inferenceId) : List.of(inputValue, inferenceId, inputOptions));
        this.inferenceId = inferenceId;
        this.inputValue = inputValue;
        this.inputOptions = inputOptions;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Must be replaced at the coordinator node and should never be serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("Must be replaced at the coordinator node and should never be serialized");
    }

    public Expression inputText() {
        return inputValue;
    }

    private String optionStringValue(String key) {
        if (inputOptions == null) return null;
        Expression valueExpr = ((MapExpression) inputOptions).get(key);
        if (valueExpr == null) return null;
        return BytesRefs.toString(((Literal) valueExpr).value());
    }

    public org.elasticsearch.inference.DataType inputDataType() {
        String value = optionStringValue(OPTION_TYPE);
        return value == null ? org.elasticsearch.inference.DataType.TEXT : org.elasticsearch.inference.DataType.fromString(value);
    }

    public TimeValue inputTimeout() {
        return TimeValue.parseTimeValue(optionStringValue(OPTION_TIMEOUT), TIMEOUT_NOT_DETERMINED, OPTION_TIMEOUT);
    }

    @Override
    public Expression inferenceId() {
        return inferenceId;
    }

    @Override
    public boolean foldable() {
        return inferenceId.foldable() && inputValue.foldable();
    }

    @Override
    public DataType dataType() {
        return inputValue.dataType() == DataType.NULL ? DataType.NULL : DataType.DENSE_VECTOR;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution textResolution = isNotNull(inputValue, sourceText(), FIRST).and(
            isType(inputValue, DataType.KEYWORD::equals, sourceText(), FIRST, "string")
        );

        if (textResolution.unresolved()) {
            return textResolution;
        }

        TypeResolution inferenceIdResolution = isNotNull(inferenceId, sourceText(), SECOND).and(
            isType(inferenceId, DataType.KEYWORD::equals, sourceText(), SECOND, "string")
        );

        if (inferenceIdResolution.unresolved()) {
            return inferenceIdResolution;
        }

        if (inputOptions != null) {
            TypeResolution optionsResolution = Options.resolve(inputOptions, source(), THIRD, ALLOWED_OPTIONS, optionsMap -> {
                try {
                    inputDataType();
                    inputTimeout();
                } catch (IllegalArgumentException e) {
                    throw new InvalidArgumentException("Invalid options for EMBEDDING: " + e.getMessage());
                }
            });
            if (optionsResolution.unresolved()) {
                return optionsResolution;
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        if (inputValue.foldable() == false) {
            failures.add(Failure.fail(this, "first argument for [" + sourceText() + "] must be a constant string"));
        }
        if (inferenceId.foldable() == false) {
            failures.add(Failure.fail(this, "second argument for [" + sourceText() + "] must be a constant string"));
        }
    }

    @Override
    public TaskType taskType() {
        return TaskType.EMBEDDING;
    }

    @Override
    public Embedding withInferenceResolutionError(String inferenceId, String error) {
        return new Embedding(source(), inputValue, new UnresolvedAttribute(inferenceId().source(), inferenceId, error), inputOptions);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Embedding(source(), newChildren.get(0), newChildren.get(1), newChildren.size() > 2 ? newChildren.get(2) : null);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Embedding::new, inputValue, inferenceId, inputOptions);
    }
}

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
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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

    private static final String OPTION_TYPE = "type";
    private static final String OPTION_FORMAT = "format";
    private static final String OPTION_TIMEOUT = "timeout";
    private static final Map<String, DataType> ALLOWED_OPTIONS = Map.of(
        OPTION_TYPE,
        DataType.KEYWORD,
        OPTION_FORMAT,
        DataType.KEYWORD,
        OPTION_TIMEOUT,
        DataType.KEYWORD
    );

    private final Expression inferenceId;
    private final Expression inputValue;
    private final MapExpression inputOptions;

    private org.elasticsearch.inference.DataType resolvedDataType = org.elasticsearch.inference.DataType.TEXT;
    private DataFormat resolvedDataFormat = DataFormat.TEXT;
    private TimeValue resolvedTimeout = InferenceAction.Request.DEFAULT_TIMEOUT;

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
                description = "Generate embeddings using the 'test_dense_inference' inference endpoint, using defaults:",
                file = "embedding",
                tag = "embedding-knn"
            ),
            @Example(
                description = "Generate embeddings using an inference endpoint, specifying the data type and format:",
                file = "embedding",
                tag = "embedding-base64"
            ) }
    )
    public Embedding(
        Source source,
        @Param(
            name = "value",
            type = { "keyword" },
            description = "Value to generate embeddings from. Must be a non-null literal string value. " +
                "Use data type and format options to specify the content type and format " +
                "(e.g. plain text, base64-encoded images) of the input."
        ) Expression inputValue,
        @Param(
            name = InferenceFunction.INFERENCE_ID_PARAMETER_NAME,
            type = { "keyword" },
            description = "Identifier of an existing inference endpoint that will generate the embeddings. "
                + "The inference endpoint must have the `embedding` task type and should use the same model "
                + "that was used to embed your indexed data.",
            hint = @Param.Hint(
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
                    name = "format",
                    type = { "keyword" },
                    description = "Format of the input content (e.g. \"text\", \"base64\")."
                ),
                @MapParam.MapParamEntry(
                    name = "timeout",
                    type = { "keyword" },
                    description = "Timeout for the inference request (e.g. \"30s\", \"1m\")."
                ) },
            optional = true
        ) MapExpression inputOptions
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

    public org.elasticsearch.inference.DataType inputDataType() {
        return resolvedDataType;
    }

    public DataFormat inputDataFormat() {
        return resolvedDataFormat;
    }

    public TimeValue inputTimeout() {
        return resolvedTimeout;
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
                Object typeValue = optionsMap.get(OPTION_TYPE);
                if (typeValue != null) {
                    try {
                        resolvedDataType = org.elasticsearch.inference.DataType.fromString(BytesRefs.toString(typeValue));
                    } catch (IllegalArgumentException e) {
                        throw new InvalidArgumentException(e.getMessage());
                    }
                }
                Object formatValue = optionsMap.get(OPTION_FORMAT);
                if (formatValue != null) {
                    try {
                        resolvedDataFormat = DataFormat.fromString(BytesRefs.toString(formatValue));
                    } catch (IllegalArgumentException e) {
                        throw new InvalidArgumentException(e.getMessage());
                    }
                }
                Object timeoutValue = optionsMap.get(OPTION_TIMEOUT);
                if (timeoutValue != null) {
                    try {
                        resolvedTimeout = TimeValue.parseTimeValue(BytesRefs.toString(timeoutValue), OPTION_TIMEOUT);
                    } catch (IllegalArgumentException e) {
                        throw new InvalidArgumentException(e.getMessage());
                    }
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
            failures.add(Failure.fail(this, "First argument for EMBEDDING must be a constant string"));
        }
        if (inferenceId.foldable() == false) {
            failures.add(Failure.fail(this, "Second argument for EMBEDDING must be a constant string"));
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
        return new Embedding(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.size() > 2 ? (MapExpression) newChildren.get(2) : null
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Embedding::new, inputValue, inferenceId, inputOptions);
    }

    @Override
    public String toString() {
        return inputOptions == null
            ? "EMBEDDING(" + inputValue + ", " + inferenceId + ")"
            : "EMBEDDING(" + inputValue + ", " + inferenceId + ", " + inputOptions + ")";
    }
}

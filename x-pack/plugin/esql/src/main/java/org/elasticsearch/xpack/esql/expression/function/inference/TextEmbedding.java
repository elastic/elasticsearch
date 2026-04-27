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

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * TEXT_EMBEDDING function converts text to dense vector embeddings using an inference endpoint.
 */
public class TextEmbedding extends InferenceFunction<TextEmbedding> implements OptionalArgument {

    private static final String OPTION_TIMEOUT = "timeout";
    private static final Map<String, DataType> ALLOWED_OPTIONS = Map.of(OPTION_TIMEOUT, DataType.KEYWORD);

    private final Expression inferenceId;
    private final Expression inputText;
    private final Expression inputOptions;

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(TextEmbedding.class)
        .ternary(TextEmbedding::new)
        .name("text_embedding");

    @FunctionInfo(
        returnType = "dense_vector",
        description = "Generates dense vector embeddings from text input using a specified "
            + "[inference endpoint](docs-content://explore-analyze/elastic-inference/inference-api.md). "
            + "Use this function to generate query vectors for KNN searches against your vectorized data "
            + "or others dense vector based operations.",
        appliesTo = {
            @FunctionAppliesTo(version = "9.4.0", lifeCycle = FunctionAppliesToLifecycle.GA),
            @FunctionAppliesTo(version = "9.3.0", lifeCycle = FunctionAppliesToLifecycle.PREVIEW), },
        examples = {
            @Example(
                description = "Generate text embeddings using the 'test_dense_inference' inference endpoint.",
                file = "text-embedding",
                tag = "text-embedding-knn-inline"
            ) }
    )
    public TextEmbedding(
        Source source,
        @Param(
            name = "text",
            type = { "keyword" },
            description = "Text string to generate embeddings from. Must be a non-null literal string value."
        ) Expression inputText,
        @Param(
            name = InferenceFunction.INFERENCE_ID_PARAMETER_NAME,
            type = { "keyword" },
            description = "Identifier of an existing inference endpoint that will generate the embeddings. "
                + "The inference endpoint must have the `text_embedding` task type and should use the same model "
                + "that was used to embed your indexed data.",
            hint = @Param.Hint(
                entityType = Param.Hint.ENTITY_TYPE.INFERENCE_ENDPOINT,
                constraints = { @Param.Hint.Constraint(name = "task_type", value = "text_embedding") }
            )
        ) Expression inferenceId,
        @MapParam(
            name = "options",
            description = "(Optional) Options for the inference request.",
            applies_to = "{\"stack\": \"ga 9.5+\", \"serverless\": \"ga\"}",
            params = {
                @MapParam.MapParamEntry(
                    name = "timeout",
                    type = { "keyword" },
                    description = "Timeout for the inference request (e.g. \"30s\", \"1m\").",
                    applies_to = "{\"stack\": \"ga 9.5+\", \"serverless\": \"ga\"}"
                ) },
            optional = true
        ) Expression inputOptions
    ) {
        super(source, inputOptions == null ? List.of(inputText, inferenceId) : List.of(inputText, inferenceId, inputOptions));
        this.inferenceId = inferenceId;
        this.inputText = inputText;
        this.inputOptions = inputOptions;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    public Expression inputText() {
        return inputText;
    }

    @Override
    public Expression inferenceId() {
        return inferenceId;
    }

    private String optionStringValue(String key) {
        if (inputOptions == null) return null;
        Expression valueExpr = ((MapExpression) inputOptions).get(key);
        if (valueExpr == null) return null;
        return BytesRefs.toString(((Literal) valueExpr).value());
    }

    public TimeValue inputTimeout() {
        String value = optionStringValue(OPTION_TIMEOUT);
        return value == null ? null : TimeValue.parseTimeValue(value, OPTION_TIMEOUT);
    }

    @Override
    public boolean foldable() {
        return inferenceId.foldable() && inputText.foldable();
    }

    @Override
    public DataType dataType() {
        return inputText.dataType() == DataType.NULL ? DataType.NULL : DataType.DENSE_VECTOR;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution textResolution = isNotNull(inputText, sourceText(), FIRST).and(isFoldable(inputText, sourceText(), FIRST))
            .and(isType(inputText, DataType.KEYWORD::equals, sourceText(), FIRST, "string"));

        if (textResolution.unresolved()) {
            return textResolution;
        }

        TypeResolution inferenceIdResolution = isNotNull(inferenceId, sourceText(), SECOND).and(
            isType(inferenceId, DataType.KEYWORD::equals, sourceText(), SECOND, "string")
        ).and(isFoldable(inferenceId, sourceText(), SECOND));

        if (inferenceIdResolution.unresolved()) {
            return inferenceIdResolution;
        }

        if (inputOptions != null) {
            TypeResolution optionsResolution = Options.resolve(inputOptions, source(), THIRD, ALLOWED_OPTIONS, optionsMap -> {
                try {
                    inputTimeout();
                } catch (IllegalArgumentException e) {
                    throw new InvalidArgumentException("Invalid timeout option for TEXT_EMBEDDING: " + e.getMessage());
                }
            });
            if (optionsResolution.unresolved()) {
                return optionsResolution;
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public TaskType taskType() {
        return TaskType.TEXT_EMBEDDING;
    }

    @Override
    public TextEmbedding withInferenceResolutionError(String inferenceId, String error) {
        return new TextEmbedding(source(), inputText, new UnresolvedAttribute(inferenceId().source(), inferenceId, error), inputOptions);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new TextEmbedding(source(), newChildren.get(0), newChildren.get(1), newChildren.size() > 2 ? newChildren.get(2) : null);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TextEmbedding::new, inputText, inferenceId, inputOptions);
    }

    @Override
    public String toString() {
        return "TEXT_EMBEDDING(" + inputText + ", " + inferenceId + ")";
    }
}

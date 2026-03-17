/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.TaskType;
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
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * EMBEDDING function converts text to dense vector embeddings using an inference endpoint with the {@code embedding} task type.
 */
public class Embedding extends InferenceFunction<Embedding> implements OptionalArgument {

    private final Expression inferenceId;
    private final Expression inputText;
    private final MapExpression inputOptions;

    private org.elasticsearch.inference.DataType resolvedDataType = org.elasticsearch.inference.DataType.TEXT;
    private DataFormat resolvedDataFormat = DataFormat.TEXT;

    @FunctionInfo(
        returnType = "dense_vector",
        description = "Generates dense vector embeddings from multimodal input using a specified "
            + "[inference endpoint](docs-content://explore-analyze/elastic-inference/inference-api.md) "
            + "with the {@code embedding} task type. "
            + "Use this function to generate query vectors for KNN searches against your vectorized data "
            + "or other dense vector based operations.",
        appliesTo = {
            @FunctionAppliesTo(version = "9.5.0", lifeCycle = FunctionAppliesToLifecycle.PREVIEW),
        },
        examples = {
            @Example(
                description = "Generate embeddings using the 'test_dense_inference' inference endpoint.",
                file = "embedding",
                tag = "embedding-knn"
            ) }
    )
    public Embedding(
        Source source,
        @Param(
            name = "value",
            type = { "keyword" },
            description = "Value to generate embeddings from. Must be a non-null literal string value."
        ) Expression inputText,
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
                )
            },
            optional = true
        ) MapExpression inputOptions
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

    public MapExpression inputOptions() {
        return inputOptions;
    }

    public org.elasticsearch.inference.DataType inputDataType() {
        return resolvedDataType;
    }

    public DataFormat inputDataFormat() {
        return resolvedDataFormat;
    }

    @Override
    public Expression inferenceId() {
        return inferenceId;
    }

    @Override
    public boolean foldable() {
        if (inferenceId.foldable() == false || inputText.foldable() == false) {
            return false;
        }
        if (inputOptions != null) {
            if (inputOptions.resolved() == false) {
                return false;
            }
            for (Expression e : inputOptions.children()) {
                if (e.foldable() == false) {
                    return false;
                }
            }
        }
        return true;
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
            // Validate keys
            for (String key : inputOptions.keyFoldedMap().keySet()) {
                if ("type".equals(key) == false && "format".equals(key) == false) {
                    return new TypeResolution("Unknown option [" + key + "] in EMBEDDING, valid options are [type, format]");
                }
            }
            // Validate and store "type"
            Expression typeExpr = inputOptions.get("type");
            if (typeExpr instanceof Literal l) {
                try {
                    resolvedDataType = org.elasticsearch.inference.DataType.fromString(BytesRefs.toString(l.value()));
                } catch (IllegalArgumentException e) {
                    return new TypeResolution(e.getMessage());
                }
            }
            // Validate and store "format"
            Expression formatExpr = inputOptions.get("format");
            if (formatExpr instanceof Literal l) {
                try {
                    resolvedDataFormat = DataFormat.fromString(BytesRefs.toString(l.value()));
                } catch (IllegalArgumentException e) {
                    return new TypeResolution(e.getMessage());
                }
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public TaskType taskType() {
        return TaskType.EMBEDDING;
    }

    @Override
    public Embedding withInferenceResolutionError(String inferenceId, String error) {
        return new Embedding(source(), inputText, new UnresolvedAttribute(inferenceId().source(), inferenceId, error), inputOptions);
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
        return NodeInfo.create(this, Embedding::new, inputText, inferenceId, inputOptions);
    }

    @Override
    public String toString() {
        return inputOptions == null
            ? "EMBEDDING(" + inputText + ", " + inferenceId + ")"
            : "EMBEDDING(" + inputText + ", " + inferenceId + ", " + inputOptions + ")";
    }
}

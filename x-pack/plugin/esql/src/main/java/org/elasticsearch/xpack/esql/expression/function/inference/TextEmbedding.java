/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * TEXT_EMBEDDING function converts text to dense vector embeddings using an inference endpoint.
 */
public class TextEmbedding extends InferenceFunction<TextEmbedding> {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TextEmbedding",
        TextEmbedding::new
    );

    private final Expression inferenceId;
    private final Expression inputText;

    @FunctionInfo(
        returnType = "dense_vector",
        description = "Generates dense vector embeddings for text using a specified inference endpoint.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.DEVELOPMENT) },
        preview = true
    )
    public TextEmbedding(
        Source source,
        @Param(name = "text", type = { "keyword", "text" }, description = "Text to embed") Expression inputText,
        @Param(
            name = InferenceFunction.INFERENCE_ID_PARAMETER_NAME,
            type = { "keyword", "text" },
            description = "Identifier of the inference endpoint"
        ) Expression inferenceId
    ) {
        super(source, List.of(inputText, inferenceId));
        this.inferenceId = inferenceId;
        this.inputText = inputText;
    }

    private TextEmbedding(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(inputText);
        out.writeNamedWriteable(inferenceId);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression inputText() {
        return inputText;
    }

    @Override
    public Expression inferenceId() {
        return inferenceId;
    }

    @Override
    public boolean foldable() {
        return inferenceId.foldable() && inputText.foldable();
    }

    @Override
    public DataType dataType() {
        return DataType.DENSE_VECTOR;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution textResolution = isNotNull(inputText, sourceText(), FIRST).and(isFoldable(inputText, sourceText(), FIRST))
            .and(isString(inputText, sourceText(), FIRST));

        if (textResolution.unresolved()) {
            return textResolution;
        }

        TypeResolution inferenceIdResolution = isNotNull(inferenceId, sourceText(), SECOND).and(isString(inferenceId, sourceText(), SECOND))
            .and(isFoldable(inferenceId, sourceText(), SECOND));

        if (inferenceIdResolution.unresolved()) {
            return inferenceIdResolution;
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public TaskType taskType() {
        return TaskType.TEXT_EMBEDDING;
    }

    @Override
    public TextEmbedding withInferenceResolutionError(String inferenceId, String error) {
        return new TextEmbedding(source(), inputText, new UnresolvedAttribute(inferenceId().source(), inferenceId, error));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new TextEmbedding(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TextEmbedding::new, inputText, inferenceId);
    }

    @Override
    public String toString() {
        return "TEXT_EMBEDDING(" + inputText + ", " + inferenceId + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        TextEmbedding textEmbedding = (TextEmbedding) o;
        return Objects.equals(inferenceId, textEmbedding.inferenceId) && Objects.equals(inputText, textEmbedding.inputText);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inferenceId, inputText);
    }
}

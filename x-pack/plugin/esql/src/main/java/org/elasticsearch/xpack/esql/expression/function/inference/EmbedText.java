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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
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
 * EMBED_TEXT function that generates dense vector embeddings for text using a specified inference deployment.
 */
public class EmbedText extends Function implements InferenceFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "EmbedText",
        EmbedText::new
    );

    private final Expression inferenceId;
    private final Expression inputText;

    @FunctionInfo(
        returnType = "dense_vector",
        description = "Generates dense vector embeddings for text using a specified inference deployment.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.DEVELOPMENT) },
        preview = true
    )
    public EmbedText(
        Source source,
        @Param(name = "text", type = { "keyword", "text" }, description = "Text to embed") Expression inputText,
        @Param(
            name = InferenceFunction.INFERENCE_ID_PARAMETER_NAME,
            type = { "keyword", "text" },
            description = "Inference deployment ID"
        ) Expression inferenceId
    ) {
        super(source, List.of(inputText, inferenceId));
        this.inferenceId = inferenceId;
        this.inputText = inputText;
    }

    private EmbedText(StreamInput in) throws IOException {
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
    public DataType dataType() {
        return DataType.DENSE_VECTOR;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution textResolution = isNotNull(inputText, sourceText(), FIRST).and(isFoldable(inferenceId, sourceText(), FIRST))
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
    public boolean foldable() {
        // The function is foldable only if both arguments are foldable
        return inputText.foldable() && inferenceId.foldable();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new EmbedText(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, EmbedText::new, inputText, inferenceId);
    }

    @Override
    public String toString() {
        return "EMBED_TEXT(" + inputText + ", " + inferenceId + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        EmbedText embedText = (EmbedText) o;
        return Objects.equals(inferenceId, embedText.inferenceId) && Objects.equals(inputText, embedText.inputText);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inferenceId, inputText);
    }
}

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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * * A function that embeds input text into a dense vector representation using an inference model.
 */
public class DenseVectorEmbeddingFunction extends InferenceFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TextDenseVectorEmbedding",
        DenseVectorEmbeddingFunction::new
    );

    private final Expression inputText;
    private final Attribute tmpAttribute;

    @FunctionInfo(
        returnType = "dense_vector",
        preview = true,
        description = "Embed input text into a dense vector representation using an inference model.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.DEVELOPMENT) }
    )
    public DenseVectorEmbeddingFunction(
        Source source,
        @Param(name = "inputText", type = { "keyword", "text" }, description = "Input text") Expression inputText,
        @MapParam(
            name = "options",
            params = { @MapParam.MapParamEntry(name = "inference_id", type = "keyword", description = "Inference endpoint to use.") },
            optional = true
        ) Expression options
    ) {
        this(source, inputText, options, new ReferenceAttribute(Source.EMPTY, ENTRY.name + "_" + UUID.randomUUID(), DataType.DOUBLE));
    }

    private DenseVectorEmbeddingFunction(Source source, Expression inputText, Expression options, Attribute tmpAttribute) {
        super(source, List.of(inputText, tmpAttribute), options);
        this.inputText = inputText;
        this.tmpAttribute = tmpAttribute;
    }

    public DenseVectorEmbeddingFunction(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(inputText);
        out.writeNamedWriteable(options());
        out.writeNamedWriteable(tmpAttribute);
    }

    @Override
    public String functionName() {
        super.functionName();
        return getWriteableName();
    }

    @Override
    public DataType dataType() {
        return DataType.DENSE_VECTOR;
    }

    @Override
    public DenseVectorEmbeddingFunction replaceChildren(List<Expression> newChildren) {
        return new DenseVectorEmbeddingFunction(
            source(),
            newChildren.get(0),
            newChildren.size() > 1 ? newChildren.get(1) : null,
            tmpAttribute
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DenseVectorEmbeddingFunction::new, inputText, options(), tmpAttribute);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Literal defaultInferenceId() {
        return Literal.NULL;
    }

    @Override
    public List<Attribute> temporaryAttributes() {
        return List.of(tmpAttribute);
    }

    @Override
    protected TypeResolution resolveParams() {
        return TypeResolutions.isString(inputText, sourceText(), TypeResolutions.ParamOrdinal.FIRST);
    }

    @Override
    protected TypeResolutions.ParamOrdinal optionsParamsOrdinal() {
        return TypeResolutions.ParamOrdinal.SECOND;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        DenseVectorEmbeddingFunction that = (DenseVectorEmbeddingFunction) o;
        return Objects.equals(inputText, that.inputText) && Objects.equals(tmpAttribute, that.tmpAttribute);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inputText, tmpAttribute);
    }
}

package org.elasticsearch.xpack.esql.expression.function.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.*;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class RerankFunction extends InferenceFunction implements OptionalArgument {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Rerank", RerankFunction::new);

    private final Expression query;
    private final Expression field;
    private final Attribute tmpAttribute;

    @FunctionInfo(
        returnType = "double",
        description = "Compute text similarity score using an inference model."
    )
    public RerankFunction(
        Source source,
        @Param(name = "field", type = { "keyword", "text" }, description = "Field used as input of the reranker") Expression field,
        @Param(name = "query", type = { "keyword", "text" }, description = "The query") Expression query,
        @MapParam(
            name = "options",
            params = {
                @MapParam.MapParamEntry(
                    name = "inference_id",
                    type = "keyword",
                    valueHint = { ".rerank-v1-elasticsearch" },
                    description = "Reranker inference endpoint to use."
                )
            },
            optional = true
        ) Expression options
    ) {
        this(source, field, query, options, new ReferenceAttribute(Source.EMPTY, ENTRY.name + "_" + UUID.randomUUID(), DataType.DOUBLE));
    }


    private RerankFunction(Source source, Expression field, Expression query, Expression options, Attribute tmpAttribute) {
        super(source, List.of(field, query), options);
        this.query = query;
        this.field = field;
        this.tmpAttribute = tmpAttribute;
    }

    public RerankFunction(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(query);
        out.writeNamedWriteable(options());
        out.writeNamedWriteable(tmpAttribute);
    }

    @Override
    public String functionName() {
        return getWriteableName();
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new RerankFunction(source(), newChildren.get(0), newChildren.get(1), options(), tmpAttribute);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, RerankFunction::new, query, field, options(), tmpAttribute);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Expression parseInferenceId(Expression options) {
        return readOption("inference_id", TypeResolutions.ParamOrdinal.THIRD, options, defaultInferenceId());
    }

    private Literal defaultInferenceId() {
        return new Literal(Source.EMPTY, Rerank.DEFAULT_INFERENCE_ID, DataType.KEYWORD);
    }

    @Override
    public List<Attribute> temporaryAttributes() {
        return List.of(tmpAttribute);
    }

    @Override
    protected TypeResolution resolveParams() {
        return resolveField().and(resoolveQueru());
    }

    @Override
    protected TypeResolution resolveOptions() {
        return TypeResolution.TYPE_RESOLVED;
    }

    private TypeResolution resolveField() {
        return isString(field, functionName(), TypeResolutions.ParamOrdinal.FIRST)
            .and(isNotNull(field, functionName(), TypeResolutions.ParamOrdinal.FIRST));
    }

    private TypeResolution resoolveQueru() {
        return isString(query, functionName(), TypeResolutions.ParamOrdinal.SECOND)
            .and(isNotNull(query, functionName(), TypeResolutions.ParamOrdinal.SECOND))
            .and(isFoldable(query, functionName(), TypeResolutions.ParamOrdinal.SECOND));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        RerankFunction that = (RerankFunction) o;
        return Objects.equals(query, that.query) && Objects.equals(field, that.field) && Objects.equals(tmpAttribute, that.tmpAttribute);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), query, field, tmpAttribute);
    }
}

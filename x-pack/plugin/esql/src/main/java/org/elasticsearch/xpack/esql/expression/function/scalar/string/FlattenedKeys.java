/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.index.mapper.flattened.FlattenedKeysConfig;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * A {@code flattened} value with only the keys a query keeps, such as {@code KEEP attrs.b*}. It is not a function users call:
 * the analyzer builds it from the patterns of {@code KEEP} and {@code DROP}. Over a {@code flattened} field it is loaded that
 * way on the shard (see {@link FlattenedKeysConfig}); otherwise each value is filtered here.
 */
public class FlattenedKeys extends EsqlScalarFunction implements BlockLoaderExpression, AnyNullIsNull {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FlattenedKeys",
        FlattenedKeys::new
    );

    private final Expression field;
    private final FlattenedKeysConfig keys;

    /**
     * @param includes the patterns of the keys to keep, every key if empty
     * @param excludes the patterns of the keys to drop
     */
    public FlattenedKeys(Source source, Expression field, List<String> includes, List<String> excludes) {
        super(source, List.of(field));
        this.field = field;
        this.keys = new FlattenedKeysConfig(includes, excludes);
    }

    private FlattenedKeys(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readStringCollectionAsImmutableList(),
            in.readStringCollectionAsImmutableList()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeStringCollection(keys.includes());
        out.writeStringCollection(keys.excludes());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression field() {
        return field;
    }

    public List<String> includes() {
        return keys.includes();
    }

    public List<String> excludes() {
        return keys.excludes();
    }

    @Override
    public DataType dataType() {
        return DataType.FLATTENED;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isType(field, t -> t == DataType.FLATTENED, sourceText(), DEFAULT, "flattened");
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FlattenedKeys(source(), newChildren.get(0), keys.includes(), keys.excludes());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FlattenedKeys::new, field, keys.includes(), keys.excludes());
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && keys.equals(((FlattenedKeys) obj).keys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keys);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new FlattenedKeysEvaluator.Factory(source(), toEvaluator.apply(field), keys);
    }

    @Override
    public PushedBlockLoaderExpression tryPushToFieldLoading(SearchStats stats) {
        if (field instanceof FieldAttribute fa && fa.dataType() == DataType.FLATTENED) {
            return new PushedBlockLoaderExpression(fa, keys);
        }
        return null;
    }

    @Evaluator(warnExceptions = IllegalArgumentException.class)
    static void process(BytesRefBlock.Builder builder, BytesRef flattened, @Fixed(includeInToString = false) FlattenedKeysConfig keys) {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, flattened.bytes, flattened.offset, flattened.length);
            XContentBuilder kept = XContentFactory.jsonBuilder()
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("a flattened value must be an object");
            }
            kept.startObject();
            boolean any = false;
            for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                if (token == null) {
                    throw new IllegalArgumentException("invalid JSON input");
                }
                String key = parser.currentName();
                parser.nextToken();
                if (keys.includes(key)) {
                    kept.field(key);
                    kept.copyCurrentStructure(parser);
                    any = true;
                } else {
                    parser.skipChildren();
                }
            }
            kept.endObject();
            if (any) {
                builder.appendBytesRef(BytesReference.bytes(kept).toBytesRef());
            } else {
                builder.appendNull();
            }
        } catch (IOException | XContentParseException e) {
            throw new IllegalArgumentException("invalid JSON input");
        }
    }
}

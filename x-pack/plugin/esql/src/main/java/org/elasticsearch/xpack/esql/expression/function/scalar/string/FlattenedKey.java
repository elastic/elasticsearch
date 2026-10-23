/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * The value of one key of a {@code flattened} value, as a {@code keyword}, or {@code null} without the key. It is not a function
 * users call: the analyzer builds it for a name under a {@code flattened} column that is no field of the indices, such as
 * {@code a.bar} after {@code RENAME attrs AS a}. It reads the value the column holds, so every shard answers it the same way.
 */
public class FlattenedKey extends EsqlScalarFunction implements AnyNullIsNull {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FlattenedKey",
        FlattenedKey::new
    );

    private final Expression field;
    private final String key;

    public FlattenedKey(Source source, Expression field, String key) {
        super(source, List.of(field));
        this.field = field;
        this.key = Objects.requireNonNull(key);
    }

    private FlattenedKey(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeString(key);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression field() {
        return field;
    }

    public String key() {
        return key;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
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
        return new FlattenedKey(source(), newChildren.get(0), key);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FlattenedKey::new, field, key);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && key.equals(((FlattenedKey) obj).key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), key);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new FlattenedKeyEvaluator.Factory(source(), toEvaluator.apply(field), key);
    }

    @Evaluator(warnExceptions = IllegalArgumentException.class)
    static void process(BytesRefBlock.Builder builder, BytesRef flattened, @Fixed String key) {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, flattened.bytes, flattened.offset, flattened.length)
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("a flattened value must be an object");
            }
            List<String> values = new ArrayList<>();
            for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                if (token == null) {
                    throw new IllegalArgumentException("invalid JSON input");
                }
                String name = parser.currentName();
                XContentParser.Token value = parser.nextToken();
                if (name.equals(key) == false) {
                    parser.skipChildren();
                } else if (value == XContentParser.Token.START_ARRAY) {
                    for (XContentParser.Token item = parser.nextToken(); item != XContentParser.Token.END_ARRAY; item = parser
                        .nextToken()) {
                        if (item.isValue()) {
                            values.add(parser.text());
                        } else {
                            parser.skipChildren();
                        }
                    }
                } else if (value.isValue()) {
                    values.add(parser.text());
                } else {
                    parser.skipChildren();
                }
            }
            if (values.isEmpty()) {
                builder.appendNull();
            } else if (values.size() == 1) {
                builder.appendBytesRef(new BytesRef(values.getFirst()));
            } else {
                builder.beginPositionEntry();
                for (String value : values) {
                    builder.appendBytesRef(new BytesRef(value));
                }
                builder.endPositionEntry();
            }
        } catch (IOException | XContentParseException e) {
            throw new IllegalArgumentException("invalid JSON input");
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Internal JSON object overlay used by generated plans. Members from the second object replace members in the first,
 * including explicit nulls and empty strings. This is not JSON Merge Patch: null does not mean deletion.
 * Both inputs and the result use ordinary byte blocks; no label or planner state belongs to the evaluator.
 */
public final class JsonMerge extends EsqlScalarFunction implements AnyNullIsNull, VersionedNamedWriteable {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "JsonMerge",
        JsonMerge::new
    );

    public JsonMerge(Source source, Expression object, Expression updates) {
        super(source, List.of(object, updates));
    }

    private JsonMerge(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (supportsVersion(out.getTransportVersion()) == false) {
            throw new IOException("JSON object edits are not supported by the recipient");
        }
        source().writeTo(out);
        out.writeNamedWriteable(children().get(0));
        out.writeNamedWriteable(children().get(1));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return FieldAttribute.ESQL_PROMQL_LABEL_RECORD;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public boolean foldable() {
        return children().stream().allMatch(Expression::foldable);
    }

    @Override
    protected TypeResolution resolveType() {
        return resolveObjects(children());
    }

    static TypeResolution resolveObjects(List<Expression> children) {
        for (Expression child : children) {
            if (child.resolved() == false) {
                return new TypeResolution("Unresolved children");
            }
            if (DataType.isString(child.dataType()) == false && child.dataType() != DataType.NULL) {
                return new TypeResolution("JSON object operations require string arguments");
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Expression replaceChildren(List<Expression> children) {
        return new JsonMerge(source(), children.get(0), children.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, JsonMerge::new, children().get(0), children().get(1));
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new JsonMergeEvaluator.Factory(source(), toEvaluator.apply(children().get(0)), toEvaluator.apply(children().get(1)));
    }

    @Evaluator(warnExceptions = { IOException.class, IllegalArgumentException.class })
    static BytesRef process(BytesRef object, BytesRef updates) throws IOException {
        Map<String, Object> result = readObject(object);
        result.putAll(readObject(updates));
        return writeObject(result);
    }

    static Map<String, Object> readObject(BytesRef json) throws IOException {
        try (
            var parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json.bytes, json.offset, json.length)
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("expected a JSON object");
            }
            Map<String, Object> result = parser.mapOrdered();
            if (parser.nextToken() != null) {
                throw new IllegalArgumentException("trailing content after JSON object");
            }
            return result;
        } catch (XContentParseException e) {
            throw new IllegalArgumentException("invalid JSON object", e);
        }
    }

    static BytesRef writeObject(Map<String, Object> object) throws IOException {
        try (var builder = XContentFactory.jsonBuilder()) {
            builder.value(ordered(object));
            return BytesReference.bytes(builder).toBytesRef();
        }
    }

    private static Object ordered(Object value) {
        if (value instanceof Map<?, ?> map) {
            var sorted = new TreeMap<String, Object>();
            map.forEach((key, item) -> sorted.put((String) key, ordered(item)));
            return sorted;
        }
        if (value instanceof List<?> list) {
            return list.stream().map(JsonMerge::ordered).toList();
        }
        return value;
    }
}

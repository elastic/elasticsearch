/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
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
import java.util.Objects;
import java.util.Set;

/**
 * Internal JSON projection for generated plans. Removes exact source field paths (including dotted field names),
 * without wildcard matching. Missing fields are a no-op; null and empty values are not removal sentinels.
 * Empty ancestor objects created by a removal are pruned, as in a filtered source.
 */
public final class JsonRemove extends EsqlScalarFunction implements AnyNullIsNull, VersionedNamedWriteable {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "JsonRemove",
        JsonRemove::new
    );

    private final List<String> fields;

    public JsonRemove(Source source, Expression object, List<String> fields) {
        super(source, List.of(object));
        this.fields = List.copyOf(fields);
    }

    private JsonRemove(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readStringCollectionAsList());
    }

    public List<String> fields() {
        return fields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (supportsVersion(out.getTransportVersion()) == false) {
            throw new IOException("JSON object edits are not supported by the recipient");
        }
        source().writeTo(out);
        out.writeNamedWriteable(children().getFirst());
        out.writeStringCollection(fields);
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
        return children().getFirst().foldable();
    }

    @Override
    protected TypeResolution resolveType() {
        return JsonMerge.resolveObjects(children());
    }

    @Override
    public Expression replaceChildren(List<Expression> children) {
        return new JsonRemove(source(), children.getFirst(), fields);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, JsonRemove::new, children().getFirst(), fields);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new JsonRemoveEvaluator.Factory(source(), toEvaluator.apply(children().getFirst()), Set.copyOf(fields));
    }

    @Evaluator(warnExceptions = { IOException.class, IllegalArgumentException.class })
    static BytesRef process(BytesRef object, @Fixed Set<String> fields) throws IOException {
        Map<String, Object> result = JsonMerge.readObject(object);
        remove(result, "", fields);
        return JsonMerge.writeObject(result);
    }

    private static void remove(Map<?, ?> object, String prefix, Set<String> fields) {
        var iterator = object.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            String path = prefix + entry.getKey();
            if (fields.contains(path)) {
                iterator.remove();
            } else if (entry.getValue() instanceof Map<?, ?> nested && nested.isEmpty() == false) {
                remove(nested, path + ".", fields);
                if (nested.isEmpty()) {
                    iterator.remove();
                }
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && fields.equals(((JsonRemove) obj).fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }
}

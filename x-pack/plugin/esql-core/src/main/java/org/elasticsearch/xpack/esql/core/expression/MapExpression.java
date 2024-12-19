/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;

/**
 * Represent a collect of key-value pairs.
 */
public class MapExpression extends Expression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MapExpression",
        MapExpression::readFrom
    );

    private final List<EntryExpression> entries;

    private final Map<Expression, Expression> map;

    public MapExpression(Source source, List<EntryExpression> entries) {
        super(source, entries.stream().map(Expression.class::cast).toList());
        this.entries = entries;
        this.map = entries.stream()
            .collect(Collectors.toMap(EntryExpression::key, EntryExpression::value, (x, y) -> y, LinkedHashMap::new));
    }

    private static MapExpression readFrom(StreamInput in) throws IOException {
        return new MapExpression(
            Source.readFrom((StreamInput & PlanStreamInput) in),
            in.readNamedWriteableCollectionAsList(EntryExpression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteableCollection(entries);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public MapExpression replaceChildren(List<Expression> newChildren) {
        return new MapExpression(source(), newChildren.stream().map(EntryExpression.class::cast).toList());
    }

    @Override
    protected NodeInfo<MapExpression> info() {
        return NodeInfo.create(this, MapExpression::new, entries());
    }

    public List<EntryExpression> entries() {
        return entries;
    }

    public Map<Expression, Expression> map() {
        return map;
    }

    public Expression getKey(String key) {
        for (EntryExpression entry : entries) {
            Expression k = entry.key();
            if (k.foldable()) {
                Object o = k.fold();
                if (o instanceof BytesRef br) {
                    o = br.utf8ToString();
                }
                if (o.toString().equalsIgnoreCase(key)) {
                    return entry.value();
                }
            }
        }
        return null;
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    public DataType dataType() {
        return UNSUPPORTED;
    }

    @Override
    public int hashCode() {
        return Objects.hash(entries);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        MapExpression other = (MapExpression) obj;
        return Objects.equals(entries, other.entries);
    }

    @Override
    public String toString() {
        String str = entries.stream().map(String::valueOf).collect(Collectors.joining(", "));
        return "{ " + str + " }";
    }
}

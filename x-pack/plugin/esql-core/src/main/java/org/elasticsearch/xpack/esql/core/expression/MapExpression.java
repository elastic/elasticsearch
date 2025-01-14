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
import java.util.ArrayList;
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

    private final List<EntryExpression> entryExpressions;

    private final Map<Expression, Expression> map;

    private final Map<Object, Expression> keyFoldedMap;

    public MapExpression(Source source, List<Expression> entries) {
        super(source, entries);
        int entryCount = entries.size() / 2;
        this.entryExpressions = new ArrayList<>(entryCount);
        this.map = new LinkedHashMap<>(entryCount);
        // create a map with key folded and source removed to make the retrieval of value easier
        this.keyFoldedMap = new LinkedHashMap<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            Expression key = entries.get(i * 2);
            Expression value = entries.get(i * 2 + 1);
            entryExpressions.add(new EntryExpression(key.source(), key, value));
            map.put(key, value);
            if (key instanceof Literal l) {
                this.keyFoldedMap.put(l.value(), value);
            }
        }
    }

    private static MapExpression readFrom(StreamInput in) throws IOException {
        return new MapExpression(
            Source.readFrom((StreamInput & PlanStreamInput) in),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteableCollection(children());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public MapExpression replaceChildren(List<Expression> newChildren) {
        return new MapExpression(source(), newChildren);
    }

    @Override
    protected NodeInfo<MapExpression> info() {
        return NodeInfo.create(this, MapExpression::new, children());
    }

    public List<EntryExpression> entryExpressions() {
        return entryExpressions;
    }

    public Map<Expression, Expression> map() {
        return map;
    }

    public Map<Object, Expression> keyFoldedMap() {
        return keyFoldedMap;
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
        return Objects.hash(entryExpressions);
    }

    public Expression get(Object key) {
        if (key instanceof Expression) {
            return map.get(key);
        } else {
            // the key(literal) could be converted to BytesRef by ConvertStringToByteRef
            return keyFoldedMap.containsKey(key) ? keyFoldedMap.get(key) : keyFoldedMap.get(new BytesRef(key.toString()));
        }
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
        return Objects.equals(entryExpressions, other.entryExpressions);
    }

    @Override
    public String toString() {
        String str = entryExpressions.stream().map(String::valueOf).collect(Collectors.joining(", "));
        return "{ " + str + " }";
    }
}

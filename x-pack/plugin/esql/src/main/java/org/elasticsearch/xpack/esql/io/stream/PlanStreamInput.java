/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry.PlanNamedReader;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry.PlanReader;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.LongFunction;
import java.util.function.Supplier;

/**
 * A customized stream input used to deserialize ESQL physical plan fragments. Complements stream
 * input with methods that read plan nodes, Attributes, Expressions, etc.
 */
public final class PlanStreamInput extends NamedWriteableAwareStreamInput {

    /**
     * A Mapper of stream named id, represented as a primitive long value, to NameId instance.
     * The no-args NameId constructor is used for absent entries, as it will automatically select
     * and increment an id from the global counter, thus avoiding potential conflicts between the
     * id in the stream and id's during local re-planning on the data node.
     */
    static final class NameIdMapper implements LongFunction<NameId> {
        final Map<Long, NameId> seen = new HashMap<>();

        @Override
        public NameId apply(long streamNameId) {
            return seen.computeIfAbsent(streamNameId, k -> new NameId());
        }
    }

    private static final Supplier<LongFunction<NameId>> DEFAULT_NAME_ID_FUNC = NameIdMapper::new;

    private final PlanNameRegistry registry;

    // hook for nameId, where can cache and map, for now just return a NameId of the same long value.
    private final LongFunction<NameId> nameIdFunction;

    private final EsqlConfiguration configuration;

    public PlanStreamInput(
        StreamInput streamInput,
        PlanNameRegistry registry,
        NamedWriteableRegistry namedWriteableRegistry,
        EsqlConfiguration configuration
    ) {
        super(streamInput, namedWriteableRegistry);
        this.registry = registry;
        this.configuration = configuration;
        this.nameIdFunction = DEFAULT_NAME_ID_FUNC.get();
    }

    NameId nameIdFromLongValue(long value) {
        return nameIdFunction.apply(value);
    }

    DataType dataTypeFromTypeName(String typeName) throws IOException {
        DataType dataType;
        if (typeName.equalsIgnoreCase(EsQueryExec.DOC_DATA_TYPE.name())) {
            dataType = EsQueryExec.DOC_DATA_TYPE;
        } else {
            dataType = EsqlDataTypes.fromTypeName(typeName);
        }
        if (dataType == null) {
            throw new IOException("Unknown DataType for type name: " + typeName);
        }
        return dataType;
    }

    public LogicalPlan readLogicalPlanNode() throws IOException {
        return readNamed(LogicalPlan.class);
    }

    public PhysicalPlan readPhysicalPlanNode() throws IOException {
        return readNamed(PhysicalPlan.class);
    }

    public Source readSource() throws IOException {
        boolean hasSource = readBoolean();
        if (hasSource) {
            int line = readInt();
            int column = readInt();
            int length = readInt();
            int charPositionInLine = column - 1;
            return new Source(new Location(line, charPositionInLine), sourceText(configuration.query(), line, column, length));
        }
        return Source.EMPTY;
    }

    private static String sourceText(String query, int line, int column, int length) {
        if (line <= 0 || column <= 0 || query.isEmpty()) {
            return StringUtils.EMPTY;
        }
        int offset = textOffset(query, line, column);
        if (offset + length > query.length()) {
            throw new EsqlIllegalArgumentException(
                "location [@" + line + ":" + column + "] and length [" + length + "] overrun query size [" + query.length() + "]"
            );
        }
        return query.substring(offset, offset + length);
    }

    private static int textOffset(String query, int line, int column) {
        int offset = 0;
        if (line > 1) {
            String[] lines = query.split("\n");
            if (line > lines.length) {
                throw new EsqlIllegalArgumentException(
                    "line location [" + line + "] higher than max [" + lines.length + "] in query [" + query + "]"
                );
            }
            for (int i = 0; i < line - 1; i++) {
                offset += lines[i].length() + 1; // +1 accounts for the removed \n
            }
        }
        offset += column - 1; // -1 since column is 1-based indexed
        return offset;
    }

    public Expression readExpression() throws IOException {
        return readNamed(Expression.class);
    }

    public NamedExpression readNamedExpression() throws IOException {
        return readNamed(NamedExpression.class);
    }

    public Attribute readAttribute() throws IOException {
        return readNamed(Attribute.class);
    }

    public EsField readEsFieldNamed() throws IOException {
        return readNamed(EsField.class);
    }

    public <T> T readNamed(Class<T> type) throws IOException {
        String name = readString();
        @SuppressWarnings("unchecked")
        PlanReader<T> reader = (PlanReader<T>) registry.getReader(type, name);
        if (reader instanceof PlanNamedReader<T> namedReader) {
            return namedReader.read(this, name);
        } else {
            return reader.read(this);
        }
    }

    public <T> T readOptionalNamed(Class<T> type) throws IOException {
        if (readBoolean()) {
            T t = readNamed(type);
            if (t == null) {
                throwOnNullOptionalRead(type);
            }
            return t;
        } else {
            return null;
        }
    }

    public <T> T readOptionalWithReader(PlanReader<T> reader) throws IOException {
        if (readBoolean()) {
            T t = reader.read(this);
            if (t == null) {
                throwOnNullOptionalRead(reader);
            }
            return t;
        } else {
            return null;
        }
    }

    public AttributeSet readAttributeSet(Writeable.Reader<Attribute> reader) throws IOException {
        int count = readArraySize();
        if (count == 0) {
            return new AttributeSet();
        }
        Collection<Attribute> builder = new HashSet<>();
        for (int i = 0; i < count; i++) {
            builder.add(reader.read(this));
        }
        return new AttributeSet(builder);
    }

    public EsqlConfiguration configuration() throws IOException {
        return configuration;
    }

    static void throwOnNullOptionalRead(Class<?> type) throws IOException {
        final IOException e = new IOException("read optional named returned null which is not allowed, type:" + type);
        assert false : e;
        throw e;
    }

    static void throwOnNullOptionalRead(PlanReader<?> reader) throws IOException {
        final IOException e = new IOException("read optional named returned null which is not allowed, reader:" + reader);
        assert false : e;
        throw e;
    }
}

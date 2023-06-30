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
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry.PlanNamedReader;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry.PlanReader;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.AggregateMapper;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.function.LongFunction;

/**
 * A customized stream input used to deserialize ESQL physical plan fragments. Complements stream
 * input with methods that read plan nodes, Attributes, Expressions, etc.
 */
public final class PlanStreamInput extends NamedWriteableAwareStreamInput {

    private static final LongFunction<NameId> DEFAULT_NAME_ID_FUNC = NameId::new;

    private final PlanNameRegistry registry;

    // hook for nameId, where can cache and map, for now just return a NameId of the same long value.
    private final LongFunction<NameId> nameIdFunction;

    private EsqlConfiguration configuration;

    public PlanStreamInput(
        StreamInput streamInput,
        PlanNameRegistry registry,
        NamedWriteableRegistry namedWriteableRegistry,
        EsqlConfiguration configuration
    ) {
        this(streamInput, registry, namedWriteableRegistry, configuration, DEFAULT_NAME_ID_FUNC);
    }

    public PlanStreamInput(
        StreamInput streamInput,
        PlanNameRegistry registry,
        NamedWriteableRegistry namedWriteableRegistry,
        EsqlConfiguration configuration,
        LongFunction<NameId> nameIdFunction
    ) {
        super(streamInput, namedWriteableRegistry);
        this.registry = registry;
        this.nameIdFunction = nameIdFunction;
        this.configuration = configuration;
    }

    NameId nameIdFromLongValue(long value) {
        return nameIdFunction.apply(value);
    }

    DataType dataTypeFromTypeName(String typeName) throws IOException {
        DataType dataType;
        if (typeName.equalsIgnoreCase(EsQueryExec.DOC_DATA_TYPE.name())) {
            dataType = EsQueryExec.DOC_DATA_TYPE;
        } else if (typeName.equalsIgnoreCase(AggregateMapper.AGG_STATE_TYPE.name())) {
            dataType = AggregateMapper.AGG_STATE_TYPE;
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

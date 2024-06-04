/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBigArrayBlock;
import org.elasticsearch.compute.data.DoubleBigArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.LongBigArrayBlock;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry.PlanWriter;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * A customized stream output used to serialize ESQL physical plan fragments. Complements stream
 * output with methods that write plan nodes, Attributes, Expressions, etc.
 */
public final class PlanStreamOutput extends StreamOutput {

    /**
     * Cache of written blocks. We use an {@link IdentityHashMap} for this
     * because calculating the {@link Object#hashCode} of a {@link Block}
     * is slow. And so is {@link Object#equals}. So, instead we just use
     * object identity.
     */
    private final Map<Block, BytesReference> cachedBlocks = new IdentityHashMap<>();

    private final StreamOutput delegate;
    private final PlanNameRegistry registry;

    private final Function<Class<?>, String> nameSupplier;

    private int nextCachedBlock = 0;

    public PlanStreamOutput(StreamOutput delegate, PlanNameRegistry registry, @Nullable EsqlConfiguration configuration)
        throws IOException {
        this(delegate, registry, configuration, PlanNamedTypes::name);
    }

    public PlanStreamOutput(
        StreamOutput delegate,
        PlanNameRegistry registry,
        @Nullable EsqlConfiguration configuration,
        Function<Class<?>, String> nameSupplier
    ) throws IOException {
        this.delegate = delegate;
        this.registry = registry;
        this.nameSupplier = nameSupplier;
        if (configuration != null) {
            for (Map.Entry<String, Map<String, Column>> table : configuration.tables().entrySet()) {
                for (Map.Entry<String, Column> column : table.getValue().entrySet()) {
                    cachedBlocks.put(column.getValue().values(), fromConfigKey(table.getKey(), column.getKey()));
                }
            }
        }
    }

    public void writeLogicalPlanNode(LogicalPlan logicalPlan) throws IOException {
        assert logicalPlan.children().size() <= 1;
        writeNamed(LogicalPlan.class, logicalPlan);
    }

    public void writePhysicalPlanNode(PhysicalPlan physicalPlan) throws IOException {
        assert physicalPlan.children().size() <= 1;
        writeNamed(PhysicalPlan.class, physicalPlan);
    }

    public void writeOptionalPhysicalPlanNode(PhysicalPlan physicalPlan) throws IOException {
        if (physicalPlan == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writePhysicalPlanNode(physicalPlan);
        }
    }

    public void writeExpression(Expression expression) throws IOException {
        writeNamed(Expression.class, expression);
    }

    public void writeNamedExpression(NamedExpression namedExpression) throws IOException {
        writeNamed(NamedExpression.class, namedExpression);
    }

    public void writeOptionalExpression(Expression expression) throws IOException {
        if (expression == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeExpression(expression);
        }
    }

    public <T> void writeNamed(Class<T> type, T value) throws IOException {
        String name = nameSupplier.apply(value.getClass());
        @SuppressWarnings("unchecked")
        PlanWriter<T> writer = (PlanWriter<T>) registry.getWriter(type, name);
        writeString(name);
        writer.write(this, value);
    }

    @Override
    public void writeByte(byte b) throws IOException {
        delegate.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        delegate.writeBytes(b, offset, length);
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public TransportVersion getTransportVersion() {
        return delegate.getTransportVersion();
    }

    @Override
    public void setTransportVersion(TransportVersion version) {
        delegate.setTransportVersion(version);
        super.setTransportVersion(version);
    }

    /**
     * Write a {@link Block} as part of the plan.
     * <p>
     *     These {@link Block}s are not tracked by {@link BlockFactory} and closing them
     *     does nothing so they should be small. We do make sure not to send duplicates,
     *     reusing blocks sent as part of the {@link EsqlConfiguration#tables()} if
     *     possible, otherwise sending a {@linkplain Block} inline.
     * </p>
     */
    public void writeCachedBlock(Block block) throws IOException {
        assert block instanceof LongBigArrayBlock == false : "BigArrays not supported because we don't close";
        assert block instanceof IntBigArrayBlock == false : "BigArrays not supported because we don't close";
        assert block instanceof DoubleBigArrayBlock == false : "BigArrays not supported because we don't close";
        assert block instanceof BooleanBigArrayBlock == false : "BigArrays not supported because we don't close";
        BytesReference key = cachedBlocks.get(block);
        if (key != null) {
            key.writeTo(this);
            return;
        }
        writeByte(NEW_BLOCK_KEY);
        writeVInt(nextCachedBlock);
        cachedBlocks.put(block, fromPreviousKey(nextCachedBlock));
        writeNamedWriteable(block);
        nextCachedBlock++;
    }

    /**
     * The byte representing a {@link Block} sent for the first time. The byte
     * will be followed by a {@link StreamOutput#writeVInt} encoded identifier
     * and then the contents of the {@linkplain Block} will immediately follow
     * this byte.
     */
    static final byte NEW_BLOCK_KEY = 0;

    /**
     * The byte representing a {@link Block} that has previously been sent.
     * This byte will be followed up a {@link StreamOutput#writeVInt} encoded
     * identifier pointing to the block to read.
     */
    static final byte FROM_PREVIOUS_KEY = 1;

    /**
     * The byte representing a {@link Block} that was part of the
     * {@link EsqlConfiguration#tables()} map. It is followed a string for
     * the table name and then a string for the column name.
     */
    static final byte FROM_CONFIG_KEY = 2;

    /**
     * Build the key for reading a {@link Block} from the cache of previously
     * received {@linkplain Block}s.
     */
    static BytesReference fromPreviousKey(int id) throws IOException {
        try (BytesStreamOutput key = new BytesStreamOutput()) {
            key.writeByte(FROM_PREVIOUS_KEY);
            key.writeVInt(id);
            return key.bytes();
        }
    }

    /**
     * Build the key for reading a {@link Block} from the {@link EsqlConfiguration}.
     * This is important because some operations like {@code LOOKUP} frequently read
     * {@linkplain Block}s directly from the configuration.
     * <p>
     *     It'd be possible to implement this by adding all of the Blocks as "previous"
     *     keys in the constructor and never use this construct at all, but that'd
     *     require there be a consistent ordering of Blocks there. We could make one,
     *     but I'm afraid that'd be brittle as we evolve the code. It'd make wire
     *     compatibility difficult. This signal is much simpler to deal with even though
     *     it is more bytes over the wire.
     * </p>
     */
    static BytesReference fromConfigKey(String table, String column) throws IOException {
        try (BytesStreamOutput key = new BytesStreamOutput()) {
            key.writeByte(FROM_CONFIG_KEY);
            key.writeString(table);
            key.writeString(column);
            return key.bytes();
        }
    }
}

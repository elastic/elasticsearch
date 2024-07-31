/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBigArrayBlock;
import org.elasticsearch.compute.data.DoubleBigArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.LongBigArrayBlock;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry.PlanNamedReader;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry.PlanReader;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongFunction;

/**
 * A customized stream input used to deserialize ESQL physical plan fragments. Complements stream
 * input with methods that read plan nodes, Attributes, Expressions, etc.
 */
public final class PlanStreamInput extends NamedWriteableAwareStreamInput
    implements
        org.elasticsearch.xpack.esql.core.util.PlanStreamInput {

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

    private final Map<Integer, Block> cachedBlocks = new HashMap<>();

    private Attribute[] attributesCache = new Attribute[64];

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
        this.nameIdFunction = new NameIdMapper();
    }

    public LogicalPlan readLogicalPlanNode() throws IOException {
        return readNamed(LogicalPlan.class);
    }

    public PhysicalPlan readPhysicalPlanNode() throws IOException {
        return readNamed(PhysicalPlan.class);
    }

    public PhysicalPlan readOptionalPhysicalPlanNode() throws IOException {
        return readOptionalNamed(PhysicalPlan.class);
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

    public EsqlConfiguration configuration() throws IOException {
        return configuration;
    }

    /**
     * Read a {@link Block} as part of the plan.
     * <p>
     *     These {@link Block}s are not tracked by {@link BlockFactory} and closing them
     *     does nothing so they should be small. We do make sure not to send duplicates,
     *     reusing blocks sent as part of the {@link EsqlConfiguration#tables()} if
     *     possible, otherwise sending a {@linkplain Block} inline.
     * </p>
     */
    public Block readCachedBlock() throws IOException {
        byte key = readByte();
        Block block = switch (key) {
            case PlanStreamOutput.NEW_BLOCK_KEY -> {
                int id = readVInt();
                // TODO track blocks read over the wire.... Or slice them from BigArrays? Something.
                Block b = new BlockStreamInput(
                    this,
                    new BlockFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), BigArrays.NON_RECYCLING_INSTANCE)
                ).readNamedWriteable(Block.class);
                cachedBlocks.put(id, b);
                yield b;
            }
            case PlanStreamOutput.FROM_PREVIOUS_KEY -> cachedBlocks.get(readVInt());
            case PlanStreamOutput.FROM_CONFIG_KEY -> {
                String tableName = readString();
                Map<String, Column> table = configuration.tables().get(tableName);
                if (table == null) {
                    throw new IOException("can't find table [" + tableName + "]");
                }
                String columnName = readString();
                Column column = table.get(columnName);
                if (column == null) {
                    throw new IOException("can't find column[" + columnName + "]");
                }
                yield column.values();
            }
            default -> throw new IOException("invalid encoding for Block");
        };
        assert block instanceof LongBigArrayBlock == false : "BigArrays not supported because we don't close";
        assert block instanceof IntBigArrayBlock == false : "BigArrays not supported because we don't close";
        assert block instanceof DoubleBigArrayBlock == false : "BigArrays not supported because we don't close";
        assert block instanceof BooleanBigArrayBlock == false : "BigArrays not supported because we don't close";
        return block;
    }

    /**
     * Read an array of {@link Block}s as part of the plan.
     * <p>
     *     These {@link Block}s are not tracked by {@link BlockFactory} and closing them
     *     does nothing so they should be small. We do make sure not to send duplicates,
     *     reusing blocks sent as part of the {@link EsqlConfiguration#tables()} if
     *     possible, otherwise sending a {@linkplain Block} inline.
     * </p>
     */
    public Block[] readCachedBlockArray() throws IOException {
        int len = readArraySize();
        if (len == 0) {
            return BlockUtils.NO_BLOCKS;
        }
        Block[] blocks = new Block[len];
        try {
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = readCachedBlock();
            }
            return blocks;
        } finally {
            if (blocks[blocks.length - 1] == null) {
                // Wasn't successful reading all blocks
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    @Override
    public String sourceText() {
        return configuration.query();
    }

    static void throwOnNullOptionalRead(Class<?> type) throws IOException {
        final IOException e = new IOException("read optional named returned null which is not allowed, type:" + type);
        assert false : e;
        throw e;
    }

    @Override
    public NameId mapNameId(long l) {
        return nameIdFunction.apply(l);
    }

    /**
     * @param constructor the constructor needed to build the actual attribute when read from the wire
     * @throws IOException
     */
    @Override
    @SuppressWarnings("unchecked")
    public <A extends Attribute> A readAttributeWithCache(CheckedFunction<StreamInput, A, IOException> constructor) throws IOException {
        if (getTransportVersion().onOrAfter(TransportVersions.ESQL_ATTRIBUTE_CACHED_SERIALIZATION)) {
            // it's safe to cast to int, since the max value for this is {@link PlanStreamOutput#MAX_SERIALIZED_ATTRIBUTES}
            int cacheId = Math.toIntExact(readZLong());
            if (cacheId < 0) {
                cacheId = -1 - cacheId;
                Attribute result = constructor.apply(this);
                cacheAttribute(cacheId, result);
                return (A) result;
            } else {
                return (A) attributeFromCache(cacheId);
            }
        } else {
            return constructor.apply(this);
        }
    }

    private Attribute attributeFromCache(int id) throws IOException {
        if (attributesCache[id] == null) {
            throw new IOException("Attribute ID not found in serialization cache [" + id + "]");
        }
        return attributesCache[id];
    }

    /**
     * Add and attribute to the cache, based on the serialization ID generated by {@link PlanStreamOutput}
     * @param id The ID that will reference the attribute. Generated  at serialization time
     * @param attr The attribute to cache
     */
    private void cacheAttribute(int id, Attribute attr) {
        assert id >= 0;
        if (id >= attributesCache.length) {
            attributesCache = ArrayUtil.grow(attributesCache);
        }
        attributesCache[id] = attr;
    }
}

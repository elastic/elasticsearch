/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
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
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.util.PlanStreamOutput.writeCachedStringWithVersionCheck;

/**
 * A customized stream output used to serialize ESQL physical plan fragments. Complements stream
 * output with methods that write plan nodes, Attributes, Expressions, etc.
 */
public final class PlanStreamOutput extends StreamOutput implements org.elasticsearch.xpack.esql.core.util.PlanStreamOutput {

    /**
     * max number of attributes that can be cached for serialization
     * <p>
     * TODO should this be a cluster setting...?
     */
    protected static final int MAX_SERIALIZED_ATTRIBUTES = 1_000_000;

    /**
     * Cache of written blocks. We use an {@link IdentityHashMap} for this
     * because calculating the {@link Object#hashCode} of a {@link Block}
     * is slow. And so is {@link Object#equals}. So, instead we just use
     * object identity.
     */
    private final Map<Block, BytesReference> cachedBlocks = new IdentityHashMap<>();

    /**
     * Cache for field attributes.
     * Field attributes can be a significant part of the query execution plan, especially
     * for queries like `from *`, that can have thousands of output columns.
     * Attributes can be shared by many plan nodes (eg. ExcahngeSink output, Project output, EsRelation fields);
     * in addition, multiple Attributes can share the same parent field.
     * This cache allows to send each attribute only once; from the second occurrence, only an id will be sent
     */
    protected final Map<Attribute, Integer> cachedAttributes = new IdentityHashMap<>();

    /**
     * Cache for EsFields.
     */
    protected final Map<EsField, Integer> cachedEsFields = new IdentityHashMap<>();

    protected final Map<String, Integer> stringCache = new HashMap<>();

    private final StreamOutput delegate;

    private int nextCachedBlock = 0;

    private final int maxSerializedAttributes;

    public PlanStreamOutput(StreamOutput delegate, @Nullable Configuration configuration) throws IOException {
        this(delegate, configuration, MAX_SERIALIZED_ATTRIBUTES);
    }

    public PlanStreamOutput(StreamOutput delegate, @Nullable Configuration configuration, int maxSerializedAttributes) throws IOException {
        this.delegate = delegate;
        if (configuration != null) {
            for (Map.Entry<String, Map<String, Column>> table : configuration.tables().entrySet()) {
                for (Map.Entry<String, Column> column : table.getValue().entrySet()) {
                    cachedBlocks.put(column.getValue().values(), fromConfigKey(table.getKey(), column.getKey()));
                }
            }
        }
        this.maxSerializedAttributes = maxSerializedAttributes;
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
        stringCache.clear();
        cachedEsFields.clear();
        cachedAttributes.clear();
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
     * These {@link Block}s are not tracked by {@link BlockFactory} and closing them
     * does nothing so they should be small. We do make sure not to send duplicates,
     * reusing blocks sent as part of the {@link Configuration#tables()} if
     * possible, otherwise sending a {@linkplain Block} inline.
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
        Block.writeTypedBlock(block, this);
        nextCachedBlock++;
    }

    @Override
    public boolean writeAttributeCacheHeader(Attribute attribute) throws IOException {
        if (getTransportVersion().onOrAfter(TransportVersions.V_8_15_2)) {
            Integer cacheId = attributeIdFromCache(attribute);
            if (cacheId != null) {
                writeZLong(cacheId);
                return false;
            }

            cacheId = cacheAttribute(attribute);
            writeZLong(-1 - cacheId);
        }
        return true;
    }

    private Integer attributeIdFromCache(Attribute attr) {
        return cachedAttributes.get(attr);
    }

    private int cacheAttribute(Attribute attr) {
        if (cachedAttributes.containsKey(attr)) {
            throw new IllegalArgumentException("Attribute already present in the serialization cache [" + attr + "]");
        }
        int id = cachedAttributes.size();
        if (id >= maxSerializedAttributes) {
            throw new InvalidArgumentException("Limit of the number of serialized attributes exceeded [{}]", maxSerializedAttributes);
        }
        cachedAttributes.put(attr, id);
        return id;
    }

    @Override
    public boolean writeEsFieldCacheHeader(EsField field) throws IOException {
        if (getTransportVersion().onOrAfter(TransportVersions.V_8_15_2)) {
            Integer cacheId = esFieldIdFromCache(field);
            if (cacheId != null) {
                writeZLong(cacheId);
                return false;
            }

            cacheId = cacheEsField(field);
            writeZLong(-1 - cacheId);
        }
        writeCachedStringWithVersionCheck(this, field.getWriteableName());
        return true;
    }

    /**
     * Writes a string caching it, ie. the second time the same string is written, only a small, numeric ID will be sent.
     * This should be used only to serialize recurring strings.
     *
     * Values serialized with this method have to be deserialized with {@link PlanStreamInput#readCachedString()}
     */
    @Override
    public void writeCachedString(String string) throws IOException {
        Integer cacheId = stringCache.get(string);
        if (cacheId != null) {
            writeZLong(cacheId);
            return;
        }
        cacheId = stringCache.size();
        if (cacheId >= maxSerializedAttributes) {
            throw new InvalidArgumentException("Limit of the number of serialized strings exceeded [{}]", maxSerializedAttributes);
        }
        stringCache.put(string, cacheId);

        writeZLong(-1 - cacheId);
        writeString(string);
    }

    @Override
    public void writeOptionalCachedString(String str) throws IOException {
        if (str == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeCachedString(str);
        }
    }

    private Integer esFieldIdFromCache(EsField field) {
        return cachedEsFields.get(field);
    }

    private int cacheEsField(EsField attr) {
        if (cachedEsFields.containsKey(attr)) {
            throw new IllegalArgumentException("EsField already present in the serialization cache [" + attr + "]");
        }
        int id = cachedEsFields.size();
        if (id >= maxSerializedAttributes) {
            throw new InvalidArgumentException("Limit of the number of serialized EsFields exceeded [{}]", maxSerializedAttributes);
        }
        cachedEsFields.put(attr, id);
        return id;
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
     * {@link Configuration#tables()} map. It is followed a string for
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
     * Build the key for reading a {@link Block} from the {@link Configuration}.
     * This is important because some operations like {@code LOOKUP} frequently read
     * {@linkplain Block}s directly from the configuration.
     * <p>
     * It'd be possible to implement this by adding all of the Blocks as "previous"
     * keys in the constructor and never use this construct at all, but that'd
     * require there be a consistent ordering of Blocks there. We could make one,
     * but I'm afraid that'd be brittle as we evolve the code. It'd make wire
     * compatibility difficult. This signal is much simpler to deal with even though
     * it is more bytes over the wire.
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

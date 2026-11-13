/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads the objects of one nested field from the hidden child documents Lucene stores before each parent. Each
 * object is the map that decoding {@code _source} would give, so the rest of the expansion does not depend on where
 * the objects came from. Values come from doc values, like an ordinary field, so arrays are in doc-values order.
 * Holds per-segment iterators, so each operator needs its own instance.
 */
public final class NestedIndexObjects {
    /**
     * One shard's view of a nested field: the reader, the filters selecting parents and this field's children, and
     * each child field's doc values, {@code null} where the shard does not map that field.
     */
    public record Shard(IndexReader reader, BitSetProducer parents, @Nullable BitSetProducer children, List<IndexFieldData<?>> fields) {}

    /** Reads one child field at increasing doc ids of one segment. */
    private interface Values {
        @Nullable
        Object read(int doc) throws IOException;
    }

    private final IndexedByShardId<Shard> shards;
    private final List<String> paths;
    private final Values[] values;
    private int shard = -1;
    private int segment = -1;
    private int lastDoc = -1;

    NestedIndexObjects(IndexedByShardId<Shard> shards, List<String> paths) {
        this.shards = shards;
        this.paths = paths;
        this.values = new Values[paths.size()];
    }

    /**
     * Each shard's view of the nested field {@code path} with the child fields {@code paths}, relative to it. Returns
     * {@code null}, meaning the values must come from {@code _source}, when no shards are local or a shard maps one of
     * those fields without doc values.
     */
    @Nullable
    public static IndexedByShardId<Shard> shards(
        @Nullable IndexedByShardId<? extends EsPhysicalOperationProviders.ShardContext> shardContexts,
        String path,
        List<String> paths
    ) {
        if (shardContexts == null || shardContexts.isEmpty()) {
            return null;
        }
        List<String> fields = paths.stream().map(p -> path + "." + p).toList();
        for (var shard : shardContexts.iterable()) {
            if (shard instanceof EsPhysicalOperationProviders.DefaultShardContext == false || withoutDocValues(shard, fields) != null) {
                return null;
            }
        }
        return shardContexts.map(shard -> {
            String missing = withoutDocValues(shard, fields);
            if (missing != null) {
                throw new IllegalStateException("nested child field [" + missing + "] has no doc values on a later shard");
            }
            SearchExecutionContext context = ((EsPhysicalOperationProviders.DefaultShardContext) shard).executionContext();
            NestedObjectMapper nested = context.nestedLookup().getNestedMappers().get(path);
            List<IndexFieldData<?>> data = new ArrayList<>(fields.size());
            for (String field : fields) {
                MappedFieldType type = shard.fieldType(field);
                data.add(type == null || nested == null ? null : context.getForField(type, MappedFieldType.FielddataOperation.SEARCH));
            }
            return new Shard(
                shard.searcher().getIndexReader(),
                context.bitsetFilter(Queries.newNonNestedFilter(context.indexVersionCreated())),
                nested == null ? null : context.bitsetFilter(nested.nestedTypeFilter()),
                data
            );
        });
    }

    @Nullable
    private static String withoutDocValues(EsPhysicalOperationProviders.ShardContext shard, List<String> fields) {
        for (String field : fields) {
            MappedFieldType type = shard.fieldType(field);
            if (type != null && type.hasDocValues() == false) {
                return field;
            }
        }
        return null;
    }

    /** The {@code _doc} vector of {@code page}, or {@code null} when the page has no doc ids to read the index with. */
    @Nullable
    static DocVector docs(Page page) {
        for (int b = 0; b < page.getBlockCount(); b++) {
            if (page.getBlock(b) instanceof DocBlock doc) {
                return doc.asVector();
            }
        }
        return null;
    }

    /** The objects of the parent at {@code position}, in document order. */
    List<Map<?, ?>> objects(DocVector docs, int position, DriverContext context) {
        try {
            Shard shard = shards.get(docs.shards().getInt(position));
            LeafReaderContext leaf = shard.reader().leaves().get(docs.segments().getInt(position));
            int parent = docs.docs().getInt(position);
            BitSet children = shard.children() == null ? null : shard.children().getBitSet(leaf);
            if (children == null || parent == 0) {
                return List.of();
            }
            BitSet parents = shard.parents().getBitSet(leaf);
            int first = parents.prevSetBit(parent - 1) + 1;
            List<Map<?, ?>> objects = new ArrayList<>();
            for (int child = children.nextSetBit(first); child < parent; child = children.nextSetBit(child + 1)) {
                if ((objects.size() & 1023) == 1023) {
                    context.checkForEarlyTermination();
                }
                objects.add(object(shard, docs.shards().getInt(position), leaf, child));
            }
            return objects;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<String, Object> object(Shard shard, int shardId, LeafReaderContext leaf, int doc) throws IOException {
        if (shardId != this.shard || leaf.ord != segment || doc <= lastDoc) {
            for (int f = 0; f < values.length; f++) {
                values[f] = values(shard.fields().get(f), leaf);
            }
            this.shard = shardId;
            segment = leaf.ord;
        }
        lastDoc = doc;
        Map<String, Object> object = new LinkedHashMap<>();
        for (int f = 0; f < values.length; f++) {
            Object value = values[f] == null ? null : values[f].read(doc);
            if (value != null) {
                put(object, paths.get(f), value);
            }
        }
        return object;
    }

    @Nullable
    private static Values values(@Nullable IndexFieldData<?> field, LeafReaderContext leaf) {
        if (field == null) {
            return null;
        }
        if (field instanceof IndexNumericFieldData numeric) {
            LeafNumericFieldData data = numeric.load(leaf);
            if (numeric.getNumericType() == IndexNumericFieldData.NumericType.BOOLEAN) {
                SortedNumericLongValues longs = data.getLongValues();
                return doc -> longs.advanceExact(doc) ? collect(longs.docValueCount(), () -> longs.nextValue() != 0) : null;
            }
            if (numeric.getNumericType().isFloatingPoint()) {
                SortedNumericDoubleValues doubles = data.getDoubleValues();
                return doc -> doubles.advanceExact(doc) ? collect(doubles.docValueCount(), doubles::nextValue) : null;
            }
            SortedNumericLongValues longs = data.getLongValues();
            return doc -> longs.advanceExact(doc) ? collect(longs.docValueCount(), longs::nextValue) : null;
        }
        SortableBinaryDocValues bytes = field.load(leaf).getBytesValues();
        return doc -> bytes.advanceExact(doc) ? collect(bytes.docValueCount(), () -> bytes.nextValue().utf8ToString()) : null;
    }

    private interface Next {
        Object next() throws IOException;
    }

    /** One value as a scalar and several as a list, as {@code _source} holds them. */
    private static Object collect(int count, Next next) throws IOException {
        if (count == 1) {
            return next.next();
        }
        List<Object> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(next.next());
        }
        return list;
    }

    /** Puts {@code value} at the dotted {@code path}, creating the objects above it. */
    @SuppressWarnings("unchecked")
    private static void put(Map<String, Object> object, String path, Object value) {
        int dot;
        while ((dot = path.indexOf('.')) >= 0) {
            object = (Map<String, Object>) object.computeIfAbsent(path.substring(0, dot), k -> new LinkedHashMap<String, Object>());
            path = path.substring(dot + 1);
        }
        object.put(path, value);
    }
}

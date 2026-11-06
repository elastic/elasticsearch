/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;

/**
 * Reports which of a shard's fields have a value in each input document, and emits every distinct set of present fields
 * once per shard.
 * <p>
 * {@code METRICS_INFO} needs the combinations of metric and dimension fields that occur in the data, never their values.
 * Rebuilding {@code _source} to learn the field names reads every mapped field and round-trips JSON for each document.
 * This operator keeps one forward-only iterator per field over the documents that have it, so a document costs one int
 * comparison per absent field and one {@link DocIdSetIterator#advance} per present field.
 * <p>
 * Each output row has two blocks: the {@link ShardFields#indexName} and the multivalued names of the fields the document has.
 * A document that has none of the fields produces no row.
 */
public final class FieldPresenceOperator extends AbstractPageMappingOperator {
    /**
     * A field whose presence is reported.
     *
     * @param name     the name reported when a document has the field
     * @param presence matches the documents that have a value for the field. Every driver shares it, which is safe because
     *                 a {@link Weight} only builds scorers that belong to the calling thread.
     */
    public record Field(String name, Weight presence) {}

    /**
     * The fields reported for the documents of one shard.
     *
     * @param indexName the name reported for the shard's documents
     * @param reader    the reader the {@link Field#presence} weights were built against. The segment ordinals of the
     *                  doc vector index its leaves.
     * @param fields    the fields to check
     */
    public record ShardFields(String indexName, IndexReader reader, List<Field> fields) {
        public ShardFields {
            fields = List.copyOf(fields);
        }
    }

    /**
     * Builds the operators of every driver of a plan. They share the fields of each shard, which are only built when a driver
     * first reads the shard, so the shards that no document comes from cost nothing.
     */
    public static final class Factory implements OperatorFactory {
        private final IntFunction<ShardFields> buildShardFields;
        private final int docChannel;
        private final Map<Integer, ShardFields> fieldsByShard = new ConcurrentHashMap<>();

        /**
         * @param buildShardFields builds the fields of a shard, by the shard ordinal of the doc vector. Called at most once per
         *                         shard, from the driver thread that reads the shard first.
         * @param docChannel       channel of the {@link DocBlock} to read
         */
        public Factory(IntFunction<ShardFields> buildShardFields, int docChannel) {
            this.buildShardFields = buildShardFields;
            this.docChannel = docChannel;
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new FieldPresenceOperator(driverContext.blockFactory(), this);
        }

        @Override
        public String describe() {
            return "FieldPresenceOperator[docChannel=" + docChannel + "]";
        }

        private ShardFields shardFields(int shard) {
            return fieldsByShard.computeIfAbsent(shard, s -> {
                ShardFields fields = buildShardFields.apply(s);
                if (fields == null) {
                    throw new IllegalStateException("no fields configured for shard [" + s + "]");
                }
                return fields;
            });
        }
    }

    /**
     * A distinct set of present fields of one shard. Bit {@code i} stands for {@code ShardFields#fields().get(i)}.
     *
     * @param fieldCount the number of set bits, so a set of one field is written as a single value without counting them
     */
    private record FieldSet(int shard, FixedBitSet presentFields, int fieldCount) {}

    private static final long FIELD_SET_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(FieldSet.class);

    private final BlockFactory blockFactory;
    private final CircuitBreaker breaker;
    private final Factory factory;
    private final int docChannel;
    private final Set<FieldSet> seenFieldSets = new HashSet<>();
    private long trackedBytes;

    private SegmentIterators segmentIterators;

    private FieldPresenceOperator(BlockFactory blockFactory, Factory factory) {
        this.blockFactory = blockFactory;
        this.breaker = blockFactory.breaker();
        this.factory = factory;
        this.docChannel = factory.docChannel;
    }

    @Override
    protected Page process(Page page) {
        Page output;
        try {
            output = discoverFieldSets(page.getBlock(docChannel));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        // Released only on success, because the superclass releases the page it still holds when this fails.
        page.releaseBlocks();
        return output;
    }

    private Page discoverFieldSets(DocBlock docBlock) throws IOException {
        DocVector docVector = docBlock.asVector();
        docVector.ensureAttached("FieldPresenceOperator");
        IntVector shards = docVector.shards();
        IntVector segments = docVector.segments();
        IntVector docs = docVector.docs();
        List<FieldSet> discovered = new ArrayList<>();
        if (docVector.singleSegmentNonDecreasing()) {
            for (int p = 0; p < docVector.getPositionCount(); p++) {
                discover(shards.getInt(p), segments.getInt(p), docs.getInt(p), discovered);
            }
        } else {
            // Field iterators only move forward, so read in shard, segment and doc order.
            for (int p : docVector.shardSegmentDocMapForwards()) {
                discover(shards.getInt(p), segments.getInt(p), docs.getInt(p), discovered);
            }
        }
        return discovered.isEmpty() ? null : buildPage(shards, discovered);
    }

    private void discover(int shard, int segment, int doc, List<FieldSet> discovered) throws IOException {
        if (segmentIterators == null || segmentIterators.canRead(shard, segment, doc) == false) {
            segmentIterators = new SegmentIterators(shard, segment, factory.shardFields(shard));
        }
        int fieldCount = segmentIterators.readPresentFields(doc);
        FixedBitSet presentFields = segmentIterators.presentFields;
        if (fieldCount == 0 || seenFieldSets.contains(new FieldSet(shard, presentFields, fieldCount))) {
            return;
        }
        FieldSet fieldSet = new FieldSet(shard, presentFields.clone(), fieldCount);
        long bytes = FIELD_SET_SHALLOW_SIZE + fieldSet.presentFields().ramBytesUsed();
        breaker.addEstimateBytesAndMaybeBreak(bytes, "FieldPresenceOperator");
        trackedBytes += bytes;
        seenFieldSets.add(fieldSet);
        discovered.add(fieldSet);
    }

    private Page buildPage(IntVector shards, List<FieldSet> fieldSets) {
        Block indexNames = indexNames(shards, fieldSets);
        try (BytesRefBlock.Builder fieldNames = blockFactory.newBytesRefBlockBuilder(fieldSets.size())) {
            int fieldsShard = -1;
            ShardFields fields = null;
            for (FieldSet fieldSet : fieldSets) {
                if (fieldSet.shard() != fieldsShard) {
                    fieldsShard = fieldSet.shard();
                    fields = factory.shardFields(fieldsShard);
                }
                appendFieldNames(fieldNames, fields, fieldSet);
            }
            Page page = new Page(indexNames, fieldNames.build());
            indexNames = null;
            return page;
        } finally {
            Releasables.closeExpectNoException(indexNames);
        }
    }

    /** Sources emit pages of one shard, so the index name is usually a constant block. */
    private Block indexNames(IntVector shards, List<FieldSet> fieldSets) {
        if (shards.isConstant()) {
            BytesRef indexName = new BytesRef(factory.shardFields(shards.getInt(0)).indexName());
            return blockFactory.newConstantBytesRefBlockWith(indexName, fieldSets.size());
        }
        try (BytesRefBlock.Builder indexNames = blockFactory.newBytesRefBlockBuilder(fieldSets.size())) {
            for (FieldSet fieldSet : fieldSets) {
                indexNames.appendBytesRef(new BytesRef(factory.shardFields(fieldSet.shard()).indexName()));
            }
            return indexNames.build();
        }
    }

    private static void appendFieldNames(BytesRefBlock.Builder fieldNames, ShardFields fields, FieldSet fieldSet) {
        FixedBitSet present = fieldSet.presentFields();
        boolean multivalued = fieldSet.fieldCount() > 1;
        if (multivalued) {
            fieldNames.beginPositionEntry();
        }
        for (int f = present.nextSetBit(0); f != DocIdSetIterator.NO_MORE_DOCS; f = nextSetBit(present, f + 1)) {
            fieldNames.appendBytesRef(new BytesRef(fields.fields().get(f).name()));
        }
        if (multivalued) {
            fieldNames.endPositionEntry();
        }
    }

    private static int nextSetBit(FixedBitSet bits, int from) {
        return from < bits.length() ? bits.nextSetBit(from) : DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(super::close, () -> breaker.addWithoutBreaking(-trackedBytes));
    }

    @Override
    public String toString() {
        return "FieldPresenceOperator[docChannel=" + docChannel + "]";
    }

    /**
     * Forward-only iterators over the documents of one segment that have each field, by the position of the field in
     * {@link ShardFields#fields}. Fields missing from the segment get an empty iterator. Rebuilt when documents arrive from
     * another segment or before the last document read, and when the driver resumes on another thread, because doc values
     * iterators belong to the thread that created them.
     */
    private static final class SegmentIterators {
        private final Thread creationThread = Thread.currentThread();
        private final int shard;
        private final int segment;
        private final DocIdSetIterator[] iterators;
        /** Cached {@link DocIdSetIterator#docID} of each iterator, so an absent field costs no virtual call. */
        private final int[] currentDocs;
        /** The fields of the latest document read, overwritten by the next read. */
        private final FixedBitSet presentFields;
        private int lastDoc = -1;

        SegmentIterators(int shard, int segment, ShardFields shardFields) throws IOException {
            this.shard = shard;
            this.segment = segment;
            LeafReaderContext leaf = shardFields.reader().leaves().get(segment);
            List<Field> fields = shardFields.fields();
            this.iterators = new DocIdSetIterator[fields.size()];
            this.currentDocs = new int[fields.size()];
            for (int f = 0; f < fields.size(); f++) {
                Scorer scorer = fields.get(f).presence().scorer(leaf);
                iterators[f] = scorer == null ? DocIdSetIterator.empty() : scorer.iterator();
                currentDocs[f] = iterators[f].docID();
            }
            this.presentFields = new FixedBitSet(fields.size());
        }

        boolean canRead(int shard, int segment, int doc) {
            return creationThread == Thread.currentThread() && this.shard == shard && this.segment == segment && doc >= lastDoc;
        }

        /** Reads the fields {@code doc} has into {@link #presentFields} and returns how many it has. */
        int readPresentFields(int doc) throws IOException {
            lastDoc = doc;
            presentFields.clear(0, presentFields.length());
            int fieldCount = 0;
            for (int f = 0; f < iterators.length; f++) {
                int current = currentDocs[f];
                if (current < doc) {
                    current = iterators[f].advance(doc);
                    currentDocs[f] = current;
                }
                if (current == doc) {
                    presentFields.set(f);
                    fieldCount++;
                }
            }
            return fieldCount;
        }
    }
}

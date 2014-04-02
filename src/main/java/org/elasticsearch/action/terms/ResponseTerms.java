/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.terms;

import com.carrotsearch.hppc.DoubleOpenHashSet;
import com.carrotsearch.hppc.LongOpenHashSet;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.action.terms.TransportTermsByQueryAction.HitSetCollector;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.fielddata.*;

import java.io.IOException;
import java.util.List;

/**
 * Gathers and stores terms for a {@link TermsByQueryRequest}.
 */
public abstract class ResponseTerms implements Streamable {

    protected transient final HitSetCollector collector;

    /**
     * Default constructor
     */
    ResponseTerms() {
        this.collector = null;
    }

    /**
     * Constructor used before term collection
     *
     * @param collector the collector used during the lookup query execution
     */
    public ResponseTerms(HitSetCollector collector) {
        this.collector = collector;
    }

    /**
     * Deserialize to correct {@link ResponseTerms} implementation.
     */
    public static ResponseTerms deserialize(StreamInput in) throws IOException {
        Type type = Type.fromId(in.readByte());
        ResponseTerms rt;
        switch (type) {
            case LONGS:
                rt = new LongsResponseTerms();
                break;
            case DOUBLES:
                rt = new DoublesResponseTerms();
                break;
            case BLOOM:
                rt = new BloomResponseTerms();
                break;
            default:
                rt = new BytesResponseTerms();
        }

        rt.readFrom(in);
        return rt;
    }

    /**
     * Serialize a {@link ResponseTerms}.
     */
    public static void serialize(ResponseTerms rt, StreamOutput out) throws IOException {
        out.writeByte(rt.getType().id());
        rt.writeTo(out);
    }

    /**
     * Creates a new {@link ResponseTerms} based on the fielddata type and request settings.  Returns a
     * {@link LongsResponseTerms} for non-floating point numeric fields, {@link DoublesResponseTerms} for floating point
     * numeric fields, and a {@link BytesResponseTerms} for all other field types.  If
     * {@link TermsByQueryRequest#useBloomFilter} is set, then a {@link BloomResponseTerms} is returned for
     * non-numeric fields.
     *
     * @param collector      the collector used during the lookup query execution
     * @param indexFieldData the fielddata for the lookup field
     * @param request        the lookup request
     * @return {@link ResponseTerms} for the fielddata type
     */
    public static ResponseTerms get(HitSetCollector collector, IndexFieldData indexFieldData, TermsByQueryRequest request) {
        if (indexFieldData instanceof IndexNumericFieldData) {
            IndexNumericFieldData numFieldData = (IndexNumericFieldData) indexFieldData;
            if (numFieldData.getNumericType().isFloatingPoint()) {
                return new DoublesResponseTerms(collector, numFieldData, request.maxTermsPerShard());
            } else {
                return new LongsResponseTerms(collector, numFieldData, request.maxTermsPerShard());
            }
        } else {
            // use bytes or bloom for all non-numeric fields types
            if (request.useBloomFilter()) {
                BloomResponseTerms bloomTerms =
                        new BloomResponseTerms(collector, indexFieldData, request.bloomFpp(),
                                request.bloomExpectedInsertions(), request.bloomHashFunctions(),
                                request.maxTermsPerShard());
                return bloomTerms;
            } else {
                return new BytesResponseTerms(collector, indexFieldData, request.maxTermsPerShard());
            }
        }
    }

    /**
     * Gets a {@link ResponseTerms} for a specified type and initial size.
     *
     * @param type The {@link ResponseTerms.Type} to return
     * @param size The number of expected terms.
     * @return {@link ResponseTerms} of the specified type.
     */
    public static ResponseTerms get(Type type, int size) {
        switch (type) {
            case LONGS:
                return new LongsResponseTerms(size);
            case DOUBLES:
                return new DoublesResponseTerms(size);
            case BLOOM:
                return new BloomResponseTerms();
            default:
                return new BytesResponseTerms(size);
        }
    }

    /**
     * Called before gathering terms on a new {@link AtomicReaderContext}. Should be used to perform operations such
     * as loading fielddata, etc.
     *
     * @param context The {@link AtomicReaderContext} we are about to process
     */
    protected abstract void load(AtomicReaderContext context);

    /**
     * Called for each hit in the current {@link AtomicReaderContext}.  Should be used to extract terms.
     *
     * @param docId The internal lucene docid for the hit in the current {@link AtomicReaderContext}.
     */
    protected abstract void processDoc(int docId);

    /**
     * Returns the type.
     *
     * @return The {@link ResponseTerms.Type}
     */
    public abstract Type getType();

    /**
     * Called to merging {@link ResponseTerms} from other shards.
     *
     * @param other The {@link ResponseTerms} to merge with
     */
    public abstract void merge(ResponseTerms other);

    /**
     * The number of terms in the {@link ResponseTerms}.
     *
     * @return The number of terms
     */
    public abstract long size();

    /**
     * The size of the {@link ResponseTerms} in bytes.
     *
     * @return The size in bytes
     */
    public abstract long getSizeInBytes();

    /**
     * Returns the the terms.
     *
     * @return The terms
     */
    public abstract Object getTerms();

    /**
     * Returns if the max number of terms has been gathered or not.
     *
     * @return true if we have hit the max number of terms, false otherwise.
     */
    public abstract boolean isFull();

    /**
     * Process the terms lookup query.
     *
     * @param leaves a list of {@link AtomicReaderContext} the lookup query was executed against.
     * @throws IOException
     */
    public void process(List<AtomicReaderContext> leaves) throws IOException {
        FixedBitSet[] bitSets = collector.getFixedSets();
        for (int i = 0; i < leaves.size(); i++) {
            AtomicReaderContext readerContext = leaves.get(i);
            load(readerContext);
            DocIdSetIterator iterator = bitSets[i].iterator();
            int docId = 0;
            while ((docId = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS && !isFull()) {
                processDoc(docId);
            }
        }
    }

    /**
     * The various types of {@link ResponseTerms}.
     */
    public static enum Type {
        BYTES((byte) 0),
        LONGS((byte) 1),
        DOUBLES((byte) 2),
        BLOOM((byte) 3);

        private final byte id;

        Type(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static Type fromId(byte id) {
            switch (id) {
                case 0:
                    return BYTES;
                case 1:
                    return LONGS;
                case 2:
                    return DOUBLES;
                case 3:
                    return BLOOM;
                default:
                    return BYTES;
            }
        }
    }

    /**
     * A {@link ResponseTerms} implementation that creates a {@link BloomFilter} while collecting terms.  The {@link BloomFilter}
     * is less expensive to serialize over the network resulting in much better response times.  The trade off is the
     * possibility of false positives.  The {@link BloomFilter} can be tuned to for size (faster) or accuracy (slower) depending
     * on the user's needs.
     */
    public static class BloomResponseTerms extends ResponseTerms {

        private transient final IndexFieldData indexFieldData;
        private transient BytesValues values;
        private BloomFilter bloomFilter;
        private long maxTerms = Long.MAX_VALUE;  // max number of terms to gather per shard
        private long size = 0;

        /**
         * Default constructor
         */
        BloomResponseTerms() {
            this.indexFieldData = null;
        }

        /**
         * Constructor to be used before term collection and to specify custom {@link BloomFilter} settings.
         *
         * @param collector          the collector used during terms lookup query
         * @param indexFieldData     the fielddata for the lookup field
         * @param fpp                false positive probability, must be between value between 0 and 1.
         * @param expectedInsertions the expected number of terms to be inserted into the bloom filter
         * @param numHashFunctions   the number of hash functions to use
         */
        BloomResponseTerms(HitSetCollector collector, IndexFieldData indexFieldData,
                           Double fpp, Integer expectedInsertions, Integer numHashFunctions, Long maxTerms) {
            super(collector);
            this.indexFieldData = indexFieldData;

            if (maxTerms != null) {
                this.maxTerms = maxTerms;
            }

            if (fpp == null) {
                fpp = 0.03;
            }

            if (expectedInsertions == null) {
                expectedInsertions = 100;
            }

            if (numHashFunctions != null) {
                bloomFilter = BloomFilter.create(expectedInsertions, fpp, numHashFunctions);
            } else {
                bloomFilter = BloomFilter.create(expectedInsertions, fpp);
            }
        }

        /**
         * Load the fielddata
         *
         * @param context The {@link AtomicReaderContext} we are about to process
         */
        @Override
        protected void load(AtomicReaderContext context) {
            values = indexFieldData.load(context).getBytesValues(false); // load field data cache
        }

        /**
         * Extracts all values from the fielddata for the lookup field and inserts them in the bloom filter.
         *
         * @param docId The internal lucene docid for the hit in the current {@link AtomicReaderContext}.
         */
        @Override
        protected void processDoc(int docId) {
            final int numVals = values.setDocument(docId);
            for (int i = 0; i < numVals && !isFull(); i++) {
                final BytesRef term = values.nextValue();
                bloomFilter.put(term);
                size += 1;
            }
        }

        /**
         * The type.
         *
         * @return {@link BloomResponseTerms.Type#BLOOM}
         */
        @Override
        public Type getType() {
            return Type.BLOOM;
        }

        /**
         * Merge with another {@link BloomResponseTerms}
         *
         * @param other The {@link ResponseTerms} to merge with
         */
        @Override
        public void merge(ResponseTerms other) {
            assert other.getType() == Type.BLOOM; // must be a bloom

            BloomResponseTerms ot = (BloomResponseTerms) other;
            size += ot.size;

            if (bloomFilter == null) {
                bloomFilter = ot.bloomFilter;
            } else if (ot.bloomFilter != null) {
                bloomFilter.merge(ot.bloomFilter);
            }
        }

        /**
         * The number of terms represented in the bloom filter.
         *
         * @return the number of terms.
         */
        @Override
        public long size() {
            return size;
        }

        /**
         * The size of the bloom filter.
         *
         * @return The size of the bloom filter in bytes.
         */
        @Override
        public long getSizeInBytes() {
            return bloomFilter != null ? bloomFilter.getSizeInBytes() : 0;
        }

        /**
         * Returns the bloom filter.
         *
         * @return the {@link BloomFilter}
         */
        @Override
        public Object getTerms() {
            return bloomFilter;
        }

        /**
         * Returns if the max number of terms has been gathered or not.
         *
         * @return true if we have hit the max number of terms, false otherwise.
         */
        @Override
        public boolean isFull() {
            return size == maxTerms;
        }

        /**
         * Deserialize
         *
         * @param in the input
         * @throws IOException
         */
        @Override
        public void readFrom(StreamInput in) throws IOException {
            maxTerms = in.readVLong();
            if (in.readBoolean()) {
                bloomFilter = BloomFilter.readFrom(in);
            }

            size = in.readVLong();
        }

        /**
         * Serialize
         *
         * @param out the output
         * @throws IOException
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(maxTerms);
            if (bloomFilter != null) {
                out.writeBoolean(true);
                BloomFilter.writeTo(bloomFilter, out);
            } else {
                out.writeBoolean(false);
            }

            out.writeVLong(size);
        }
    }

    /**
     * A {@link ResponseTerms} implementation for non-numeric fields that collects all terms into a set.  All terms
     * are serialized and transferred over the network which could lead to slower response times if performing a lookup
     * on a field with a high-cardinality and a large result set.
     */
    public static class BytesResponseTerms extends ResponseTerms {

        private transient final IndexFieldData indexFieldData;
        private long maxTerms = Long.MAX_VALUE;  // max number of terms to gather per shard
        private long sizeInBytes;
        private transient BytesValues values;
        private BytesRefHash termsHash;

        /**
         * Default constructor
         */
        BytesResponseTerms() {
            this.indexFieldData = null;
        }

        /**
         * Constructor to be used before merging.  Initializes the set with the correct size to avoid rehashing during
         * the merge.
         *
         * @param size the number of terms that will be in the resulting set
         */
        BytesResponseTerms(long size) {
            this.termsHash = new BytesRefHash(size, BigArrays.NON_RECYCLING_INSTANCE);
            this.indexFieldData = null;
        }

        /**
         * Constructor to be used for term collection on each shard.
         *
         * @param collector      the collector used during the lookup query execution
         * @param indexFieldData the fielddata for the lookup field.
         */
        BytesResponseTerms(HitSetCollector collector, IndexFieldData indexFieldData, Long maxTerms) {
            super(collector);
            if (maxTerms != null) {
                this.maxTerms = maxTerms;
            }

            long collectorHits = collector.getHits();
            this.indexFieldData = indexFieldData;
            // TODO: use collectorHits to init?
            this.termsHash = new BytesRefHash(this.maxTerms < collectorHits ? this.maxTerms : collectorHits,
                    BigArrays.NON_RECYCLING_INSTANCE);
        }

        /**
         * Load the fielddata.
         *
         * @param context The {@link AtomicReaderContext} we are about to process
         */
        @Override
        protected void load(AtomicReaderContext context) {
            values = indexFieldData.load(context).getBytesValues(true); // load field data cache + hashes
        }

        /**
         * Extracts all values from the fielddata for the lookup field and inserts them in the terms set.
         *
         * @param docId The internal lucene docid for the hit in the current {@link AtomicReaderContext}.
         */
        @Override
        protected void processDoc(int docId) {
            final int numVals = values.setDocument(docId);
            for (int i = 0; i < numVals && !isFull(); i++) {
                final BytesRef term = values.nextValue();
                termsHash.add(term, values.currentValueHash());
                // offset int + length int + object pointer + object header + array pointer + array header = 64
                sizeInBytes += term.length + 64;
            }
        }

        /**
         * Merge with another {@link BytesResponseTerms}
         *
         * @param other The {@link ResponseTerms} to merge with
         */
        @Override
        public void merge(ResponseTerms other) {
            assert other.getType() == Type.BYTES;
            BytesResponseTerms ot = (BytesResponseTerms) other;
            sizeInBytes += ot.sizeInBytes;  // update size
            if (termsHash == null) {
                // probably never hit this since we init terms to known size before merge
                termsHash = new BytesRefHash(ot.size(), BigArrays.NON_RECYCLING_INSTANCE);
            }

            // TODO: maybe make it an option not to merge?
            BytesRef spare = new BytesRef();
            for (long i = 0; i < ot.termsHash.size(); i++) {
                ot.termsHash.get(i, spare);
                // TODO: avoid rehash of hashCode by pulling out of ot.termsHash somehow?
                termsHash.add(spare, spare.hashCode());
            }
        }

        /**
         * The type.
         *
         * @return {@link ResponseTerms.Type#BYTES}
         */
        @Override
        public Type getType() {
            return Type.BYTES;
        }


        /**
         * Returns if the max number of terms has been gathered or not.
         *
         * @return true if we have hit the max number of terms, false otherwise.
         */
        @Override
        public boolean isFull() {
            return termsHash.size() == maxTerms;
        }

        /**
         * Deserialize
         *
         * @param in the input
         * @throws IOException
         */
        @Override
        public void readFrom(StreamInput in) throws IOException {
            long size = in.readVLong();
            maxTerms = in.readVLong();
            sizeInBytes = in.readVLong();
            termsHash = new BytesRefHash(size, BigArrays.NON_RECYCLING_INSTANCE);
            for (long i = 0; i < size; i++) {
                BytesRef term = in.readBytesRef();
                termsHash.add(term, term.hashCode());
            }
        }

        /**
         * Serialize
         *
         * @param out the output
         * @throws IOException
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(termsHash.size());
            out.writeVLong(maxTerms);
            out.writeVLong(sizeInBytes);
            BytesRef spare = new BytesRef();
            for (long i = 0; i < termsHash.size(); i++) {
                termsHash.get(i, spare);
                out.writeBytesRef(spare);
            }
        }

        /**
         * The number of collected terms.
         *
         * @return the number of terms.
         */
        @Override
        public long size() {
            return termsHash.size();
        }

        /**
         * The size of the terms.
         *
         * @return the size in bytes.
         */
        @Override
        public long getSizeInBytes() {
            return sizeInBytes;
        }

        /**
         * Returns the terms.
         *
         * @return {@link org.elasticsearch.common.util.BytesRefHash}
         */
        @Override
        public Object getTerms() {
            return termsHash;
        }
    }

    /**
     * A {@link ResponseTerms} implementation for non-floating point numeric fields that collects all terms into a set.
     * All terms are gathered and serialized as primitive longs.
     */
    public static class LongsResponseTerms extends ResponseTerms {

        private transient final IndexNumericFieldData indexFieldData;
        private transient LongValues values;
        private long maxTerms = Long.MAX_VALUE;  // max number of terms to gather per shard
        private LongHash termsHash;

        /**
         * Default constructor
         */
        LongsResponseTerms() {
            this.indexFieldData = null;
        }

        /**
         * Constructor to be used before merging.  Initializes the set with the correct size to avoid rehashing during
         * the merge.
         *
         * @param size the number of terms that will be in the resulting set
         */
        LongsResponseTerms(long size) {
            // create terms set, size is known so adjust for load factor so no rehashing is needed
            this.termsHash = new LongHash(size, BigArrays.NON_RECYCLING_INSTANCE);
            this.indexFieldData = null;
        }

        /**
         * Constructor to be used for term collection on each shard.
         *
         * @param collector      the collector used during the lookup query execution
         * @param indexFieldData the fielddata for the lookup field.
         */
        LongsResponseTerms(HitSetCollector collector, IndexNumericFieldData indexFieldData, Long maxTerms) {
            super(collector);
            if (maxTerms != null) {
                this.maxTerms = maxTerms;
            }

            long collectorHits = collector.getHits();
            this.indexFieldData = indexFieldData;
            this.termsHash = new LongHash(this.maxTerms < collectorHits ? this.maxTerms : collectorHits,
                    BigArrays.NON_RECYCLING_INSTANCE);
        }

        /**
         * Load the fielddata
         *
         * @param context The {@link AtomicReaderContext} we are about to process
         */
        @Override
        protected void load(AtomicReaderContext context) {
            values = indexFieldData.load(context).getLongValues(); // load field data cache
        }

        /**
         * Extracts all values from the fielddata for the lookup field and inserts them in the terms set.
         *
         * @param docId The internal lucene docid for the hit in the current {@link AtomicReaderContext}.
         */
        @Override
        protected void processDoc(int docId) {
            final int numVals = values.setDocument(docId);
            for (int i = 0; i < numVals && !isFull(); i++) {
                final long term = values.nextValue();
                termsHash.add(term);
            }
        }

        /**
         * Merge with another {@link LongsResponseTerms}.
         *
         * @param other The {@link ResponseTerms} to merge with
         */
        @Override
        public void merge(ResponseTerms other) {
            assert other.getType() == Type.LONGS;
            LongsResponseTerms ot = (LongsResponseTerms) other;

            if (termsHash == null) {
                // probably never hit this since we init terms to known size before merge
                termsHash = new LongHash(ot.size(), BigArrays.NON_RECYCLING_INSTANCE);
            }

            // TODO: maybe make it an option not to merge?
            for (long i = 0; i < ot.termsHash.size(); i++) {
                termsHash.add(ot.termsHash.get(i));
            }
        }

        /**
         * The type
         *
         * @return {@link ResponseTerms.Type#LONGS}
         */
        @Override
        public Type getType() {
            return Type.LONGS;
        }

        /**
         * Returns if the max number of terms has been gathered or not.
         *
         * @return true if we have hit the max number of terms, false otherwise.
         */
        @Override
        public boolean isFull() {
            return termsHash.size() == maxTerms;
        }

        /**
         * Deserialize
         *
         * @param in the input
         * @throws IOException
         */
        @Override
        public void readFrom(StreamInput in) throws IOException {
            maxTerms = in.readVLong();
            long size = in.readVLong();
            termsHash = new LongHash(size, BigArrays.NON_RECYCLING_INSTANCE);
            for (long i = 0; i < size; i++) {
                termsHash.add(in.readLong());
            }
        }

        /**
         * Serialize
         *
         * @param out the output
         * @throws IOException
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(maxTerms);
            out.writeVLong(termsHash.size());
            for (long i = 0; i < termsHash.size(); i++) {
                out.writeLong(termsHash.get(i));
            }
        }

        /**
         * The number of terms.
         *
         * @return the number of terms collected.
         */
        @Override
        public long size() {
            return termsHash.size();
        }

        /**
         * The size of the gathered terms
         *
         * @return The size in bytes
         */
        @Override
        public long getSizeInBytes() {
            return termsHash.size() * 8;
        }

        /**
         * Returns the terms.
         *
         * @return {@link LongOpenHashSet}
         */
        @Override
        public Object getTerms() {
            return termsHash;
        }
    }

    /**
     * A {@link ResponseTerms} implementation for floating point numeric fields that collects all terms into a set.
     * All terms are gathered and serialized as primitive doubles.
     */
    public static class DoublesResponseTerms extends ResponseTerms {

        private transient final IndexNumericFieldData indexFieldData;
        private transient DoubleValues values;
        private long maxTerms = Long.MAX_VALUE;  // max number of terms to gather per shard
        private LongHash termsHash;

        /**
         * Default constructor
         */
        DoublesResponseTerms() {
            this.indexFieldData = null;
        }

        /**
         * Constructor to be used before merging.  Initializes the set with the correct size to avoid rehashing during
         * the merge.
         *
         * @param size the number of terms that will be in the resulting set
         */
        DoublesResponseTerms(long size) {
            this.termsHash = new LongHash(size, BigArrays.NON_RECYCLING_INSTANCE);
            this.indexFieldData = null;
        }

        /**
         * Constructor to be used for term collection on each shard.
         *
         * @param collector      the collector used during the lookup query execution
         * @param indexFieldData the fielddata for the lookup field.
         */
        DoublesResponseTerms(HitSetCollector collector, IndexNumericFieldData indexFieldData, Long maxTerms) {
            super(collector);
            if (maxTerms != null) {
                this.maxTerms = maxTerms;
            }

            long collectorHits = collector.getHits();
            this.indexFieldData = indexFieldData;
            this.termsHash = new LongHash(this.maxTerms < collectorHits ? this.maxTerms : collectorHits,
                    BigArrays.NON_RECYCLING_INSTANCE);
        }

        /**
         * Load the fielddata
         *
         * @param context The {@link AtomicReaderContext} we are about to process
         */
        @Override
        protected void load(AtomicReaderContext context) {
            values = indexFieldData.load(context).getDoubleValues(); // load field data cache
        }

        /**
         * Extracts all values from the fielddata for the lookup field and inserts them in the terms set.
         *
         * @param docId The internal lucene docid for the hit in the current {@link AtomicReaderContext}.
         */
        @Override
        protected void processDoc(int docId) {
            final int numVals = values.setDocument(docId);
            for (int i = 0; i < numVals && !isFull(); i++) {
                final double term = values.nextValue();
                final long longTerm = Double.doubleToLongBits(term);
                termsHash.add(longTerm);
            }
        }

        /**
         * Merge with another {@link DoublesResponseTerms}
         *
         * @param other The {@link ResponseTerms} to merge with
         */
        @Override
        public void merge(ResponseTerms other) {
            assert other.getType() == Type.DOUBLES;
            DoublesResponseTerms ot = (DoublesResponseTerms) other;

            if (termsHash == null) {
                // probably never hit this since we init terms to known size before merge
                termsHash = new LongHash(ot.size(), BigArrays.NON_RECYCLING_INSTANCE);
            }

            // TODO: maybe make it an option not to merge?
            for (long i = 0; i < ot.termsHash.size(); i++) {
                termsHash.add(ot.termsHash.get(i));
            }
        }

        /**
         * The type
         *
         * @return {@link ResponseTerms.Type#DOUBLES}
         */
        @Override
        public Type getType() {
            return Type.DOUBLES;
        }

        @Override
        public boolean isFull() {
            return termsHash.size() == maxTerms;
        }

        /**
         * Deserialize
         *
         * @param in the input
         * @throws IOException
         */
        @Override
        public void readFrom(StreamInput in) throws IOException {
            maxTerms = in.readVLong();
            long size = in.readVLong();
            termsHash = new LongHash(size, BigArrays.NON_RECYCLING_INSTANCE);
            for (long i = 0; i < size; i++) {
                termsHash.add(in.readLong());
            }
        }

        /**
         * Serialize
         *
         * @param out the output
         * @throws IOException
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(maxTerms);
            out.writeVLong(termsHash.size());
            for (long i = 0; i < termsHash.capacity(); i++) {
                out.writeLong(termsHash.get(i));
            }
        }

        /**
         * The number of collected terms
         *
         * @return the number of terms.
         */
        @Override
        public long size() {
            return termsHash.size();
        }

        /**
         * The size of the collected terms
         *
         * @return The size of the terms in bytes.
         */
        @Override
        public long getSizeInBytes() {
            return termsHash.size() * 8;
        }

        /**
         * Returns the terms.
         *
         * @return {@link DoubleOpenHashSet}
         */
        @Override
        public Object getTerms() {
            return termsHash;
        }
    }
}

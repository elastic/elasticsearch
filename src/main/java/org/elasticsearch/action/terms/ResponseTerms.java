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
import com.carrotsearch.hppc.ObjectOpenHashSet;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.action.terms.TransportTermsByQueryAction.HitSetCollector;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.util.BloomFilter;
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
        Type type = Type.fromOrd(in.readVInt());
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
        out.writeVInt(rt.getType().ordinal());
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
    public abstract int size();

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
        BYTES, LONGS, DOUBLES, BLOOM;

        public static Type fromOrd(int ord) {
            switch (ord) {
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
        private int maxTerms = Integer.MAX_VALUE;  // max number of terms to gather per shard
        private int size = 0;

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
                           Double fpp, Integer expectedInsertions, Integer numHashFunctions, Integer maxTerms) {
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
        public int size() {
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
            maxTerms = in.readVInt();
            if (in.readBoolean()) {
                bloomFilter = BloomFilter.readFrom(in);
            }

            size = in.readVInt();
        }

        /**
         * Serialize
         *
         * @param out the output
         * @throws IOException
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(maxTerms);
            if (bloomFilter != null) {
                out.writeBoolean(true);
                BloomFilter.writeTo(bloomFilter, out);
            } else {
                out.writeBoolean(false);
            }

            out.writeVInt(size);
        }
    }

    /**
     * A {@link ResponseTerms} implementation for non-numeric fields that collects all terms into a set.  All terms
     * are serialized and transferred over the network which could lead to slower response times if performing a lookup
     * on a field with a high-cardinality and a large result set.
     */
    public static class BytesResponseTerms extends ResponseTerms {

        private transient final IndexFieldData indexFieldData;
        private int maxTerms = Integer.MAX_VALUE;  // max number of terms to gather per shard
        private long sizeInBytes;
        private transient BytesValues values;
        private ObjectOpenHashSet<BytesRef> terms;


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
        BytesResponseTerms(int size) {
            // create terms set, size is known so adjust for load factor so no rehashing is needed
            this.terms = new ObjectOpenHashSet<BytesRef>(optimalSetSize(size));
            this.indexFieldData = null;
        }

        /**
         * Constructor to be used for term collection on each shard.
         *
         * @param collector      the collector used during the lookup query execution
         * @param indexFieldData the fielddata for the lookup field.
         */
        BytesResponseTerms(HitSetCollector collector, IndexFieldData indexFieldData, Integer maxTerms) {
            super(collector);
            if (maxTerms != null) {
                this.maxTerms = maxTerms;
            }

            int collectorHits = collector.getHits();
            this.indexFieldData = indexFieldData;
            this.terms = new ObjectOpenHashSet<BytesRef>(optimalSetSize(this.maxTerms < collectorHits ? this.maxTerms : collectorHits));
        }

        /**
         * Calculates the optimal set size to avoid rehashing
         *
         * @param size the number of items being inserted
         * @return the optimal size
         */
        protected int optimalSetSize(int size) {
            return (int) (size / ObjectOpenHashSet.DEFAULT_LOAD_FACTOR) + 1;
        }

        /**
         * Load the fielddata.
         *
         * @param context The {@link AtomicReaderContext} we are about to process
         */
        @Override
        protected void load(AtomicReaderContext context) {
            values = indexFieldData.load(context).getBytesValues(false); // load field data cache
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
                terms.add(values.copyShared());  // so it is safe to be placed in set
                sizeInBytes += term.length + 8;  // 8 additional bytes for the 2 ints in a BytesRef
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
            sizeInBytes += ot.sizeInBytes;  // update size and hashcode
            if (terms == null) {
                // probably never hit this since we init terms to known size before merge
                terms = new ObjectOpenHashSet<BytesRef>(ot.terms);
            } else {
                // TODO: maybe make it an option not to merge and just store all the sets (to avoid internal hashing)
                // this would use more memory due to duplicate terms but might be faster?
                terms.addAll(ot.terms);
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
            return terms.size() == maxTerms;
        }

        /**
         * Deserialize
         *
         * @param in the input
         * @throws IOException
         */
        @Override
        public void readFrom(StreamInput in) throws IOException {
            int size = in.readVInt();
            maxTerms = in.readVInt();
            sizeInBytes = in.readVLong();
            // init set to account large enough to avoid rehashing during insert
            terms = new ObjectOpenHashSet<BytesRef>(optimalSetSize(size));
            for (int i = 0; i < size; i++) {
                BytesRef term = in.readBytesRef();
                terms.add(term);
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
            out.writeVInt(terms.size());
            out.writeVInt(maxTerms);
            out.writeVLong(sizeInBytes);

            final boolean[] allocated = terms.allocated;
            final Object[] keys = terms.keys;
            for (int i = 0; i < allocated.length; i++) {
                if (allocated[i]) {
                    out.writeBytesRef((BytesRef) keys[i]);
                }
            }
        }

        /**
         * The number of collected terms.
         *
         * @return the number of terms.
         */
        @Override
        public int size() {
            return terms.size();
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
         * @return {@link ObjectOpenHashSet} of {@link BytesRef}
         */
        @Override
        public Object getTerms() {
            return terms;
        }
    }

    /**
     * A {@link ResponseTerms} implementation for non-floating point numeric fields that collects all terms into a set.
     * All terms are gathered and serialized as primitive longs.
     */
    public static class LongsResponseTerms extends ResponseTerms {

        private transient final IndexNumericFieldData indexFieldData;
        private transient LongValues values;
        private int maxTerms = Integer.MAX_VALUE;  // max number of terms to gather per shard
        private LongOpenHashSet terms;

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
        LongsResponseTerms(int size) {
            // create terms set, size is known so adjust for load factor so no rehashing is needed
            this.terms = new LongOpenHashSet(optimalSetSize(size));
            this.indexFieldData = null;
        }

        /**
         * Constructor to be used for term collection on each shard.
         *
         * @param collector      the collector used during the lookup query execution
         * @param indexFieldData the fielddata for the lookup field.
         */
        LongsResponseTerms(HitSetCollector collector, IndexNumericFieldData indexFieldData, Integer maxTerms) {
            super(collector);
            if (maxTerms != null) {
                this.maxTerms = maxTerms;
            }

            int collectorHits = collector.getHits();
            this.indexFieldData = indexFieldData;
            this.terms = new LongOpenHashSet(optimalSetSize(this.maxTerms < collectorHits ? this.maxTerms : collectorHits));
        }

        /**
         * Calculates the optimal set size to avoid rehashing
         *
         * @param size the number of items being inserted
         * @return the optimal size
         */
        protected int optimalSetSize(int size) {
            return (int) (size / LongOpenHashSet.DEFAULT_LOAD_FACTOR) + 1;
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
                terms.add(term);
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
            if (terms == null) {
                terms = new LongOpenHashSet(ot.terms);
            } else {
                terms.addAll(ot.terms);
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
            return terms.size() == maxTerms;
        }

        /**
         * Deserialize
         *
         * @param in the input
         * @throws IOException
         */
        @Override
        public void readFrom(StreamInput in) throws IOException {
            maxTerms = in.readVInt();
            int size = in.readVInt();
            terms = new LongOpenHashSet(optimalSetSize(size));
            for (int i = 0; i < size; i++) {
                terms.add(in.readVLong());
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
            out.writeVInt(maxTerms);
            out.writeVInt(terms.size());

            final boolean[] allocated = terms.allocated;
            final long[] keys = terms.keys;
            for (int i = 0; i < allocated.length; i++) {
                if (allocated[i]) {
                    out.writeVLong(keys[i]);
                }
            }
        }

        /**
         * The number of terms.
         *
         * @return the number of terms collected.
         */
        @Override
        public int size() {
            return terms.size();
        }

        /**
         * The size of the gathered terms
         *
         * @return The size in bytes
         */
        @Override
        public long getSizeInBytes() {
            return terms.size() * 8;
        }

        /**
         * Returns the terms.
         *
         * @return {@link LongOpenHashSet}
         */
        @Override
        public Object getTerms() {
            return terms;
        }
    }

    /**
     * A {@link ResponseTerms} implementation for floating point numeric fields that collects all terms into a set.
     * All terms are gathered and serialized as primitive doubles.
     */
    public static class DoublesResponseTerms extends ResponseTerms {

        private transient final IndexNumericFieldData indexFieldData;
        private transient DoubleValues values;
        private int maxTerms = Integer.MAX_VALUE;  // max number of terms to gather per shard
        private DoubleOpenHashSet terms;

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
        DoublesResponseTerms(int size) {
            // create terms set, size is known so adjust for load factor so no rehashing is needed
            this.terms = new DoubleOpenHashSet(optimalSetSize(size));
            this.indexFieldData = null;
        }

        /**
         * Constructor to be used for term collection on each shard.
         *
         * @param collector      the collector used during the lookup query execution
         * @param indexFieldData the fielddata for the lookup field.
         */
        DoublesResponseTerms(HitSetCollector collector, IndexNumericFieldData indexFieldData, Integer maxTerms) {
            super(collector);
            if (maxTerms != null) {
                this.maxTerms = maxTerms;
            }

            int collectorHits = collector.getHits();
            this.indexFieldData = indexFieldData;
            this.terms = new DoubleOpenHashSet(optimalSetSize(this.maxTerms < collectorHits ? this.maxTerms : collectorHits));
        }

        /**
         * Calculates the optimal set size to avoid rehashing
         *
         * @param size the number of items being inserted
         * @return the optimal size
         */
        protected int optimalSetSize(int size) {
            return (int) (size / DoubleOpenHashSet.DEFAULT_LOAD_FACTOR) + 1;
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
                terms.add(term);
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
            if (terms == null) {
                terms = new DoubleOpenHashSet(ot.terms);
            } else {
                terms.addAll(ot.terms);
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
            return terms.size() == maxTerms;
        }

        /**
         * Deserialize
         *
         * @param in the input
         * @throws IOException
         */
        @Override
        public void readFrom(StreamInput in) throws IOException {
            maxTerms = in.readVInt();
            int size = in.readVInt();
            terms = new DoubleOpenHashSet(optimalSetSize(size));
            for (int i = 0; i < size; i++) {
                terms.add(in.readDouble());
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
            out.writeVInt(maxTerms);
            out.writeVInt(terms.size());
            final boolean[] allocated = terms.allocated;
            final double[] keys = terms.keys;
            for (int i = 0; i < allocated.length; i++) {
                if (allocated[i]) {
                    out.writeDouble(keys[i]);
                }
            }
        }

        /**
         * The number of collected terms
         *
         * @return the number of terms.
         */
        @Override
        public int size() {
            return terms.size();
        }

        /**
         * The size of the collected terms
         *
         * @return The size of the terms in bytes.
         */
        @Override
        public long getSizeInBytes() {
            return terms.size() * 8;
        }

        /**
         * Returns the terms.
         *
         * @return {@link DoubleOpenHashSet}
         */
        @Override
        public Object getTerms() {
            return terms;
        }
    }
}

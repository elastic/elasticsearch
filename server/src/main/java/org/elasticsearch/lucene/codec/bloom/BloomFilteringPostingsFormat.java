/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.lucene.codec.bloom;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A {@link PostingsFormat} useful for low doc-frequency fields such as primary keys. Bloom filters
 * are maintained in a ".blm" file which offers "fast-fail" for reads in segments known to have no
 * record of the key. A choice of delegate PostingsFormat is used to record all other Postings data.
 *
 * <p>A choice of {@link BloomFilterFactory} can be passed to tailor Bloom Filter settings on a
 * per-field basis. The default configuration is {@link DefaultBloomFilterFactory} which allocates a
 * ~8mb bitset and hashes values using {@link MurmurHash2}. This should be suitable for most
 * purposes.
 *
 * <p>The format of the blm file is as follows:
 *
 * <ul>
 *   <li>BloomFilter (.blm) --&gt; Header, DelegatePostingsFormatName, NumFilteredFields,
 *       Filter<sup>NumFilteredFields</sup>, Footer
 *   <li>Filter --&gt; FieldNumber, FuzzySet
 *   <li>FuzzySet --&gt;See {@link FuzzySet#serialize(DataOutput)}
 *   <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}
 *   <li>DelegatePostingsFormatName --&gt; {@link DataOutput#writeString(String) String} The name of
 *       a ServiceProvider registered {@link PostingsFormat}
 *   <li>NumFilteredFields --&gt; {@link DataOutput#writeInt Uint32}
 *   <li>FieldNumber --&gt; {@link DataOutput#writeInt Uint32} The number of the field in this
 *       segment
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 * </ul>
 *
 * @lucene.experimental
 */
public final class BloomFilteringPostingsFormat extends PostingsFormat {

    public static final String BLOOM_CODEC_NAME = "BloomFilter";
    public static final int VERSION_START = 3;
    public static final int VERSION_CURRENT = VERSION_START;

    /** Extension of Bloom Filters file */
    static final String BLOOM_EXTENSION = "blm";

    BloomFilterFactory bloomFilterFactory = new DefaultBloomFilterFactory();
    private PostingsFormat delegatePostingsFormat;

    /**
     * Creates Bloom filters for a selection of fields created in the index. This is recorded as a set
     * of Bitsets held as a segment summary in an additional "blm" file. This PostingsFormat delegates
     * to a choice of delegate PostingsFormat for encoding all other postings data.
     *
     * @param delegatePostingsFormat The PostingsFormat that records all the non-bloom filter data
     *     i.e. postings info.
     * @param bloomFilterFactory The {@link BloomFilterFactory} responsible for sizing BloomFilters
     *     appropriately
     */
    public BloomFilteringPostingsFormat(PostingsFormat delegatePostingsFormat, BloomFilterFactory bloomFilterFactory) {
        super(BLOOM_CODEC_NAME);
        this.delegatePostingsFormat = delegatePostingsFormat;
        this.bloomFilterFactory = bloomFilterFactory;
    }

    /**
     * Creates Bloom filters for a selection of fields created in the index. This is recorded as a set
     * of Bitsets held as a segment summary in an additional "blm" file. This PostingsFormat delegates
     * to a choice of delegate PostingsFormat for encoding all other postings data. This choice of
     * constructor defaults to the {@link DefaultBloomFilterFactory} for configuring per-field
     * BloomFilters.
     *
     * @param delegatePostingsFormat The PostingsFormat that records all the non-bloom filter data
     *     i.e. postings info.
     */
    public BloomFilteringPostingsFormat(PostingsFormat delegatePostingsFormat) {
        this(delegatePostingsFormat, new DefaultBloomFilterFactory());
    }

    // Used only by core Lucene at read-time via Service Provider instantiation -
    // do not use at Write-time in application code.
    public BloomFilteringPostingsFormat() {
        super(BLOOM_CODEC_NAME);
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        if (delegatePostingsFormat == null) {
            throw new UnsupportedOperationException(
                "Error - " + getClass().getName() + " has been constructed without a choice of PostingsFormat"
            );
        }
        FieldsConsumer fieldsConsumer = delegatePostingsFormat.fieldsConsumer(state);
        return new BloomFilteredFieldsConsumer(fieldsConsumer, state);
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new BloomFilteredFieldsProducer(state);
    }

    static class BloomFilteredFieldsProducer extends FieldsProducer {
        private FieldsProducer delegateFieldsProducer;
        HashMap<String, OnDiskFuzzySet> bloomsByFieldName = new HashMap<>();
        private final IndexInput bloomIn;

        public BloomFilteredFieldsProducer(SegmentReadState state) throws IOException {
            String bloomFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BLOOM_EXTENSION);
            this.bloomIn = state.directory.openInput(bloomFileName, state.context);
            boolean success = false;
            try {
                CodecUtil.checkIndexHeader(
                    bloomIn,
                    BLOOM_CODEC_NAME,
                    VERSION_START,
                    VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                // // Load the hash function used in the BloomFilter
                // hashFunction = HashFunction.forName(bloomIn.readString());
                // Load the delegate postings format
                PostingsFormat delegatePostingsFormat = PostingsFormat.forName(bloomIn.readString());

                this.delegateFieldsProducer = delegatePostingsFormat.fieldsProducer(state);
                int numBlooms = bloomIn.readInt();
                if (numBlooms > 1) {
                    throw new IllegalStateException("This version supports single bloom filter");
                }
                for (int i = 0; i < numBlooms; i++) {
                    int fieldNum = bloomIn.readInt();
                    FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldNum);
                    bloomsByFieldName.put(fieldInfo.name, new OnDiskFuzzySet(bloomIn));
                }
                success = true;
            } finally {
                if (!success) {
                    IOUtils.closeWhileHandlingException(bloomIn, delegateFieldsProducer);
                }
            }
        }

        @Override
        public Iterator<String> iterator() {
            return delegateFieldsProducer.iterator();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(delegateFieldsProducer, bloomIn);
        }

        @Override
        public Terms terms(String field) throws IOException {
            OnDiskFuzzySet filter = bloomsByFieldName.get(field);
            if (filter == null) {
                return delegateFieldsProducer.terms(field);
            } else {
                Terms result = delegateFieldsProducer.terms(field);
                if (result == null) {
                    return null;
                }
                return new BloomFilteredTerms(result, filter);
            }
        }

        @Override
        public int size() {
            return delegateFieldsProducer.size();
        }

        static final class BloomFilteredTerms extends Terms {
            private Terms delegateTerms;
            private OnDiskFuzzySet filter;

            public BloomFilteredTerms(Terms terms, OnDiskFuzzySet filter) {
                this.delegateTerms = terms;
                this.filter = filter;
            }

            @Override
            public TermsEnum intersect(CompiledAutomaton compiled, final BytesRef startTerm) throws IOException {
                return delegateTerms.intersect(compiled, startTerm);
            }

            @Override
            public TermsEnum iterator() throws IOException {
                return new BloomFilteredTermsEnum(delegateTerms, filter);
            }

            @Override
            public long size() throws IOException {
                return delegateTerms.size();
            }

            @Override
            public long getSumTotalTermFreq() throws IOException {
                return delegateTerms.getSumTotalTermFreq();
            }

            @Override
            public long getSumDocFreq() throws IOException {
                return delegateTerms.getSumDocFreq();
            }

            @Override
            public int getDocCount() throws IOException {
                return delegateTerms.getDocCount();
            }

            @Override
            public boolean hasFreqs() {
                return delegateTerms.hasFreqs();
            }

            @Override
            public boolean hasOffsets() {
                return delegateTerms.hasOffsets();
            }

            @Override
            public boolean hasPositions() {
                return delegateTerms.hasPositions();
            }

            @Override
            public boolean hasPayloads() {
                return delegateTerms.hasPayloads();
            }

            @Override
            public BytesRef getMin() throws IOException {
                return delegateTerms.getMin();
            }

            @Override
            public BytesRef getMax() throws IOException {
                return delegateTerms.getMax();
            }

            @Override
            public Object getStats() throws IOException {
                return delegateTerms.getStats();
            }
        }

        static final class BloomFilteredTermsEnum extends BaseTermsEnum {
            private Terms delegateTerms;
            private TermsEnum delegateTermsEnum;
            private final OnDiskFuzzySet filter;

            public BloomFilteredTermsEnum(Terms delegateTerms, OnDiskFuzzySet filter) throws IOException {
                this.delegateTerms = delegateTerms;
                this.filter = filter;
            }

            void reset(Terms delegateTerms) throws IOException {
                this.delegateTerms = delegateTerms;
                this.delegateTermsEnum = null;
            }

            private TermsEnum delegate() throws IOException {
                if (delegateTermsEnum == null) {
                    /* pull the iterator only if we really need it -
                     * this can be a relativly heavy operation depending on the
                     * delegate postings format and they underlying directory
                     * (clone IndexInput) */
                    delegateTermsEnum = delegateTerms.iterator();
                }
                return delegateTermsEnum;
            }

            @Override
            public BytesRef next() throws IOException {
                return delegate().next();
            }

            @Override
            public boolean seekExact(BytesRef text) throws IOException {
                // The magical fail-fast speed up that is the entire point of all of
                // this code - save a disk seek if there is a match on an in-memory
                // structure
                // that may occasionally give a false positive but guaranteed no false
                // negatives
                if (filter.contains(text) == FuzzySet.ContainsResult.NO) {
                    return false;
                }
                return delegate().seekExact(text);
            }

            @Override
            public SeekStatus seekCeil(BytesRef text) throws IOException {
                return delegate().seekCeil(text);
            }

            @Override
            public void seekExact(long ord) throws IOException {
                delegate().seekExact(ord);
            }

            @Override
            public BytesRef term() throws IOException {
                return delegate().term();
            }

            @Override
            public long ord() throws IOException {
                return delegate().ord();
            }

            @Override
            public int docFreq() throws IOException {
                return delegate().docFreq();
            }

            @Override
            public long totalTermFreq() throws IOException {
                return delegate().totalTermFreq();
            }

            @Override
            public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
                return delegate().postings(reuse, flags);
            }

            @Override
            public ImpactsEnum impacts(int flags) throws IOException {
                return delegate().impacts(flags);
            }
        }

        @Override
        public void checkIntegrity() throws IOException {
            delegateFieldsProducer.checkIntegrity();
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(fields=" + bloomsByFieldName.size() + ",delegate=" + delegateFieldsProducer + ")";
        }
    }

    class BloomFilteredFieldsConsumer extends FieldsConsumer {
        private FieldsConsumer delegateFieldsConsumer;
        private Map<FieldInfo, FuzzySet> bloomFilters = new HashMap<>();
        private SegmentWriteState state;

        public BloomFilteredFieldsConsumer(FieldsConsumer fieldsConsumer, SegmentWriteState state) {
            this.delegateFieldsConsumer = fieldsConsumer;
            this.state = state;
        }

        @Override
        public void write(Fields fields, NormsProducer norms) throws IOException {

            // Delegate must write first: it may have opened files
            // on creating the class
            // (e.g. Lucene41PostingsConsumer), and write() will
            // close them; alternatively, if we delayed pulling
            // the fields consumer until here, we could do it
            // afterwards:
            delegateFieldsConsumer.write(fields, norms);

            for (String field : fields) {
                Terms terms = fields.terms(field);
                if (terms == null) {
                    continue;
                }
                FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
                TermsEnum termsEnum = terms.iterator();

                FuzzySet bloomFilter = null;

                PostingsEnum postingsEnum = null;
                while (true) {
                    BytesRef term = termsEnum.next();
                    if (term == null) {
                        break;
                    }
                    if (bloomFilter == null) {
                        bloomFilter = bloomFilterFactory.getSetForField(state, fieldInfo);
                        if (bloomFilter == null) {
                            // Field not bloom'd
                            break;
                        }
                        assert bloomFilters.containsKey(fieldInfo) == false;
                        bloomFilters.put(fieldInfo, bloomFilter);
                    }
                    // Make sure there's at least one doc for this term:
                    postingsEnum = termsEnum.postings(postingsEnum, 0);
                    if (postingsEnum.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
                        bloomFilter.addValue(term);
                    }
                }
            }
        }

        private boolean closed;

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            delegateFieldsConsumer.close();

            // Now we are done accumulating values for these fields
            List<Entry<FieldInfo, FuzzySet>> nonSaturatedBlooms = new ArrayList<>();

            for (Entry<FieldInfo, FuzzySet> entry : bloomFilters.entrySet()) {
                FuzzySet bloomFilter = entry.getValue();
                if (!bloomFilterFactory.isSaturated(bloomFilter, entry.getKey())) {
                    nonSaturatedBlooms.add(entry);
                }
            }
            String bloomFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BLOOM_EXTENSION);
            try (IndexOutput bloomOutput = state.directory.createOutput(bloomFileName, state.context)) {
                CodecUtil.writeIndexHeader(bloomOutput, BLOOM_CODEC_NAME, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
                // remember the name of the postings format we will delegate to
                bloomOutput.writeString(delegatePostingsFormat.getName());

                // First field in the output file is the number of fields+blooms saved
                bloomOutput.writeInt(nonSaturatedBlooms.size());
                for (Entry<FieldInfo, FuzzySet> entry : nonSaturatedBlooms) {
                    FieldInfo fieldInfo = entry.getKey();
                    FuzzySet bloomFilter = entry.getValue();
                    bloomOutput.writeInt(fieldInfo.number);
                    saveAppropriatelySizedBloomFilter(bloomOutput, bloomFilter, fieldInfo);
                }
                CodecUtil.writeFooter(bloomOutput);
            }
            // We are done with large bitsets so no need to keep them hanging around
            bloomFilters.clear();
        }

        private void saveAppropriatelySizedBloomFilter(IndexOutput bloomOutput, FuzzySet bloomFilter, FieldInfo fieldInfo)
            throws IOException {

            FuzzySet rightSizedSet = bloomFilterFactory.downsize(fieldInfo, bloomFilter);
            if (rightSizedSet == null) {
                rightSizedSet = bloomFilter;
            }
            rightSizedSet.serialize(bloomOutput);
        }
    }

    @Override
    public String toString() {
        return "BloomFilteringPostingsFormat(" + delegatePostingsFormat + ")";
    }
}

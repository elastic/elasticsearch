/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.codec.postingsformat;

import org.apache.lucene.codecs.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * <p>
 * A {@link PostingsFormat} useful for low doc-frequency fields such as primary
 * keys. Bloom filters are maintained in a ".blm" file which offers "fast-fail"
 * for reads in segments known to have no record of the key. A choice of
 * delegate PostingsFormat is used to record all other Postings data.
 * </p>
 * <p>
 * This is a special bloom filter version, based on {@link BloomFilter} and inspired
 * by Lucene {@link org.apache.lucene.codecs.bloom.BloomFilteringPostingsFormat}.
 * </p>
 */
public final class BloomFilterPostingsFormat extends PostingsFormat {

    public static final String BLOOM_CODEC_NAME = "XBloomFilter"; // the Lucene one is named BloomFilter
    public static final int BLOOM_CODEC_VERSION = 1;

    /**
     * Extension of Bloom Filters file
     */
    static final String BLOOM_EXTENSION = "blm";

    private BloomFilter.Factory bloomFilterFactory = BloomFilter.Factory.DEFAULT;
    private PostingsFormat delegatePostingsFormat;

    /**
     * Creates Bloom filters for a selection of fields created in the index. This
     * is recorded as a set of Bitsets held as a segment summary in an additional
     * "blm" file. This PostingsFormat delegates to a choice of delegate
     * PostingsFormat for encoding all other postings data.
     *
     * @param delegatePostingsFormat The PostingsFormat that records all the non-bloom filter data i.e.
     *                               postings info.
     * @param bloomFilterFactory     The {@link BloomFilter.Factory} responsible for sizing BloomFilters
     *                               appropriately
     */
    public BloomFilterPostingsFormat(PostingsFormat delegatePostingsFormat,
                                     BloomFilter.Factory bloomFilterFactory) {
        super(BLOOM_CODEC_NAME);
        this.delegatePostingsFormat = delegatePostingsFormat;
        this.bloomFilterFactory = bloomFilterFactory;
    }

    // Used only by core Lucene at read-time via Service Provider instantiation -
    // do not use at Write-time in application code.
    public BloomFilterPostingsFormat() {
        super(BLOOM_CODEC_NAME);
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state)
            throws IOException {
        if (delegatePostingsFormat == null) {
            throw new UnsupportedOperationException("Error - " + getClass().getName()
                    + " has been constructed without a choice of PostingsFormat");
        }
        return new BloomFilteredFieldsConsumer(
                delegatePostingsFormat.fieldsConsumer(state), state,
                delegatePostingsFormat);
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state)
            throws IOException {
        return new BloomFilteredFieldsProducer(state);
    }

    public class BloomFilteredFieldsProducer extends FieldsProducer {
        private FieldsProducer delegateFieldsProducer;
        HashMap<String, BloomFilter> bloomsByFieldName = new HashMap<String, BloomFilter>();

        public BloomFilteredFieldsProducer(SegmentReadState state)
                throws IOException {

            String bloomFileName = IndexFileNames.segmentFileName(
                    state.segmentInfo.name, state.segmentSuffix, BLOOM_EXTENSION);
            IndexInput bloomIn = null;
            boolean success = false;
            try {
                bloomIn = state.dir.openInput(bloomFileName, state.context);
                CodecUtil.checkHeader(bloomIn, BLOOM_CODEC_NAME, BLOOM_CODEC_VERSION,
                        BLOOM_CODEC_VERSION);
                // // Load the hash function used in the BloomFilter
                // hashFunction = HashFunction.forName(bloomIn.readString());
                // Load the delegate postings format
                PostingsFormat delegatePostingsFormat = PostingsFormat.forName(bloomIn
                        .readString());

                this.delegateFieldsProducer = delegatePostingsFormat
                        .fieldsProducer(state);
                int numBlooms = bloomIn.readInt();
                for (int i = 0; i < numBlooms; i++) {
                    int fieldNum = bloomIn.readInt();
                    BloomFilter bloom = BloomFilter.deserialize(bloomIn);
                    FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldNum);
                    bloomsByFieldName.put(fieldInfo.name, bloom);
                }
                IOUtils.close(bloomIn);
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
            delegateFieldsProducer.close();
        }

        @Override
        public Terms terms(String field) throws IOException {
            BloomFilter filter = bloomsByFieldName.get(field);
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

        public long getUniqueTermCount() throws IOException {
            return delegateFieldsProducer.getUniqueTermCount();
        }

        public class BloomFilteredTerms extends Terms {
            private Terms delegateTerms;
            private BloomFilter filter;

            public BloomFilteredTerms(Terms terms, BloomFilter filter) {
                this.delegateTerms = terms;
                this.filter = filter;
            }

            public BloomFilter getFilter() {
                return filter;
            }

            @Override
            public TermsEnum intersect(CompiledAutomaton compiled,
                                       final BytesRef startTerm) throws IOException {
                return delegateTerms.intersect(compiled, startTerm);
            }

            @Override
            public TermsEnum iterator(TermsEnum reuse) throws IOException {
                TermsEnum result;
                if ((reuse != null) && (reuse instanceof BloomFilteredTermsEnum)) {
                    // recycle the existing BloomFilteredTermsEnum by asking the delegate
                    // to recycle its contained TermsEnum
                    BloomFilteredTermsEnum bfte = (BloomFilteredTermsEnum) reuse;
                    if (bfte.filter == filter) {
                        bfte.delegateTermsEnum = delegateTerms.iterator(bfte.delegateTermsEnum);
                        return bfte;
                    }
                }
                // We have been handed something we cannot reuse (either null, wrong
                // class or wrong filter) so allocate a new object
                result = new BloomFilteredTermsEnum(delegateTerms.iterator(reuse), filter);
                return result;
            }

            @Override
            public Comparator<BytesRef> getComparator() throws IOException {
                return delegateTerms.getComparator();
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
        }

        class BloomFilteredTermsEnum extends TermsEnum {

            TermsEnum delegateTermsEnum;
            private BloomFilter filter;

            public BloomFilteredTermsEnum(TermsEnum iterator, BloomFilter filter) {
                this.delegateTermsEnum = iterator;
                this.filter = filter;
            }

            @Override
            public final BytesRef next() throws IOException {
                return delegateTermsEnum.next();
            }

            @Override
            public final Comparator<BytesRef> getComparator() {
                return delegateTermsEnum.getComparator();
            }

            @Override
            public final boolean seekExact(BytesRef text, boolean useCache)
                    throws IOException {
                // The magical fail-fast speed up that is the entire point of all of
                // this code - save a disk seek if there is a match on an in-memory
                // structure
                // that may occasionally give a false positive but guaranteed no false
                // negatives
                if (!filter.mightContain(text)) {
                    return false;
                }
                return delegateTermsEnum.seekExact(text, useCache);
            }

            @Override
            public final SeekStatus seekCeil(BytesRef text, boolean useCache)
                    throws IOException {
                return delegateTermsEnum.seekCeil(text, useCache);
            }

            @Override
            public final void seekExact(long ord) throws IOException {
                delegateTermsEnum.seekExact(ord);
            }

            @Override
            public final BytesRef term() throws IOException {
                return delegateTermsEnum.term();
            }

            @Override
            public final long ord() throws IOException {
                return delegateTermsEnum.ord();
            }

            @Override
            public final int docFreq() throws IOException {
                return delegateTermsEnum.docFreq();
            }

            @Override
            public final long totalTermFreq() throws IOException {
                return delegateTermsEnum.totalTermFreq();
            }


            @Override
            public DocsAndPositionsEnum docsAndPositions(Bits liveDocs,
                                                         DocsAndPositionsEnum reuse, int flags) throws IOException {
                return delegateTermsEnum.docsAndPositions(liveDocs, reuse, flags);
            }

            @Override
            public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags)
                    throws IOException {
                return delegateTermsEnum.docs(liveDocs, reuse, flags);
            }


        }

    }

    class BloomFilteredFieldsConsumer extends FieldsConsumer {
        private FieldsConsumer delegateFieldsConsumer;
        private Map<FieldInfo, BloomFilter> bloomFilters = new HashMap<FieldInfo, BloomFilter>();
        private SegmentWriteState state;

        // private PostingsFormat delegatePostingsFormat;

        public BloomFilteredFieldsConsumer(FieldsConsumer fieldsConsumer,
                                           SegmentWriteState state, PostingsFormat delegatePostingsFormat) {
            this.delegateFieldsConsumer = fieldsConsumer;
            // this.delegatePostingsFormat=delegatePostingsFormat;
            this.state = state;
        }

        @Override
        public TermsConsumer addField(FieldInfo field) throws IOException {
            BloomFilter bloomFilter = bloomFilterFactory.createFilter(state.segmentInfo.getDocCount());
            if (bloomFilter != null) {
                assert bloomFilters.containsKey(field) == false;
                bloomFilters.put(field, bloomFilter);
                return new WrappedTermsConsumer(delegateFieldsConsumer.addField(field), bloomFilter);
            } else {
                // No, use the unfiltered fieldsConsumer - we are not interested in
                // recording any term Bitsets.
                return delegateFieldsConsumer.addField(field);
            }
        }

        @Override
        public void close() throws IOException {
            delegateFieldsConsumer.close();
            // Now we are done accumulating values for these fields
            List<Entry<FieldInfo, BloomFilter>> nonSaturatedBlooms = new ArrayList<Map.Entry<FieldInfo, BloomFilter>>();

            for (Entry<FieldInfo, BloomFilter> entry : bloomFilters.entrySet()) {
                BloomFilter bloomFilter = entry.getValue();
                //if (!bloomFilterFactory.isSaturated(bloomFilter, entry.getKey())) {
                nonSaturatedBlooms.add(entry);
                //}
            }
            String bloomFileName = IndexFileNames.segmentFileName(
                    state.segmentInfo.name, state.segmentSuffix, BLOOM_EXTENSION);
            IndexOutput bloomOutput = null;
            try {
                bloomOutput = state.directory
                        .createOutput(bloomFileName, state.context);
                CodecUtil.writeHeader(bloomOutput, BLOOM_CODEC_NAME,
                        BLOOM_CODEC_VERSION);
                // remember the name of the postings format we will delegate to
                bloomOutput.writeString(delegatePostingsFormat.getName());

                // First field in the output file is the number of fields+blooms saved
                bloomOutput.writeInt(nonSaturatedBlooms.size());
                for (Entry<FieldInfo, BloomFilter> entry : nonSaturatedBlooms) {
                    FieldInfo fieldInfo = entry.getKey();
                    BloomFilter bloomFilter = entry.getValue();
                    bloomOutput.writeInt(fieldInfo.number);
                    saveAppropriatelySizedBloomFilter(bloomOutput, bloomFilter, fieldInfo);
                }
            } finally {
                IOUtils.close(bloomOutput);
            }
            //We are done with large bitsets so no need to keep them hanging around
            bloomFilters.clear();
        }

        private void saveAppropriatelySizedBloomFilter(IndexOutput bloomOutput,
                                                       BloomFilter bloomFilter, FieldInfo fieldInfo) throws IOException {

//            FuzzySet rightSizedSet = bloomFilterFactory.downsize(fieldInfo,
//                    bloomFilter);
//            if (rightSizedSet == null) {
//                rightSizedSet = bloomFilter;
//            }
//            rightSizedSet.serialize(bloomOutput);
            BloomFilter.serilaize(bloomFilter, bloomOutput);
        }

    }

    class WrappedTermsConsumer extends TermsConsumer {
        private TermsConsumer delegateTermsConsumer;
        private BloomFilter bloomFilter;

        public WrappedTermsConsumer(TermsConsumer termsConsumer, BloomFilter bloomFilter) {
            this.delegateTermsConsumer = termsConsumer;
            this.bloomFilter = bloomFilter;
        }

        @Override
        public PostingsConsumer startTerm(BytesRef text) throws IOException {
            return delegateTermsConsumer.startTerm(text);
        }

        @Override
        public void finishTerm(BytesRef text, TermStats stats) throws IOException {

            // Record this term in our BloomFilter
            if (stats.docFreq > 0) {
                bloomFilter.put(text);
            }
            delegateTermsConsumer.finishTerm(text, stats);
        }

        @Override
        public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount)
                throws IOException {
            delegateTermsConsumer.finish(sumTotalTermFreq, sumDocFreq, docCount);
        }

        @Override
        public Comparator<BytesRef> getComparator() throws IOException {
            return delegateTermsConsumer.getComparator();
        }

    }

}

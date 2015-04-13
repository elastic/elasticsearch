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

package org.elasticsearch.index.codec.postingsformat;

import org.apache.lucene.codecs.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.*;
import org.apache.lucene.util.*;
import org.elasticsearch.common.util.BloomFilter;

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
 * This is a special bloom filter version, based on {@link org.elasticsearch.common.util.BloomFilter} and inspired
 * by Lucene {@link org.apache.lucene.codecs.bloom.BloomFilteringPostingsFormat}.
 * </p>
 * @deprecated only for reading old segments
 */
@Deprecated
public class BloomFilterPostingsFormat extends PostingsFormat {

    public static final String BLOOM_CODEC_NAME = "XBloomFilter"; // the Lucene one is named BloomFilter
    public static final int BLOOM_CODEC_VERSION = 1;
    public static final int BLOOM_CODEC_VERSION_CHECKSUM = 2;
    public static final int BLOOM_CODEC_VERSION_CURRENT = BLOOM_CODEC_VERSION_CHECKSUM;

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
    public BloomFilteredFieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        throw new UnsupportedOperationException("this codec can only be used for reading");
    }

    @Override
    public BloomFilteredFieldsProducer fieldsProducer(SegmentReadState state)
            throws IOException {
        return new BloomFilteredFieldsProducer(state);
    }

    public PostingsFormat getDelegate() {
        return delegatePostingsFormat;
    }

    private final class LazyBloomLoader implements Accountable {
        private final long offset;
        private final IndexInput indexInput;
        private BloomFilter filter;

        private LazyBloomLoader(long offset, IndexInput origial) {
            this.offset = offset;
            this.indexInput = origial.clone();
        }

        synchronized BloomFilter get() throws IOException {
            if (filter == null) {
                try (final IndexInput input = indexInput) {
                    input.seek(offset);
                    this.filter = BloomFilter.deserialize(input);
                }
            }
            return filter;
        }

        @Override
        public long ramBytesUsed() {
            return filter == null ? 0l : filter.getSizeInBytes();
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.singleton(Accountables.namedAccountable("bloom", ramBytesUsed()));
        }
    }

    public final class BloomFilteredFieldsProducer extends FieldsProducer {
        private FieldsProducer delegateFieldsProducer;
        HashMap<String, LazyBloomLoader> bloomsByFieldName = new HashMap<>();
        private final int version;
        private final IndexInput data;

        // for internal use only
        FieldsProducer getDelegate() {
            return delegateFieldsProducer;
        }

        public BloomFilteredFieldsProducer(SegmentReadState state)
                throws IOException {

            final String bloomFileName = IndexFileNames.segmentFileName(
                    state.segmentInfo.name, state.segmentSuffix, BLOOM_EXTENSION);
            final Directory directory = state.directory;
            IndexInput dataInput = directory.openInput(bloomFileName, state.context);
            try {
                ChecksumIndexInput bloomIn = new BufferedChecksumIndexInput(dataInput.clone());
                version = CodecUtil.checkHeader(bloomIn, BLOOM_CODEC_NAME, BLOOM_CODEC_VERSION,
                        BLOOM_CODEC_VERSION_CURRENT);
                // // Load the hash function used in the BloomFilter
                // hashFunction = HashFunction.forName(bloomIn.readString());
                // Load the delegate postings format
               final String delegatePostings = bloomIn.readString();
                this.delegateFieldsProducer = PostingsFormat.forName(delegatePostings)
                        .fieldsProducer(state);
                this.data = dataInput;
                dataInput = null; // null it out such that we don't close it
            } finally {
                IOUtils.closeWhileHandlingException(dataInput);
            }
        }

        @Override
        public Iterator<String> iterator() {
            return delegateFieldsProducer.iterator();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(data, delegateFieldsProducer);
        }

        @Override
        public Terms terms(String field) throws IOException {
            LazyBloomLoader filter = bloomsByFieldName.get(field);
            if (filter == null) {
                return delegateFieldsProducer.terms(field);
            } else {
                Terms result = delegateFieldsProducer.terms(field);
                if (result == null) {
                    return null;
                }
                return new BloomFilteredTerms(result, filter.get());
            }
        }

        @Override
        public int size() {
            return delegateFieldsProducer.size();
        }

        @Override
        public long ramBytesUsed() {
            long size = delegateFieldsProducer.ramBytesUsed();
            for (LazyBloomLoader bloomFilter : bloomsByFieldName.values()) {
                size += bloomFilter.ramBytesUsed();
            }
            return size;
        }

        @Override
        public Collection<Accountable> getChildResources() {
            List<Accountable> resources = new ArrayList<>();
            resources.addAll(Accountables.namedAccountables("field", bloomsByFieldName));
            if (delegateFieldsProducer != null) {
                resources.add(Accountables.namedAccountable("delegate", delegateFieldsProducer));
            }
            return Collections.unmodifiableList(resources);
        }

        @Override
        public void checkIntegrity() throws IOException {
            delegateFieldsProducer.checkIntegrity();
            if (version >= BLOOM_CODEC_VERSION_CHECKSUM) {
                CodecUtil.checksumEntireFile(data);
            }
        }

        @Override
        public FieldsProducer getMergeInstance() throws IOException {
            return delegateFieldsProducer.getMergeInstance();
        }
    }

    public static final class BloomFilteredTerms extends FilterLeafReader.FilterTerms {
        private BloomFilter filter;

        public BloomFilteredTerms(Terms terms, BloomFilter filter) {
            super(terms);
            this.filter = filter;
        }

        public BloomFilter getFilter() {
            return filter;
        }

        @Override
        public TermsEnum iterator() throws IOException {
            return new BloomFilteredTermsEnum(this.in, filter);
        }
    }

    static final class BloomFilteredTermsEnum extends TermsEnum {

        private Terms delegateTerms;
        private TermsEnum delegateTermsEnum;
        private BloomFilter filter;

        public BloomFilteredTermsEnum(Terms other, BloomFilter filter) {
            this.delegateTerms = other;
            this.filter = filter;
        }

        void reset(Terms others) {
            this.delegateTermsEnum = null;
            this.delegateTerms = others;
        }

        private TermsEnum getDelegate() throws IOException {
            if (delegateTermsEnum == null) {
                /* pull the iterator only if we really need it -
                 * this can be a relatively heavy operation depending on the 
                 * delegate postings format and they underlying directory
                 * (clone IndexInput) */
                delegateTermsEnum = delegateTerms.iterator();
            }
            return delegateTermsEnum;
        }

        @Override
        public final BytesRef next() throws IOException {
            return getDelegate().next();
        }

        @Override
        public final boolean seekExact(BytesRef text)
                throws IOException {
            // The magical fail-fast speed up that is the entire point of all of
            // this code - save a disk seek if there is a match on an in-memory
            // structure
            // that may occasionally give a false positive but guaranteed no false
            // negatives
            if (!filter.mightContain(text)) {
                return false;
            }
            return getDelegate().seekExact(text);
        }

        @Override
        public final SeekStatus seekCeil(BytesRef text)
                throws IOException {
            return getDelegate().seekCeil(text);
        }

        @Override
        public final void seekExact(long ord) throws IOException {
            getDelegate().seekExact(ord);
        }

        @Override
        public final BytesRef term() throws IOException {
            return getDelegate().term();
        }

        @Override
        public final long ord() throws IOException {
            return getDelegate().ord();
        }

        @Override
        public final int docFreq() throws IOException {
            return getDelegate().docFreq();
        }

        @Override
        public final long totalTermFreq() throws IOException {
            return getDelegate().totalTermFreq();
        }


        @Override
        public PostingsEnum postings(Bits liveDocs, PostingsEnum reuse, int flags) throws IOException {
            return getDelegate().postings(liveDocs, reuse, flags);
        }
    }

    // TODO: would be great to move this out to test code, but the interaction between es090 and bloom is complex
    // at least it is not accessible via SPI
    public final class BloomFilteredFieldsConsumer extends FieldsConsumer {
        private final FieldsConsumer delegateFieldsConsumer;
        private final Map<FieldInfo, BloomFilter> bloomFilters = new HashMap<>();
        private final SegmentWriteState state;
        private boolean closed = false;

        // private PostingsFormat delegatePostingsFormat;

        public BloomFilteredFieldsConsumer(FieldsConsumer fieldsConsumer,
                                           SegmentWriteState state, PostingsFormat delegatePostingsFormat) {
            this.delegateFieldsConsumer = fieldsConsumer;
            // this.delegatePostingsFormat=delegatePostingsFormat;
            this.state = state;
        }

        // for internal use only
        public FieldsConsumer getDelegate() {
            return delegateFieldsConsumer;
        }


        @Override
        public void write(Fields fields) throws IOException {

            // Delegate must write first: it may have opened files
            // on creating the class
            // (e.g. Lucene41PostingsConsumer), and write() will
            // close them; alternatively, if we delayed pulling
            // the fields consumer until here, we could do it
            // afterwards:
            delegateFieldsConsumer.write(fields);

            for(String field : fields) {
                Terms terms = fields.terms(field);
                if (terms == null) {
                    continue;
                }
                FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
                TermsEnum termsEnum = terms.iterator();

                BloomFilter bloomFilter = null;

                PostingsEnum postings = null;
                while (true) {
                    BytesRef term = termsEnum.next();
                    if (term == null) {
                        break;
                    }
                    if (bloomFilter == null) {
                        bloomFilter = bloomFilterFactory.createFilter(state.segmentInfo.maxDoc());
                        assert bloomFilters.containsKey(field) == false;
                        bloomFilters.put(fieldInfo, bloomFilter);
                    }
                    // Make sure there's at least one doc for this term:
                    postings = termsEnum.postings(null, postings, 0);
                    if (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        bloomFilter.put(term);
                    }
                }
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            delegateFieldsConsumer.close();
            // Now we are done accumulating values for these fields
            List<Entry<FieldInfo, BloomFilter>> nonSaturatedBlooms = new ArrayList<>();

            for (Entry<FieldInfo, BloomFilter> entry : bloomFilters.entrySet()) {
                nonSaturatedBlooms.add(entry);
            }
            String bloomFileName = IndexFileNames.segmentFileName(
                    state.segmentInfo.name, state.segmentSuffix, BLOOM_EXTENSION);
            IndexOutput bloomOutput = null;
            try {
                bloomOutput = state.directory
                        .createOutput(bloomFileName, state.context);
                CodecUtil.writeHeader(bloomOutput, BLOOM_CODEC_NAME,
                        BLOOM_CODEC_VERSION_CURRENT);
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
                CodecUtil.writeFooter(bloomOutput);
            } finally {
                IOUtils.close(bloomOutput);
            }
            //We are done with large bitsets so no need to keep them hanging around
            bloomFilters.clear();
        }

        private void saveAppropriatelySizedBloomFilter(IndexOutput bloomOutput,
                                                       BloomFilter bloomFilter, FieldInfo fieldInfo) throws IOException {
            BloomFilter.serilaize(bloomFilter, bloomOutput);
        }

    }
}

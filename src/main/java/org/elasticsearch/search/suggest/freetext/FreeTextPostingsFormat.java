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
package org.elasticsearch.search.suggest.freetext;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.XFreeTextSuggester;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.elasticsearch.Version;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.analysis.ShingleTokenFilterFactory;
import org.elasticsearch.index.codec.postingsformat.Elasticsearch090PostingsFormat;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.indices.analysis.PreBuiltAnalyzers;
import org.elasticsearch.search.suggest.SuggestStats;
import org.elasticsearch.search.suggest.SuggestUtils;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class FreeTextPostingsFormat extends PostingsFormat {

    public static final String CODEC_NAME = "freetext";
    public static final int CODEC_VERSION_START = 1;
    public static final int CODEC_VERSION_LATEST = 1;
    public static final String EXTENSION = "ftxt";
    protected static final ESLogger logger = Loggers.getLogger(FreeTextPostingsFormat.class);

    private final PostingsFormat delegatePostingsFormat;

    // dont use, needed for lucene SPI
    public FreeTextPostingsFormat() {
        this(new Elasticsearch090PostingsFormat());
    }

    public FreeTextPostingsFormat(PostingsFormat delegatePostingsFormat) {
        super(CODEC_NAME);
        this.delegatePostingsFormat = delegatePostingsFormat;
    }

    @Override
    public FreeTextFieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        if (delegatePostingsFormat == null) {
            throw new UnsupportedOperationException("Error - " + getClass().getName()
                    + " has been constructed without a choice of PostingsFormat");
        }

        return new FreeTextFieldsConsumer(state);
    }

    @Override
    public FreeTextFieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new FreeTextFieldsProducer(state);
    }

    private class FreeTextFieldsConsumer extends FieldsConsumer {

        private FieldsConsumer delegatesFieldsConsumer;
        private FieldsConsumer suggestFieldsConsumer;

        public FreeTextFieldsConsumer(SegmentWriteState state) throws IOException {
            this.delegatesFieldsConsumer = delegatePostingsFormat.fieldsConsumer(state);
            String suggestFSTFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);
            IndexOutput output = null;
            boolean success = false;
            try {
                output = state.directory.createOutput(suggestFSTFile, state.context);
                CodecUtil.writeHeader(output, CODEC_NAME, CODEC_VERSION_LATEST);
                output.writeString(delegatePostingsFormat.getName());
                this.suggestFieldsConsumer = consumer(output);
                success = true;
            } finally {
                if (!success) {
                    IOUtils.closeWhileHandlingException(output);
                }
            }
        }

        @Override
        public TermsConsumer addField(final FieldInfo field) throws IOException {
            final TermsConsumer delegateConsumer = delegatesFieldsConsumer.addField(field);
            final TermsConsumer suggestTermConsumer = suggestFieldsConsumer.addField(field);
            final GroupedPostingsConsumer groupedPostingsConsumer = new GroupedPostingsConsumer(delegateConsumer, suggestTermConsumer);

            return new TermsConsumer() {
                @Override
                public PostingsConsumer startTerm(BytesRef text) throws IOException {
                    groupedPostingsConsumer.startTerm(text);
                    return groupedPostingsConsumer;
                }

                @Override
                public Comparator<BytesRef> getComparator() throws IOException {
                    return delegateConsumer.getComparator();
                }

                @Override
                public void finishTerm(BytesRef text, TermStats stats) throws IOException {
                    suggestTermConsumer.finishTerm(text, stats);
                    delegateConsumer.finishTerm(text, stats);
                }

                @Override
                public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException {
                    suggestTermConsumer.finish(sumTotalTermFreq, sumDocFreq, docCount);
                    delegateConsumer.finish(sumTotalTermFreq, sumDocFreq, docCount);
                }
            };
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(delegatesFieldsConsumer, suggestFieldsConsumer);
        }
    }

    private class GroupedPostingsConsumer extends PostingsConsumer {

        private TermsConsumer[] termsConsumers;
        private PostingsConsumer[] postingsConsumers;

        public GroupedPostingsConsumer(TermsConsumer... termsConsumersArgs) {
            termsConsumers = termsConsumersArgs;
            postingsConsumers = new PostingsConsumer[termsConsumersArgs.length];
        }

        @Override
        public void startDoc(int docID, int freq) throws IOException {
            for (PostingsConsumer postingsConsumer : postingsConsumers) {
                postingsConsumer.startDoc(docID, freq);
            }
        }

        @Override
        public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
            for (PostingsConsumer postingsConsumer : postingsConsumers) {
                postingsConsumer.addPosition(position, payload, startOffset, endOffset);
            }
        }

        @Override
        public void finishDoc() throws IOException {
            for (PostingsConsumer postingsConsumer : postingsConsumers) {
                postingsConsumer.finishDoc();
            }
        }

        public void startTerm(BytesRef text) throws IOException {
            for (int i = 0; i < termsConsumers.length; i++) {
                postingsConsumers[i] = termsConsumers[i].startTerm(text);
            }
        }
    }

    private static class FreeTextFieldsProducer extends FieldsProducer {

        private final FieldsProducer delegateProducer;
        private final LookupFactory lookupFactory;

        public FreeTextFieldsProducer(SegmentReadState state) throws IOException {
            String suggestFSTFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);
            IndexInput input = state.directory.openInput(suggestFSTFile, state.context);
            CodecUtil.checkHeader(input, CODEC_NAME, CODEC_VERSION_START, CODEC_VERSION_LATEST);
            FieldsProducer delegateProducer = null;
            boolean success = false;
            try {
                PostingsFormat delegatePostingsFormat = PostingsFormat.forName(input.readString());
                delegateProducer = delegatePostingsFormat.fieldsProducer(state);
                if (state.context.context != IOContext.Context.MERGE) {
                    this.lookupFactory = loadLookupFactory(input);
                } else {
                    this.lookupFactory = null;
                }
                this.delegateProducer = delegateProducer;
                success = true;
            } finally {
                if (!success) {
                    IOUtils.closeWhileHandlingException(delegateProducer, input);
                } else {
                    IOUtils.close(input);
                }
            }
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(delegateProducer);
        }

        @Override
        public Iterator<String> iterator() {
            return delegateProducer.iterator();
        }

        @Override
        public Terms terms(String field) throws IOException {
            final Terms terms = delegateProducer.terms(field);
            if (terms == null || lookupFactory == null) {
                return terms;
            }
            return new FreeTextTerms(terms, lookupFactory);
        }

        @Override
        public int size() {
            return delegateProducer.size();
        }

        @Override
        public long ramBytesUsed() {
            return (lookupFactory == null ? 0 : lookupFactory.ramBytesUsed()) + delegateProducer.ramBytesUsed();
        }
    }

    private static FreeTextPostingsFormat.LookupFactory loadLookupFactory(IndexInput input) throws IOException {
        long sizeInBytes = 0;
        int version = CodecUtil.checkHeader(input, CODEC_NAME, CODEC_VERSION_START, CODEC_VERSION_LATEST);
        final Map<String, FST<Long>> lookupMap = new HashMap<String, FST<Long>>();
        input.seek(input.length() - 8);
        long metaPointer = input.readLong();
        input.seek(metaPointer);
        int numFields = input.readVInt();

        Map<Long, String> meta = new TreeMap<Long, String>();
        for (int i = 0; i < numFields; i++) {
            String name = input.readString();
            long offset = input.readVLong();
            meta.put(offset, name);
        }

        for (Map.Entry<Long, String> entry : meta.entrySet()) {
            input.seek(entry.getKey());
            FST<Long> fst = new FST<Long>(input, PositiveIntOutputs.getSingleton());
            sizeInBytes += fst.sizeInBytes();
            lookupMap.put(entry.getValue(), fst);
        }
        final byte separator = input.readByte();
        final long count = input.readVLong();
        final long totalTokens = input.readVLong();
        final long ramBytesUsed = sizeInBytes;
        return new FreeTextPostingsFormat.LookupFactory() {
            @Override
            public Lookup getLookup(FreeTextSuggestionContext suggestionContext) {
                FieldMapper<?> mapper = suggestionContext.mapper();
                FST<Long> fst = lookupMap.get(mapper.names().indexName());
                if (fst == null) {
                    return null;
                }

                Analyzer indexAnalyzer = mapper.indexAnalyzer() == null ? PreBuiltAnalyzers.STANDARD.getAnalyzer(Version.CURRENT) : mapper.indexAnalyzer();
                Analyzer queryAnalyzer = mapper.searchAnalyzer() == null ? PreBuiltAnalyzers.STANDARD.getAnalyzer(Version.CURRENT) : mapper.searchAnalyzer();

                int grams = XFreeTextSuggester.DEFAULT_GRAMS;
                ShingleTokenFilterFactory.Factory shingleFilterFactory = SuggestUtils.getShingleFilterFactory(indexAnalyzer);
                if (shingleFilterFactory != null) {
                    grams = shingleFilterFactory.getMaxShingleSize();
                }

                XFreeTextSuggester suggester = new XFreeTextSuggester(indexAnalyzer, queryAnalyzer, grams, separator, fst, totalTokens, count);
                return suggester;
            }

            @Override
            public long ramBytesUsed() {
                return ramBytesUsed;
            }

            @Override
            public FreeTextStats stats(String... fields) {
                long sizeInBytes = 0;
                ObjectLongOpenHashMap<String> freeTextFields = null;
                if (fields != null  && fields.length > 0) {
                    freeTextFields = new ObjectLongOpenHashMap<String>(fields.length);
                }

                for (Map.Entry<String, FST<Long>> entry : lookupMap.entrySet()) {
                    sizeInBytes += entry.getValue().sizeInBytes();
                    if (fields == null || fields.length == 0) {
                        continue;
                    }
                    for (String field : fields) {
                        // support for getting fields by regex as in fielddata
                        if (Regex.simpleMatch(field, entry.getKey())) {
                            long fstSize = entry.getValue().sizeInBytes();
                            freeTextFields.addTo(field, fstSize);
                        }
                    }
                }

                return new FreeTextStats(sizeInBytes, freeTextFields);
            }
        };
    }


    public static final class FreeTextTerms extends FilterAtomicReader.FilterTerms {
        private final LookupFactory lookup;

        public FreeTextTerms(Terms delegate, LookupFactory lookup) {
            super(delegate);
            this.lookup = lookup;
        }

        public Lookup getLookup(FreeTextSuggestionContext suggestionContext) {
            return lookup.getLookup(suggestionContext);
        }

        public SuggestStats stats(String ... fields) {
            return lookup.stats(fields);
        }
    }

    public FieldsConsumer consumer(final IndexOutput output) throws IOException {
        CodecUtil.writeHeader(output, CODEC_NAME, CODEC_VERSION_LATEST);
        return new FieldsConsumer() {
            private Map<FieldInfo, Long> fieldOffsets = new HashMap<FieldInfo, Long>();

            @Override
            public void close() throws IOException {
                try {
                    long pointer = output.getFilePointer();
                    output.writeVInt(fieldOffsets.size());
                    for (Map.Entry<FieldInfo, Long> entry : fieldOffsets.entrySet()) {
                        output.writeString(entry.getKey().name);
                        output.writeVLong(entry.getValue());
                    }
                    output.writeLong(pointer);
                    output.flush();
                } finally {
                    IOUtils.close(output);
                }
            }

            @Override
            public TermsConsumer addField(final FieldInfo field) throws IOException {

                return new TermsConsumer() {
                    final XFreeTextSuggester.XBuilder builder = new XFreeTextSuggester.XBuilder();
                    final NullPostingsConsumer postingsConsumer = new NullPostingsConsumer();
                    long totalTokens = 0;

                    @Override
                    public PostingsConsumer startTerm(BytesRef text) throws IOException {
                        builder.startTerm(text);
                        return postingsConsumer;
                    }

                    @Override
                    public Comparator<BytesRef> getComparator() throws IOException {
                        return BytesRef.getUTF8SortedAsUnicodeComparator();
                    }

                    @Override
                    public void finishTerm(BytesRef text, TermStats stats) throws IOException {
                        // TODO what about the ngramCount == 1 check from FreeTextSuggester:355 here??
                        totalTokens += stats.totalTermFreq;
                        builder.finishTerm(stats.totalTermFreq);
                    }

                    @Override
                    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException {
                        FST<Long> build = builder.build();
                        assert build != null || docCount == 0 : "the FST is null but docCount is != 0 actual value: [" + docCount + "]";
                        if (build != null) {
                            fieldOffsets.put(field, output.getFilePointer());
                            build.save(output);
                            // write some more meta-info
                            output.writeByte(XFreeTextSuggester.DEFAULT_SEPARATOR);
                            output.writeVLong(postingsConsumer.getCount());
                            output.writeVLong(totalTokens);
                        }
                    }
                };
            }
        };
    }

    public FreeTextStats freeTextStats(IndexReader indexReader, String ... fields) {
        FreeTextStats freeTextStats = new FreeTextStats();
        for (AtomicReaderContext atomicReaderContext : indexReader.leaves()) {
            AtomicReader atomicReader = atomicReaderContext.reader();
            try {
                for (String fieldName : atomicReader.fields()) {
                    Terms terms = atomicReader.fields().terms(fieldName);
                    if (terms instanceof FreeTextTerms) {
                        FreeTextTerms freeTextTerms = (FreeTextTerms) terms;
                        freeTextStats.add(freeTextTerms.stats(fields));
                    }
                }
            } catch (IOException e) {
                logger.error("Could not get freeText stats: {}", e, e.getMessage());
            }
        }

        return freeTextStats;
    }


    private static final class NullPostingsConsumer extends PostingsConsumer {
        private long count = 0;
        @Override
        public void startDoc(int docID, int freq) throws IOException {
        }

        @Override
        public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
        }

        @Override
        public void finishDoc() throws IOException {
            count++;
        }

        public long getCount() { return count; }
    }

    public static abstract class LookupFactory {
        public abstract Lookup getLookup(FreeTextSuggestionContext suggestionContext);
        public abstract long ramBytesUsed();
        public abstract FreeTextStats stats(String ... fields);
    }

}

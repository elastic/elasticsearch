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
package org.elasticsearch.search.suggest.completion2x;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader.FilterTerms;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.store.IOContext.Context;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper2x;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This {@link PostingsFormat} is basically a T-Sink for a default postings
 * format that is used to store postings on disk fitting the lucene APIs and
 * builds a suggest FST as an auxiliary data structure next to the actual
 * postings format. It uses the delegate postings format for simplicity to
 * handle all the merge operations. The auxiliary suggest FST data structure is
 * only loaded if a FieldsProducer is requested for reading, for merging it uses
 * the low memory delegate postings format.
 */
public class Completion090PostingsFormat extends PostingsFormat {

    public static final String CODEC_NAME = "completion090";
    public static final int SUGGEST_CODEC_VERSION = 1;
    public static final int SUGGEST_VERSION_CURRENT = SUGGEST_CODEC_VERSION;
    public static final String EXTENSION = "cmp";

    private final static ESLogger logger = Loggers.getLogger(Completion090PostingsFormat.class);
    private PostingsFormat delegatePostingsFormat;
    private final static Map<String, CompletionLookupProvider> providers;
    private CompletionLookupProvider writeProvider;


    static {
        final CompletionLookupProvider provider = new AnalyzingCompletionLookupProvider(true, true, false);
        providers = Collections.singletonMap(provider.getName(), provider);
    }

    public Completion090PostingsFormat(PostingsFormat delegatePostingsFormat, CompletionLookupProvider provider) {
        super(CODEC_NAME);
        this.delegatePostingsFormat = delegatePostingsFormat;
        this.writeProvider = provider;
        assert delegatePostingsFormat != null && writeProvider != null;
    }

    /*
     * Used only by core Lucene at read-time via Service Provider instantiation
     * do not use at Write-time in application code.
     */
    public Completion090PostingsFormat() {
        super(CODEC_NAME);
    }

    @Override
    public CompletionFieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        if (delegatePostingsFormat == null) {
            throw new UnsupportedOperationException("Error - " + getClass().getName()
                    + " has been constructed without a choice of PostingsFormat");
        }
        assert writeProvider != null;
        return new CompletionFieldsConsumer(state);
    }

    @Override
    public CompletionFieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new CompletionFieldsProducer(state);
    }

    private class CompletionFieldsConsumer extends FieldsConsumer {

        private FieldsConsumer delegatesFieldsConsumer;
        private FieldsConsumer suggestFieldsConsumer;

        public CompletionFieldsConsumer(SegmentWriteState state) throws IOException {
            this.delegatesFieldsConsumer = delegatePostingsFormat.fieldsConsumer(state);
            String suggestFSTFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);
            IndexOutput output = null;
            boolean success = false;
            try {
                output = state.directory.createOutput(suggestFSTFile, state.context);
                CodecUtil.writeHeader(output, CODEC_NAME, SUGGEST_VERSION_CURRENT);
                /*
                 * we write the delegate postings format name so we can load it
                 * without getting an instance in the ctor
                 */
                output.writeString(delegatePostingsFormat.getName());
                output.writeString(writeProvider.getName());
                this.suggestFieldsConsumer = writeProvider.consumer(output);
                success = true;
            } finally {
                if (!success) {
                    IOUtils.closeWhileHandlingException(output);
                }
            }
        }

        @Override
        public void write(Fields fields) throws IOException {
            delegatesFieldsConsumer.write(fields);
            suggestFieldsConsumer.write(fields);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(delegatesFieldsConsumer, suggestFieldsConsumer);
        }
    }

    private static class CompletionFieldsProducer extends FieldsProducer {
        // TODO make this class lazyload all the things in order to take advantage of the new merge instance API
        // today we just load everything up-front
        private final FieldsProducer delegateProducer;
        private final LookupFactory lookupFactory;
        private final int version;

        public CompletionFieldsProducer(SegmentReadState state) throws IOException {
            String suggestFSTFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);
            IndexInput input = state.directory.openInput(suggestFSTFile, state.context);
            version = CodecUtil.checkHeader(input, CODEC_NAME, SUGGEST_CODEC_VERSION, SUGGEST_VERSION_CURRENT);
            FieldsProducer delegateProducer = null;
            boolean success = false;
            try {
                PostingsFormat delegatePostingsFormat = PostingsFormat.forName(input.readString());
                String providerName = input.readString();
                CompletionLookupProvider completionLookupProvider = providers.get(providerName);
                if (completionLookupProvider == null) {
                    throw new IllegalStateException("no provider with name [" + providerName + "] registered");
                }
                // TODO: we could clone the ReadState and make it always forward IOContext.MERGE to prevent unecessary heap usage?
                delegateProducer = delegatePostingsFormat.fieldsProducer(state);
                /*
                 * If we are merging we don't load the FSTs at all such that we
                 * don't consume so much memory during merge
                 */
                if (state.context.context != Context.MERGE) {
                    // TODO: maybe we can do this in a fully lazy fashion based on some configuration
                    // eventually we should have some kind of curciut breaker that prevents us from going OOM here
                    // with some configuration
                    this.lookupFactory = completionLookupProvider.load(input);
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
            return new CompletionTerms(terms, lookupFactory);
        }

        @Override
        public int size() {
            return delegateProducer.size();
        }

        @Override
        public long ramBytesUsed() {
            return (lookupFactory == null ? 0 : lookupFactory.ramBytesUsed()) + delegateProducer.ramBytesUsed();
        }

        @Override
        public Collection<Accountable> getChildResources() {
            List<Accountable> resources = new ArrayList<>();
            if (lookupFactory != null) {
                resources.add(Accountables.namedAccountable("lookup", lookupFactory));
            }
            resources.add(Accountables.namedAccountable("delegate", delegateProducer));
            return Collections.unmodifiableList(resources);
        }

        @Override
        public void checkIntegrity() throws IOException {
            delegateProducer.checkIntegrity();
        }

        @Override
        public FieldsProducer getMergeInstance() throws IOException {
            return delegateProducer.getMergeInstance();
        }
    }

    public static final class CompletionTerms extends FilterTerms {
        private final LookupFactory lookup;

        public CompletionTerms(Terms delegate, LookupFactory lookup) {
            super(delegate);
            this.lookup = lookup;
        }

        public Lookup getLookup(CompletionFieldMapper2x.CompletionFieldType mapper, CompletionSuggestionContext suggestionContext) {
            return lookup.getLookup(mapper, suggestionContext);
        }

        public CompletionStats stats(String ... fields) {
            return lookup.stats(fields);
        }
    }

    public static abstract class CompletionLookupProvider implements PayloadProcessor, CompletionTokenStream.ToFiniteStrings {

        public static final char UNIT_SEPARATOR = '\u001f';

        public abstract FieldsConsumer consumer(IndexOutput output) throws IOException;

        public abstract String getName();

        public abstract LookupFactory load(IndexInput input) throws IOException;

        @Override
        public BytesRef buildPayload(BytesRef surfaceForm, long weight, BytesRef payload) throws IOException {
            if (weight < -1 || weight > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("weight must be >= -1 && <= Integer.MAX_VALUE");
            }
            for (int i = 0; i < surfaceForm.length; i++) {
                if (surfaceForm.bytes[i] == UNIT_SEPARATOR) {
                    throw new IllegalArgumentException(
                            "surface form cannot contain unit separator character U+001F; this character is reserved");
                }
            }
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            OutputStreamDataOutput output = new OutputStreamDataOutput(byteArrayOutputStream);
            output.writeVLong(weight + 1);
            output.writeVInt(surfaceForm.length);
            output.writeBytes(surfaceForm.bytes, surfaceForm.offset, surfaceForm.length);
            output.writeVInt(payload.length);
            output.writeBytes(payload.bytes, 0, payload.length);

            output.close();
            return new BytesRef(byteArrayOutputStream.toByteArray());
        }

        @Override
        public void parsePayload(BytesRef payload, SuggestPayload ref) throws IOException {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(payload.bytes, payload.offset, payload.length);
            InputStreamDataInput input = new InputStreamDataInput(byteArrayInputStream);
            ref.weight = input.readVLong() - 1;
            int len = input.readVInt();
            ref.surfaceForm.grow(len);
            ref.surfaceForm.setLength(len);
            input.readBytes(ref.surfaceForm.bytes(), 0, ref.surfaceForm.length());
            len = input.readVInt();
            ref.payload.grow(len);
            ref.payload.setLength(len);
            input.readBytes(ref.payload.bytes(), 0, ref.payload.length());
            input.close();
        }
    }

    /**
     * Returns total in-heap bytes used by all suggesters.  This method has CPU cost <code>O(numIndexedFields)</code>.
     *
     * @param fieldNamePatterns if non-null, any completion field name matching any of these patterns will break out its in-heap bytes
     * separately in the returned {@link CompletionStats}
     */
    public CompletionStats completionStats(IndexReader indexReader, String ... fieldNamePatterns) {
        CompletionStats completionStats = new CompletionStats();
        for (LeafReaderContext atomicReaderContext : indexReader.leaves()) {
            LeafReader atomicReader = atomicReaderContext.reader();
            try {
                Fields fields = atomicReader.fields();
                for (String fieldName : fields) {
                    Terms terms = fields.terms(fieldName);
                    if (terms instanceof CompletionTerms) {
                        CompletionTerms completionTerms = (CompletionTerms) terms;
                        completionStats.add(completionTerms.stats(fieldNamePatterns));
                    }
                }
            } catch (IOException ioe) {
                logger.error("Could not get completion stats", ioe);
            }
        }

        return completionStats;
    }

    public static abstract class LookupFactory implements Accountable {
        public abstract Lookup getLookup(CompletionFieldMapper2x.CompletionFieldType fieldType,
                                         CompletionSuggestionContext suggestionContext);
        public abstract CompletionStats stats(String ... fields);
        abstract AnalyzingCompletionLookupProvider.AnalyzingSuggestHolder getAnalyzingSuggestHolder(MappedFieldType fieldType);
    }
}

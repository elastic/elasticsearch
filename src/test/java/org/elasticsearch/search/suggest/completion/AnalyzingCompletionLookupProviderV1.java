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

package org.elasticsearch.search.suggest.completion;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.*;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.XAnalyzingSuggester;
import org.apache.lucene.search.suggest.analyzing.XFuzzySuggester;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.fst.*;
import org.apache.lucene.util.fst.PairOutputs.Pair;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.search.suggest.completion.AnalyzingCompletionLookupProvider.AnalyzingSuggestHolder;
import org.elasticsearch.search.suggest.completion.Completion090PostingsFormat.CompletionLookupProvider;
import org.elasticsearch.search.suggest.completion.Completion090PostingsFormat.LookupFactory;
import org.elasticsearch.search.suggest.context.ContextMapping.ContextQuery;

import java.io.IOException;
import java.util.*;
/**
 * This is an older implementation of the AnalyzingCompletionLookupProvider class
 * We use this to test for backwards compatibility in our tests, namely
 * CompletionPostingsFormatTest
 * This ensures upgrades between versions work smoothly
 */
public class AnalyzingCompletionLookupProviderV1 extends CompletionLookupProvider {

    // for serialization
    public static final int SERIALIZE_PRESERVE_SEPERATORS = 1;
    public static final int SERIALIZE_HAS_PAYLOADS = 2;
    public static final int SERIALIZE_PRESERVE_POSITION_INCREMENTS = 4;

    private static final int MAX_SURFACE_FORMS_PER_ANALYZED_FORM = 256;
    private static final int MAX_GRAPH_EXPANSIONS = -1;

    public static final String CODEC_NAME = "analyzing";
    public static final int CODEC_VERSION = 1;

    private boolean preserveSep;
    private boolean preservePositionIncrements;
    private int maxSurfaceFormsPerAnalyzedForm;
    private int maxGraphExpansions;
    private boolean hasPayloads;
    private final XAnalyzingSuggester prototype;

    // important, these are the settings from the old xanalyzingsuggester
    public static final int SEP_LABEL = 0xFF;
    public static final int END_BYTE = 0x0;
    public static final int PAYLOAD_SEP = '\u001f';

    public AnalyzingCompletionLookupProviderV1(boolean preserveSep, boolean exactFirst, boolean preservePositionIncrements, boolean hasPayloads) {
        this.preserveSep = preserveSep;
        this.preservePositionIncrements = preservePositionIncrements;
        this.hasPayloads = hasPayloads;
        this.maxSurfaceFormsPerAnalyzedForm = MAX_SURFACE_FORMS_PER_ANALYZED_FORM;
        this.maxGraphExpansions = MAX_GRAPH_EXPANSIONS;
        int options = preserveSep ? XAnalyzingSuggester.PRESERVE_SEP : 0;
        // needs to fixed in the suggester first before it can be supported
        //options |= exactFirst ? XAnalyzingSuggester.EXACT_FIRST : 0;
        prototype = new XAnalyzingSuggester(null,null, null, options, maxSurfaceFormsPerAnalyzedForm, maxGraphExpansions, preservePositionIncrements,
                null, false, 1, SEP_LABEL, PAYLOAD_SEP, END_BYTE, XAnalyzingSuggester.HOLE_CHARACTER);
    }

    @Override
    public String getName() {
        return "analyzing";
    }

    @Override
    public FieldsConsumer consumer(final IndexOutput output) throws IOException {
        CodecUtil.writeHeader(output, CODEC_NAME, CODEC_VERSION);
        return new FieldsConsumer() {
            private Map<FieldInfo, Long> fieldOffsets = new HashMap<>();

            @Override
            public void close() throws IOException {
                try { /*
                       * write the offsets per field such that we know where
                       * we need to load the FSTs from
                       */
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
                    final XAnalyzingSuggester.XBuilder builder = new XAnalyzingSuggester.XBuilder(maxSurfaceFormsPerAnalyzedForm, hasPayloads, PAYLOAD_SEP);
                    final CompletionPostingsConsumer postingsConsumer = new CompletionPostingsConsumer(AnalyzingCompletionLookupProviderV1.this, builder);

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
                        builder.finishTerm(stats.docFreq); // use  doc freq as a fallback
                    }

                    @Override
                    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException {
                        /*
                         * Here we are done processing the field and we can
                         * buid the FST and write it to disk.
                         */
                        FST<Pair<Long, BytesRef>> build = builder.build();
                        assert build != null || docCount == 0 : "the FST is null but docCount is != 0 actual value: [" + docCount + "]";
                        /*
                         * it's possible that the FST is null if we have 2 segments that get merged
                         * and all docs that have a value in this field are deleted. This will cause
                         * a consumer to be created but it doesn't consume any values causing the FSTBuilder
                         * to return null.
                         */
                        if (build != null) {
                            fieldOffsets.put(field, output.getFilePointer());
                            build.save(output);
                            /* write some more meta-info */
                            output.writeVInt(postingsConsumer.getMaxAnalyzedPathsForOneInput());
                            output.writeVInt(maxSurfaceFormsPerAnalyzedForm);
                            output.writeInt(maxGraphExpansions); // can be negative
                            int options = 0;
                            options |= preserveSep ? SERIALIZE_PRESERVE_SEPERATORS : 0;
                            options |= hasPayloads ? SERIALIZE_HAS_PAYLOADS : 0;
                            options |= preservePositionIncrements ? SERIALIZE_PRESERVE_POSITION_INCREMENTS : 0;
                            output.writeVInt(options);
                        }
                    }
                };
            }
        };
    }

    private static final class CompletionPostingsConsumer extends PostingsConsumer {
        private final SuggestPayload spare = new SuggestPayload();
        private AnalyzingCompletionLookupProviderV1 analyzingSuggestLookupProvider;
        private XAnalyzingSuggester.XBuilder builder;
        private int maxAnalyzedPathsForOneInput = 0;

        public CompletionPostingsConsumer(AnalyzingCompletionLookupProviderV1 analyzingSuggestLookupProvider, XAnalyzingSuggester.XBuilder builder) {
            this.analyzingSuggestLookupProvider = analyzingSuggestLookupProvider;
            this.builder = builder;
        }

        @Override
        public void startDoc(int docID, int freq) throws IOException {
        }

        @Override
        public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
            analyzingSuggestLookupProvider.parsePayload(payload, spare);
            builder.addSurface(spare.surfaceForm.get(), spare.payload.get(), spare.weight);
            // multi fields have the same surface form so we sum up here
            maxAnalyzedPathsForOneInput = Math.max(maxAnalyzedPathsForOneInput, position + 1);
        }

        @Override
        public void finishDoc() throws IOException {
        }

        public int getMaxAnalyzedPathsForOneInput() {
            return maxAnalyzedPathsForOneInput;
        }
    }

    ;


    @Override
    public LookupFactory load(IndexInput input) throws IOException {
        CodecUtil.checkHeader(input, CODEC_NAME, CODEC_VERSION, CODEC_VERSION);
        final Map<String, AnalyzingSuggestHolder> lookupMap = new HashMap<>();
        input.seek(input.length() - 8);
        long metaPointer = input.readLong();
        input.seek(metaPointer);
        int numFields = input.readVInt();

        Map<Long, String> meta = new TreeMap<>();
        for (int i = 0; i < numFields; i++) {
            String name = input.readString();
            long offset = input.readVLong();
            meta.put(offset, name);
        }
        long sizeInBytes = 0;
        for (Map.Entry<Long, String> entry : meta.entrySet()) {
            input.seek(entry.getKey());
            FST<Pair<Long, BytesRef>> fst = new FST<>(input, new PairOutputs<>(
                    PositiveIntOutputs.getSingleton(), ByteSequenceOutputs.getSingleton()));
            int maxAnalyzedPathsForOneInput = input.readVInt();
            int maxSurfaceFormsPerAnalyzedForm = input.readVInt();
            int maxGraphExpansions = input.readInt();
            int options = input.readVInt();
            boolean preserveSep = (options & SERIALIZE_PRESERVE_SEPERATORS) != 0;
            boolean hasPayloads = (options & SERIALIZE_HAS_PAYLOADS) != 0;
            boolean preservePositionIncrements = (options & SERIALIZE_PRESERVE_POSITION_INCREMENTS) != 0;
            sizeInBytes += fst.ramBytesUsed();
            lookupMap.put(entry.getValue(), new AnalyzingSuggestHolder(preserveSep, preservePositionIncrements, maxSurfaceFormsPerAnalyzedForm, maxGraphExpansions,
                    hasPayloads, maxAnalyzedPathsForOneInput, fst));
        }
        final long ramBytesUsed = sizeInBytes;
        return new LookupFactory() {
            @Override
            public Lookup getLookup(CompletionFieldMapper mapper, CompletionSuggestionContext suggestionContext) {
                AnalyzingSuggestHolder analyzingSuggestHolder = lookupMap.get(mapper.names().indexName());
                if (analyzingSuggestHolder == null) {
                    return null;
                }
                int flags = analyzingSuggestHolder.getPreserveSeparator() ? XAnalyzingSuggester.PRESERVE_SEP : 0;

                final Automaton queryPrefix = mapper.requiresContext() ? ContextQuery.toAutomaton(analyzingSuggestHolder.getPreserveSeparator(), suggestionContext.getContextQueries()) : null;

                XAnalyzingSuggester suggester;
                if (suggestionContext.isFuzzy()) {
                    suggester = new XFuzzySuggester(mapper.indexAnalyzer(),queryPrefix, mapper.searchAnalyzer(), flags,
                            analyzingSuggestHolder.maxSurfaceFormsPerAnalyzedForm, analyzingSuggestHolder.maxGraphExpansions,
                            suggestionContext.getFuzzyEditDistance(), suggestionContext.isFuzzyTranspositions(),
                            suggestionContext.getFuzzyPrefixLength(), suggestionContext.getFuzzyMinLength(), false,
                            analyzingSuggestHolder.fst, analyzingSuggestHolder.hasPayloads,
                            analyzingSuggestHolder.maxAnalyzedPathsForOneInput, SEP_LABEL, PAYLOAD_SEP, END_BYTE, XAnalyzingSuggester.HOLE_CHARACTER);
                } else {
                    suggester = new XAnalyzingSuggester(mapper.indexAnalyzer(), queryPrefix, mapper.searchAnalyzer(), flags,
                            analyzingSuggestHolder.maxSurfaceFormsPerAnalyzedForm, analyzingSuggestHolder.maxGraphExpansions,
                            analyzingSuggestHolder.preservePositionIncrements,
                            analyzingSuggestHolder.fst, analyzingSuggestHolder.hasPayloads,
                            analyzingSuggestHolder.maxAnalyzedPathsForOneInput, SEP_LABEL, PAYLOAD_SEP, END_BYTE, XAnalyzingSuggester.HOLE_CHARACTER);
                }
                return suggester;
            }

            @Override
            public CompletionStats stats(String... fields) {
                long sizeInBytes = 0;
                ObjectLongOpenHashMap<String> completionFields = null;
                if (fields != null  && fields.length > 0) {
                    completionFields = new ObjectLongOpenHashMap<>(fields.length);
                }

                for (Map.Entry<String, AnalyzingSuggestHolder> entry : lookupMap.entrySet()) {
                    sizeInBytes += entry.getValue().fst.ramBytesUsed();
                    if (fields == null || fields.length == 0) {
                        continue;
                    }
                    for (String field : fields) {
                        // support for getting fields by regex as in fielddata
                        if (Regex.simpleMatch(field, entry.getKey())) {
                            long fstSize = entry.getValue().fst.ramBytesUsed();
                            completionFields.addTo(field, fstSize);
                        }
                    }
                }

                return new CompletionStats(sizeInBytes, completionFields);
            }
            @Override
            AnalyzingSuggestHolder getAnalyzingSuggestHolder(CompletionFieldMapper mapper) {
                return lookupMap.get(mapper.names().indexName());
            }

            @Override
            public long ramBytesUsed() {
                return ramBytesUsed;
            }
        };
    }

    /*
    // might be readded when we change the current impl, right now not needed
    static class AnalyzingSuggestHolder {
        final boolean preserveSep;
        final boolean preservePositionIncrements;
        final int maxSurfaceFormsPerAnalyzedForm;
        final int maxGraphExpansions;
        final boolean hasPayloads;
        final int maxAnalyzedPathsForOneInput;
        final FST<Pair<Long, BytesRef>> fst;

        public AnalyzingSuggestHolder(boolean preserveSep, boolean preservePositionIncrements, int maxSurfaceFormsPerAnalyzedForm, int maxGraphExpansions,
                                      boolean hasPayloads, int maxAnalyzedPathsForOneInput, FST<Pair<Long, BytesRef>> fst) {
            this.preserveSep = preserveSep;
            this.preservePositionIncrements = preservePositionIncrements;
            this.maxSurfaceFormsPerAnalyzedForm = maxSurfaceFormsPerAnalyzedForm;
            this.maxGraphExpansions = maxGraphExpansions;
            this.hasPayloads = hasPayloads;
            this.maxAnalyzedPathsForOneInput = maxAnalyzedPathsForOneInput;
            this.fst = fst;
        }

    }
    */

    @Override
    public Set<IntsRef> toFiniteStrings(TokenStream stream) throws IOException {
        return prototype.toFiniteStrings(prototype.getTokenStreamToAutomaton(), stream);
    }
}
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

import com.carrotsearch.hppc.ObjectLongHashMap;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.XAnalyzingSuggester;
import org.apache.lucene.search.suggest.analyzing.XFuzzySuggester;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PairOutputs;
import org.apache.lucene.util.fst.PairOutputs.Pair;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper2x;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionContext;
import org.elasticsearch.search.suggest.completion.FuzzyOptions;
import org.elasticsearch.search.suggest.completion2x.Completion090PostingsFormat.CompletionLookupProvider;
import org.elasticsearch.search.suggest.completion2x.Completion090PostingsFormat.LookupFactory;
import org.elasticsearch.search.suggest.completion2x.context.ContextMapping.ContextQuery;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class AnalyzingCompletionLookupProvider extends CompletionLookupProvider {

    // for serialization
    public static final int SERIALIZE_PRESERVE_SEPARATORS = 1;
    public static final int SERIALIZE_HAS_PAYLOADS = 2;
    public static final int SERIALIZE_PRESERVE_POSITION_INCREMENTS = 4;

    private static final int MAX_SURFACE_FORMS_PER_ANALYZED_FORM = 256;
    private static final int MAX_GRAPH_EXPANSIONS = -1;

    public static final String CODEC_NAME = "analyzing";
    public static final int CODEC_VERSION_START = 1;
    public static final int CODEC_VERSION_SERIALIZED_LABELS = 2;
    public static final int CODEC_VERSION_CHECKSUMS = 3;
    public static final int CODEC_VERSION_LATEST = CODEC_VERSION_CHECKSUMS;

    private final boolean preserveSep;
    private final boolean preservePositionIncrements;
    private final int maxSurfaceFormsPerAnalyzedForm;
    private final int maxGraphExpansions;
    private final boolean hasPayloads;
    private final XAnalyzingSuggester prototype;

    public AnalyzingCompletionLookupProvider(boolean preserveSep, boolean preservePositionIncrements, boolean hasPayloads) {
        this.preserveSep = preserveSep;
        this.preservePositionIncrements = preservePositionIncrements;
        this.hasPayloads = hasPayloads;
        this.maxSurfaceFormsPerAnalyzedForm = MAX_SURFACE_FORMS_PER_ANALYZED_FORM;
        this.maxGraphExpansions = MAX_GRAPH_EXPANSIONS;
        int options = preserveSep ? XAnalyzingSuggester.PRESERVE_SEP : 0;
        // needs to fixed in the suggester first before it can be supported
        //options |= exactFirst ? XAnalyzingSuggester.EXACT_FIRST : 0;
        prototype = new XAnalyzingSuggester(null, null, null, options, maxSurfaceFormsPerAnalyzedForm, maxGraphExpansions,
            preservePositionIncrements, null, false, 1, XAnalyzingSuggester.SEP_LABEL, XAnalyzingSuggester.PAYLOAD_SEP,
            XAnalyzingSuggester.END_BYTE, XAnalyzingSuggester.HOLE_CHARACTER);
    }

    @Override
    public String getName() {
        return "analyzing";
    }

    public boolean getPreserveSep() {
        return preserveSep;
    }

    public boolean getPreservePositionsIncrements() {
        return preservePositionIncrements;
    }

    public boolean hasPayloads() {
        return hasPayloads;
    }

    @Override
    public FieldsConsumer consumer(final IndexOutput output) throws IOException {
        CodecUtil.writeHeader(output, CODEC_NAME, CODEC_VERSION_LATEST);
        return new FieldsConsumer() {
            private Map<String, Long> fieldOffsets = new HashMap<>();

            @Override
            public void close() throws IOException {
                try {
                  /*
                   * write the offsets per field such that we know where
                   * we need to load the FSTs from
                   */
                    long pointer = output.getFilePointer();
                    output.writeVInt(fieldOffsets.size());
                    for (Map.Entry<String, Long> entry : fieldOffsets.entrySet()) {
                        output.writeString(entry.getKey());
                        output.writeVLong(entry.getValue());
                    }
                    output.writeLong(pointer);
                    CodecUtil.writeFooter(output);
                } finally {
                    IOUtils.close(output);
                }
            }

            @Override
            public void write(Fields fields) throws IOException {
                for(String field : fields) {
                    Terms terms = fields.terms(field);
                    if (terms == null) {
                        continue;
                    }
                    TermsEnum termsEnum = terms.iterator();
                    PostingsEnum docsEnum = null;
                    final SuggestPayload spare = new SuggestPayload();
                    int maxAnalyzedPathsForOneInput = 0;
                    final XAnalyzingSuggester.XBuilder builder = new XAnalyzingSuggester.XBuilder(
                        maxSurfaceFormsPerAnalyzedForm, hasPayloads, XAnalyzingSuggester.PAYLOAD_SEP);
                    int docCount = 0;
                    while (true) {
                        BytesRef term = termsEnum.next();
                        if (term == null) {
                            break;
                        }
                        docsEnum = termsEnum.postings(docsEnum, PostingsEnum.PAYLOADS);
                        builder.startTerm(term);
                        int docFreq = 0;
                        while (docsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                            for (int i = 0; i < docsEnum.freq(); i++) {
                                final int position = docsEnum.nextPosition();
                                AnalyzingCompletionLookupProvider.this.parsePayload(docsEnum.getPayload(), spare);
                                builder.addSurface(spare.surfaceForm.get(), spare.payload.get(), spare.weight);
                                // multi fields have the same surface form so we sum up here
                                maxAnalyzedPathsForOneInput = Math.max(maxAnalyzedPathsForOneInput, position + 1);
                            }
                            docFreq++;
                            docCount = Math.max(docCount, docsEnum.docID()+1);
                        }
                        builder.finishTerm(docFreq);
                    }
                    /*
                     * Here we are done processing the field and we can
                     * buid the FST and write it to disk.
                     */
                    FST<Pair<Long, BytesRef>> build = builder.build();
                    assert build != null || docCount == 0: "the FST is null but docCount is != 0 actual value: [" + docCount + "]";
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
                        output.writeVInt(maxAnalyzedPathsForOneInput);
                        output.writeVInt(maxSurfaceFormsPerAnalyzedForm);
                        output.writeInt(maxGraphExpansions); // can be negative
                        int options = 0;
                        options |= preserveSep ? SERIALIZE_PRESERVE_SEPARATORS : 0;
                        options |= hasPayloads ? SERIALIZE_HAS_PAYLOADS : 0;
                        options |= preservePositionIncrements ? SERIALIZE_PRESERVE_POSITION_INCREMENTS : 0;
                        output.writeVInt(options);
                        output.writeVInt(XAnalyzingSuggester.SEP_LABEL);
                        output.writeVInt(XAnalyzingSuggester.END_BYTE);
                        output.writeVInt(XAnalyzingSuggester.PAYLOAD_SEP);
                        output.writeVInt(XAnalyzingSuggester.HOLE_CHARACTER);
                    }
                }
            }
        };
    }


    @Override
    public LookupFactory load(IndexInput input) throws IOException {
        long sizeInBytes = 0;
        int version = CodecUtil.checkHeader(input, CODEC_NAME, CODEC_VERSION_START, CODEC_VERSION_LATEST);
        if (version >= CODEC_VERSION_CHECKSUMS) {
            CodecUtil.checksumEntireFile(input);
        }
        final long metaPointerPosition = input.length() - (version >= CODEC_VERSION_CHECKSUMS? 8 + CodecUtil.footerLength() : 8);
        final Map<String, AnalyzingSuggestHolder> lookupMap = new HashMap<>();
        input.seek(metaPointerPosition);
        long metaPointer = input.readLong();
        input.seek(metaPointer);
        int numFields = input.readVInt();

        Map<Long, String> meta = new TreeMap<>();
        for (int i = 0; i < numFields; i++) {
            String name = input.readString();
            long offset = input.readVLong();
            meta.put(offset, name);
        }

        for (Map.Entry<Long, String> entry : meta.entrySet()) {
            input.seek(entry.getKey());
            FST<Pair<Long, BytesRef>> fst = new FST<>(input, new PairOutputs<>(
                    PositiveIntOutputs.getSingleton(), ByteSequenceOutputs.getSingleton()));
            int maxAnalyzedPathsForOneInput = input.readVInt();
            int maxSurfaceFormsPerAnalyzedForm = input.readVInt();
            int maxGraphExpansions = input.readInt();
            int options = input.readVInt();
            boolean preserveSep = (options & SERIALIZE_PRESERVE_SEPARATORS) != 0;
            boolean hasPayloads = (options & SERIALIZE_HAS_PAYLOADS) != 0;
            boolean preservePositionIncrements = (options & SERIALIZE_PRESERVE_POSITION_INCREMENTS) != 0;

            // first version did not include these three fields, so fall back to old default (before the analyzingsuggester
            // was updated in Lucene, so we cannot use the suggester defaults)
            int sepLabel, payloadSep, endByte, holeCharacter;
            switch (version) {
                case CODEC_VERSION_START:
                    sepLabel = 0xFF;
                    payloadSep = '\u001f';
                    endByte = 0x0;
                    holeCharacter = '\u001E';
                    break;
                default:
                    sepLabel = input.readVInt();
                    endByte = input.readVInt();
                    payloadSep = input.readVInt();
                    holeCharacter = input.readVInt();
            }

            AnalyzingSuggestHolder holder = new AnalyzingSuggestHolder(preserveSep, preservePositionIncrements,
                maxSurfaceFormsPerAnalyzedForm, maxGraphExpansions, hasPayloads, maxAnalyzedPathsForOneInput,
                fst, sepLabel, payloadSep, endByte, holeCharacter);
            sizeInBytes += fst.ramBytesUsed();
            lookupMap.put(entry.getValue(), holder);
        }
        final long ramBytesUsed = sizeInBytes;
        return new LookupFactory() {
            @Override
            public Lookup getLookup(CompletionFieldMapper2x.CompletionFieldType fieldType, CompletionSuggestionContext suggestionContext) {
                AnalyzingSuggestHolder analyzingSuggestHolder = lookupMap.get(fieldType.name());
                if (analyzingSuggestHolder == null) {
                    return null;
                }
                int flags = analyzingSuggestHolder.getPreserveSeparator() ? XAnalyzingSuggester.PRESERVE_SEP : 0;

                final XAnalyzingSuggester suggester;
                final Automaton queryPrefix = fieldType.requiresContext() ?
                    ContextQuery.toAutomaton(analyzingSuggestHolder.getPreserveSeparator(), suggestionContext.getContextQueries()) : null;

                final FuzzyOptions fuzzyOptions = suggestionContext.getFuzzyOptions();
                if (fuzzyOptions != null) {
                    suggester = new XFuzzySuggester(fieldType.indexAnalyzer(), queryPrefix, fieldType.searchAnalyzer(), flags,
                        analyzingSuggestHolder.maxSurfaceFormsPerAnalyzedForm, analyzingSuggestHolder.maxGraphExpansions,
                        fuzzyOptions.getEditDistance(), fuzzyOptions.isTranspositions(),
                        fuzzyOptions.getFuzzyPrefixLength(), fuzzyOptions.getFuzzyMinLength(), fuzzyOptions.isUnicodeAware(),
                        analyzingSuggestHolder.fst, analyzingSuggestHolder.hasPayloads,
                        analyzingSuggestHolder.maxAnalyzedPathsForOneInput, analyzingSuggestHolder.sepLabel,
                        analyzingSuggestHolder.payloadSep, analyzingSuggestHolder.endByte,
                        analyzingSuggestHolder.holeCharacter);
                } else {
                    suggester = new XAnalyzingSuggester(fieldType.indexAnalyzer(), queryPrefix, fieldType.searchAnalyzer(), flags,
                        analyzingSuggestHolder.maxSurfaceFormsPerAnalyzedForm, analyzingSuggestHolder.maxGraphExpansions,
                        analyzingSuggestHolder.preservePositionIncrements, analyzingSuggestHolder.fst, analyzingSuggestHolder.hasPayloads,
                        analyzingSuggestHolder.maxAnalyzedPathsForOneInput, analyzingSuggestHolder.sepLabel,
                        analyzingSuggestHolder.payloadSep, analyzingSuggestHolder.endByte, analyzingSuggestHolder.holeCharacter);
                }
                return suggester;
            }

            @Override
            public CompletionStats stats(String... fields) {
                long sizeInBytes = 0;
                ObjectLongHashMap<String> completionFields = null;
                if (fields != null  && fields.length > 0) {
                    completionFields = new ObjectLongHashMap<>(fields.length);
                }

                for (Map.Entry<String, AnalyzingSuggestHolder> entry : lookupMap.entrySet()) {
                    sizeInBytes += entry.getValue().fst.ramBytesUsed();
                    if (fields == null || fields.length == 0) {
                        continue;
                    }
                    if (Regex.simpleMatch(fields, entry.getKey())) {
                        long fstSize = entry.getValue().fst.ramBytesUsed();
                        completionFields.addTo(entry.getKey(), fstSize);
                    }
                }

                return new CompletionStats(sizeInBytes, completionFields);
            }

            @Override
            AnalyzingSuggestHolder getAnalyzingSuggestHolder(MappedFieldType fieldType) {
                return lookupMap.get(fieldType.name());
            }

            @Override
            public long ramBytesUsed() {
                return ramBytesUsed;
            }

            @Override
            public Collection<Accountable> getChildResources() {
                return Accountables.namedAccountables("field", lookupMap);
            }
        };
    }

    static class AnalyzingSuggestHolder implements Accountable {
        final boolean preserveSep;
        final boolean preservePositionIncrements;
        final int maxSurfaceFormsPerAnalyzedForm;
        final int maxGraphExpansions;
        final boolean hasPayloads;
        final int maxAnalyzedPathsForOneInput;
        final FST<Pair<Long, BytesRef>> fst;
        final int sepLabel;
        final int payloadSep;
        final int endByte;
        final int holeCharacter;

        public AnalyzingSuggestHolder(boolean preserveSep, boolean preservePositionIncrements,
                                      int maxSurfaceFormsPerAnalyzedForm, int maxGraphExpansions,
                                      boolean hasPayloads, int maxAnalyzedPathsForOneInput,
                                      FST<Pair<Long, BytesRef>> fst, int sepLabel, int payloadSep,
                                      int endByte, int holeCharacter) {
            this.preserveSep = preserveSep;
            this.preservePositionIncrements = preservePositionIncrements;
            this.maxSurfaceFormsPerAnalyzedForm = maxSurfaceFormsPerAnalyzedForm;
            this.maxGraphExpansions = maxGraphExpansions;
            this.hasPayloads = hasPayloads;
            this.maxAnalyzedPathsForOneInput = maxAnalyzedPathsForOneInput;
            this.fst = fst;
            this.sepLabel = sepLabel;
            this.payloadSep = payloadSep;
            this.endByte = endByte;
            this.holeCharacter = holeCharacter;
        }

        public boolean getPreserveSeparator() {
            return preserveSep;
        }

        public boolean getPreservePositionIncrements() {
            return preservePositionIncrements;
        }

        public boolean hasPayloads() {
            return hasPayloads;
        }

        @Override
        public long ramBytesUsed() {
            if (fst != null) {
                return fst.ramBytesUsed();
            } else {
                return 0;
            }
        }

        @Override
        public Collection<Accountable> getChildResources() {
            if (fst != null) {
                return Collections.singleton(Accountables.namedAccountable("fst", fst));
            } else {
                return Collections.emptyList();
            }
        }
    }

    @Override
    public Set<IntsRef> toFiniteStrings(TokenStream stream) throws IOException {
        return prototype.toFiniteStrings(stream);
    }


}

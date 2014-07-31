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

package org.apache.lucene.search.suggest.analyzing;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.*;
import org.apache.lucene.index.FilterLeafReader.FilterTerms;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.suggest.analyzing.LookupPostingsFormat.LookupBuildConfiguration.LOOKUP_TYPE;
import org.apache.lucene.search.suggest.analyzing.SegmentLookup.SegmentLookupWrapper;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.*;

/**
 * Responsible for configuration, creation and retrieval of lookup fields
 * associated with the index.
 *
 * Configuration:
 *   use {@link #register(String, LookupBuildConfiguration)} to register build configuration
 *   for a lookup field
 *
 * Creation:
 *   see {@link LookupFieldsConsumer}
 *
 * Retrieval:
 *   see {@link LookupFieldsProducer}
 *
 */
public class LookupPostingsFormat extends PostingsFormat {

    /**
     * Holds build configuration for a single lookup field
     */
    final static class LookupBuildConfiguration {
        final ContextAwareScorer scorer;
        final Analyzer queryAnalyzer;
        final boolean preserveSep;
        final boolean preservePositionIncrements;
        final PayloadProcessor payloadProcessor;

        LookupBuildConfiguration(ContextAwareScorer scorer, Analyzer queryAnalyzer, boolean preserveSep,
                                 boolean preservePositionIncrements, PayloadProcessor payloadProcessor) {
            this.scorer = scorer;
            this.queryAnalyzer = queryAnalyzer;
            this.preserveSep = preserveSep;
            this.preservePositionIncrements = preservePositionIncrements;
            this.payloadProcessor = payloadProcessor;
        }

        private LOOKUP_TYPE type() {
            if (scorer instanceof ContextAwareScorer.BytesBased) {
                return LOOKUP_TYPE.BYTES;
            } else if (scorer instanceof ContextAwareScorer.LongBased) {
                return LOOKUP_TYPE.LONG;
            } else {
                // should never happen
                throw new IllegalStateException("lookup is not long or bytes based");
            }

        }

        static enum LOOKUP_TYPE {
            LONG((byte) 0),
            BYTES((byte) 1);

            private byte id;

            LOOKUP_TYPE(byte id) {
                this.id = id;
            }

            public static LOOKUP_TYPE fromId(byte id) {
                switch (id) {
                    case 0:
                        return LONG;
                    case 1:
                        return BYTES;
                    default:
                        throw new IllegalArgumentException("undefined type id " + id);
                }
            }
        }
    }

    private static Map<String, LookupBuildConfiguration> buildConfigurationMap = new HashMap<>();

    /**
     * Registers a build configuration for a particular lookup field
     *
     * @param name of the lookup field
     * @param lookupBuildConfiguration associated configuration
     */
    public static void register(String name, LookupBuildConfiguration lookupBuildConfiguration) {
        buildConfigurationMap.put(name, lookupBuildConfiguration);
    }

    private static LookupBuildConfiguration buildConfiguration(String name) {
        return buildConfigurationMap.get(name);
    }

    public static final String CODEC_NAME = "lookup";
    public static final int LOOKUP_CODEC_VERSION = 1;
    public static final int LOOKUP_VERSION_CURRENT = LOOKUP_CODEC_VERSION;
    public static final String EXTENSION = "cmp";

    /**
     */
    public LookupPostingsFormat() {
        super(CODEC_NAME);
    }

    private PostingsFormat delegatePostingsFormat;

    public LookupPostingsFormat(PostingsFormat delegatePostingsFormat) {
        super(CODEC_NAME);
        this.delegatePostingsFormat = delegatePostingsFormat;
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        if (delegatePostingsFormat == null) {
            throw new UnsupportedOperationException("Error - " + getClass().getName()
                    + " has been constructed without a choice of PostingsFormat");
        }
        return new LookupFieldsConsumer(state);
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new LookupFieldsProducer(state);
    }

    /**
     * Builds Lookup Index with {@link NRTSuggesterBuilder} using {@link LookupBuildConfiguration}
     * for the associated lookup field.
     * NOTE: the merging of the delegate field is used to build a merged lookup index
     *
     * TODO: the implementation should be pluggable, so we can support other implementations of SegmentLookup
     *
     * Lookup Index format:
     * - Header
     * - Delegate postings format name
     * - FST + configuration for each lookup field built with {@link NRTSuggesterBuilder}
     * - Map of lookup field names to lookup index and its type
     *    - maps a lookup field name to the lookup index file offset and a byte (representing lookup type)
     * -
     */
    private class LookupFieldsConsumer extends FieldsConsumer {

        private final FieldsConsumer delegateFieldsConsumer;
        private IndexOutput output = null;
        private final Map<String, Map.Entry<Long, Byte>> fieldOffsets;

        public LookupFieldsConsumer(SegmentWriteState state) throws IOException {
            this.delegateFieldsConsumer = delegatePostingsFormat.fieldsConsumer(state);
            fieldOffsets = new HashMap<>();
            String suggestFSTFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);
            boolean success = false;
            try {
                output = state.directory.createOutput(suggestFSTFile, state.context);
                CodecUtil.writeHeader(output, CODEC_NAME, LOOKUP_VERSION_CURRENT);
                /*
                 * we write the delegate postings format name so we can load it
                 * without getting an instance in the ctor
                 */
                output.writeString(delegatePostingsFormat.getName());
                success = true;
            } finally {
                if (!success) {
                    IOUtils.closeWhileHandlingException(output);
                }
            }
        }

        @Override
        public void write(Fields fields) throws IOException {
            delegateFieldsConsumer.write(fields);
            if (output != null) {
                for (String field : fields) {
                    Terms terms = fields.terms(field);
                    if (terms == null) {
                        continue;
                    }
                    LookupBuildConfiguration buildConfiguration = buildConfiguration(field);
                    if (buildConfiguration == null) {
                        continue;
                    }
                    TermsEnum termsEnum = terms.iterator(null);
                    DocsAndPositionsEnum docsEnum = null;
                    final NRTSuggesterBuilder builder = NRTSuggesterBuilder.fromConfiguration(buildConfiguration);
                    int docCount = 0;

                    while (true) {
                        BytesRef term = termsEnum.next();
                        if (term == null) {
                            break;
                        }
                        docsEnum = termsEnum.docsAndPositions(null, docsEnum, DocsAndPositionsEnum.FLAG_PAYLOADS);
                        builder.startTerm(term);
                        int docFreq = 0;
                        while (docsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                            int docID = docsEnum.docID();
                            for (int i = 0; i < docsEnum.freq(); i++) {
                                docsEnum.nextPosition();
                                builder.addEntry(docID, docsEnum.getPayload());
                            }
                            docFreq++;
                            docCount = Math.max(docCount, docsEnum.docID() + 1);
                        }
                        builder.finishTerm();
                    }

                    /*
                     * Here we are done processing the field and we can
                     * buid the FST and write it to disk.
                     */
                    final long suggesterFilePointer = output.getFilePointer();
                    if (builder.store(output)) {
                        byte type = buildConfiguration.type().id;
                        Map.Entry<Long, Byte> entry = new AbstractMap.SimpleEntry<>(suggesterFilePointer, type);
                        fieldOffsets.put(field, entry);
                    } else {
                        assert docCount == 0 : "the FST is null but docCount is != 0 actual value: [" + docCount + "]";
                    }
                }
            }
        }

        @Override
        public void close() throws IOException {
            delegateFieldsConsumer.close();
            try {
                  /*
                   * write the offsets per field such that we know where
                   * we need to load the FSTs from
                   */
                long pointer = output.getFilePointer();
                output.writeVInt(fieldOffsets.size());
                for (Map.Entry<String, Map.Entry<Long, Byte>> entry : fieldOffsets.entrySet()) {
                    output.writeString(entry.getKey());
                    output.writeByte(entry.getValue().getValue());
                    output.writeVLong(entry.getValue().getKey());
                }
                output.writeLong(pointer);
                CodecUtil.writeFooter(output);
            } finally {
                IOUtils.close(output);
            }
        }
    }

    /**
     * Produces a lookup for a particular lookup field
     *
     * On initialization loads the lookup index mapping (field name -> (file offset, type))
     * Exposes the lookup through {@link #terms(String)}
     *
     * On merge, none of the lookup indexes are loaded
     *
     * Note: the lookup index for a field is only loaded the first time {@link #terms(String)} is
     * called on a lookup field
     */
    private static class LookupFieldsProducer extends FieldsProducer {
        private FieldsProducer delegateFieldsProducer;
        private final int version;
        private final IndexInput input;
        private final LookupFactory lookupFactory;
        private Map<String, Map.Entry<Long, Byte>> fieldOffsets;

        public LookupFieldsProducer(SegmentReadState state) throws IOException {
            String suggestFSTFile = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);
            input = state.directory.openInput(suggestFSTFile, state.context);
            version = CodecUtil.checkHeader(input, CODEC_NAME, LOOKUP_CODEC_VERSION, LOOKUP_VERSION_CURRENT);
            delegateFieldsProducer = null;
            fieldOffsets = null;
            boolean success = false;
            try {
                PostingsFormat delegatePostingsFormat = PostingsFormat.forName(input.readString());
                delegateFieldsProducer = delegatePostingsFormat.fieldsProducer(state);
                CodecUtil.checksumEntireFile(input);
                final long metaPointerPosition = input.length() - (8 + CodecUtil.footerLength());
                input.seek(metaPointerPosition);
                long metaPointer = input.readLong();
                input.seek(metaPointer);
                int numFields = input.readVInt();

                fieldOffsets = new HashMap<>();
                for (int i = 0; i < numFields; i++) {
                    String name = input.readString();
                    byte type = input.readByte();
                    long offset = input.readVLong();
                    fieldOffsets.put(name, new AbstractMap.SimpleEntry<>(offset, type));
                }
                lookupFactory = new LookupFactory(fieldOffsets, input);
                success = true;
            } finally {
                if (!success) {
                    IOUtils.closeWhileHandlingException(delegateFieldsProducer, input);
                }
            }
        }

        @Override
        public void close() throws IOException {
            this.delegateFieldsProducer.close();
            IOUtils.close(input);
        }

        @Override
        public void checkIntegrity() throws IOException {
            this.delegateFieldsProducer.checkIntegrity();
        }

        @Override
        public long ramBytesUsed() {
            return this.delegateFieldsProducer.ramBytesUsed() + lookupFactory.ramBytesUsed();
        }

        @Override
        public Collection<Accountable> getChildResources() {
            List<Accountable> accountables = new ArrayList<>(delegateFieldsProducer.getChildResources());
            accountables.addAll(lookupFactory.getChildResources());
            return accountables;
        }

        @Override
        public Iterator<String> iterator() {
            return delegateFieldsProducer.iterator();
        }

        @Override
        public Terms terms(String field) throws IOException {
            return new LookupTerms(delegateFieldsProducer.terms(field), lookupFactory.get(field));
        }

        @Override
        public FieldsProducer getMergeInstance() throws IOException {
            return delegateFieldsProducer.getMergeInstance();
        }

        @Override
        public int size() {
            return delegateFieldsProducer.size();
        }
    }

    public static class LookupTerms extends FilterTerms {
        private final SegmentLookupWrapper lookup;

        public LookupTerms(Terms in, SegmentLookupWrapper lookup) {
            super(in);
            this.lookup = lookup;
        }

        public SegmentLookup.LongBased longScoreLookup() {
            if (lookup instanceof SegmentLookup.LongBased) {
                return (SegmentLookup.LongBased) lookup;
            }
            throw new IllegalStateException("the lookup is not long based");
        }

        public SegmentLookup.BytesBased bytesScoreLookup() {
            if (lookup instanceof SegmentLookup.BytesBased) {
                return (SegmentLookup.BytesBased) lookup;
            }
            throw new IllegalStateException("the lookup is not bytes based");
        }
    }


    private static class LookupFactory implements Accountable {
        private final Map<String, Map.Entry<Long, Byte>> fieldOffsets;
        private final IndexInput input;
        private final Map<String, SegmentLookupWrapper> lookupMap;

        LookupFactory(Map<String, Map.Entry<Long, Byte>> fieldOffsets, IndexInput input) {
            this.fieldOffsets = fieldOffsets;
            this.input = input;
            this.lookupMap = new HashMap<>(fieldOffsets.size());
        }

        SegmentLookupWrapper get(String field) throws IOException {
            if (fieldOffsets.containsKey(field)) {
                if (!lookupMap.containsKey(field)) {
                    final Map.Entry<Long, Byte> offsetAndTypeEntry = fieldOffsets.get(field);
                    LookupBuildConfiguration buildConfiguration = buildConfiguration(field);
                    input.seek(offsetAndTypeEntry.getKey());
                    switch (LOOKUP_TYPE.fromId(offsetAndTypeEntry.getValue())) {
                        case LONG:
                            lookupMap.put(field, new SegmentLookup.LongBased(NRTSuggester.<Long>load(input), buildConfiguration.queryAnalyzer));
                            break;
                        case BYTES:
                            lookupMap.put(field, new SegmentLookup.BytesBased(NRTSuggester.<BytesRef>load(input), buildConfiguration.queryAnalyzer));
                            break;
                    }
                }
                return lookupMap.get(field);
            } else {
                throw new IllegalArgumentException("no Lookup found for field: " + field);
            }
        }

        Set<String> fields() {
            return fieldOffsets.keySet();
        }

        @Override
        public long ramBytesUsed() {
            long ramBytesUsed = 0;
            for (SegmentLookup segmentLookup : lookupMap.values()) {
                ramBytesUsed += segmentLookup.ramBytesUsed();
            }
            return ramBytesUsed;
        }

        @Override
        public Collection<Accountable> getChildResources() {
            List<Accountable> accountables = new ArrayList<>();
            for (SegmentLookup segmentLookup : lookupMap.values()) {
                accountables.addAll(segmentLookup.getChildResources());
            }
            return accountables;
        }


    }
}

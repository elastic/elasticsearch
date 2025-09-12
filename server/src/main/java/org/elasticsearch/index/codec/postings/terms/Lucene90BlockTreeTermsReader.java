/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.postings.terms;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.lucene90.blocktree.Lucene90BlockTreeTermsWriter;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.internal.hppc.IntCursor;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.Outputs;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A block-based terms index and dictionary that assigns terms to variable length blocks according
 * to how they share prefixes. The terms index is a prefix trie whose leaves are term blocks. The
 * advantage of this approach is that seekExact is often able to determine a term cannot exist
 * without doing any IO, and intersection with Automata is very fast. Note that this terms
 * dictionary has its own fixed terms index (ie, it does not support a pluggable terms index
 * implementation).
 *
 * <p><b>NOTE</b>: this terms dictionary supports min/maxItemsPerBlock during indexing to control
 * how much memory the terms index uses.
 *
 * <p>The data structure used by this implementation is very similar to a burst trie
 * (http://citeseer.ist.psu.edu/viewdoc/summary?doi=10.1.1.18.3499), but with added logic to break
 * up too-large blocks of all terms sharing a given prefix into smaller ones.
 *
 * <p>Use {@link org.apache.lucene.index.CheckIndex} with the <code>-verbose</code> option to see
 * summary statistics on the blocks in the dictionary.
 *
 * <p>See {@link Lucene90BlockTreeTermsWriter}.
 */
public final class Lucene90BlockTreeTermsReader extends FieldsProducer {

    static final Outputs<BytesRef> FST_OUTPUTS = ByteSequenceOutputs.getSingleton();

    static final BytesRef NO_OUTPUT = FST_OUTPUTS.getNoOutput();

    static final int OUTPUT_FLAGS_NUM_BITS = 2;
    static final int OUTPUT_FLAGS_MASK = 0x3;
    static final int OUTPUT_FLAG_IS_FLOOR = 0x1;
    static final int OUTPUT_FLAG_HAS_TERMS = 0x2;

    /** Extension of terms file */
    static final String TERMS_EXTENSION = "tim";

    static final String TERMS_CODEC_NAME = "BlockTreeTermsDict";

    /** Initial terms format. */
    public static final int VERSION_START = 0;

    /**
     * Version that encode output as MSB VLong for better outputs sharing in FST, see GITHUB#12620.
     */
    public static final int VERSION_MSB_VLONG_OUTPUT = 1;

    /** The version that specialize arc store for continuous label in FST. */
    public static final int VERSION_FST_CONTINUOUS_ARCS = 2;

    /** Current terms format. */
    public static final int VERSION_CURRENT = VERSION_FST_CONTINUOUS_ARCS;

    /** Extension of terms index file */
    static final String TERMS_INDEX_EXTENSION = "tip";

    static final String TERMS_INDEX_CODEC_NAME = "BlockTreeTermsIndex";

    /** Extension of terms meta file */
    static final String TERMS_META_EXTENSION = "tmd";

    static final String TERMS_META_CODEC_NAME = "BlockTreeTermsMeta";

    // Open input to the main terms dict file (_X.tib)
    final IndexInput termsIn;
    // Open input to the terms index file (_X.tip)
    final IndexInput indexIn;

    // private static final boolean DEBUG = BlockTreeTermsWriter.DEBUG;

    // Reads the terms dict entries, to gather state to
    // produce DocsEnum on demand
    final PostingsReaderBase postingsReader;

    private final FieldInfos fieldInfos;
    private final IntObjectHashMap<FieldReader> fieldMap;
    private final List<String> fieldList;

    final String segment;

    final int version;

    /** Sole constructor. */
    @SuppressForbidden(reason = "require usage of Lucene's IOUtils")
    public Lucene90BlockTreeTermsReader(PostingsReaderBase postingsReader, SegmentReadState state) throws IOException {
        boolean success = false;

        this.postingsReader = postingsReader;
        this.segment = state.segmentInfo.name;

        try {
            String termsName = IndexFileNames.segmentFileName(segment, state.segmentSuffix, TERMS_EXTENSION);
            termsIn = state.directory.openInput(termsName, state.context);
            version = CodecUtil.checkIndexHeader(
                termsIn,
                TERMS_CODEC_NAME,
                VERSION_START,
                VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );

            String indexName = IndexFileNames.segmentFileName(segment, state.segmentSuffix, TERMS_INDEX_EXTENSION);
            indexIn = state.directory.openInput(indexName, state.context.withReadAdvice(ReadAdvice.RANDOM_PRELOAD));
            CodecUtil.checkIndexHeader(indexIn, TERMS_INDEX_CODEC_NAME, version, version, state.segmentInfo.getId(), state.segmentSuffix);

            // Read per-field details
            String metaName = IndexFileNames.segmentFileName(segment, state.segmentSuffix, TERMS_META_EXTENSION);
            IntObjectHashMap<FieldReader> fieldMap = null;
            Throwable priorE = null;
            long indexLength = -1, termsLength = -1;
            try (ChecksumIndexInput metaIn = state.directory.openChecksumInput(metaName)) {
                try {
                    CodecUtil.checkIndexHeader(
                        metaIn,
                        TERMS_META_CODEC_NAME,
                        version,
                        version,
                        state.segmentInfo.getId(),
                        state.segmentSuffix
                    );
                    postingsReader.init(metaIn, state);

                    final int numFields = metaIn.readVInt();
                    if (numFields < 0) {
                        throw new CorruptIndexException("invalid numFields: " + numFields, metaIn);
                    }
                    fieldMap = new IntObjectHashMap<>(numFields);
                    for (int i = 0; i < numFields; ++i) {
                        final int field = metaIn.readVInt();
                        final long numTerms = metaIn.readVLong();
                        if (numTerms <= 0) {
                            throw new CorruptIndexException("Illegal numTerms for field number: " + field, metaIn);
                        }
                        final BytesRef rootCode = readBytesRef(metaIn);
                        final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
                        if (fieldInfo == null) {
                            throw new CorruptIndexException("invalid field number: " + field, metaIn);
                        }
                        final long sumTotalTermFreq = metaIn.readVLong();
                        // when frequencies are omitted, sumDocFreq=sumTotalTermFreq and only one value is
                        // written.
                        final long sumDocFreq = fieldInfo.getIndexOptions() == IndexOptions.DOCS ? sumTotalTermFreq : metaIn.readVLong();
                        final int docCount = metaIn.readVInt();
                        BytesRef minTerm = readBytesRef(metaIn);
                        BytesRef maxTerm = readBytesRef(metaIn);
                        if (numTerms == 1) {
                            assert maxTerm.equals(minTerm);
                            // save heap for edge case of a single term only so min == max
                            maxTerm = minTerm;
                        }
                        // START CHANGE:
                        minTerm = NonForkedHelper.shouldKeepMinOrMaxTerm(minTerm);
                        maxTerm = NonForkedHelper.shouldKeepMinOrMaxTerm(maxTerm);
                        // END CHANGE
                        if (docCount < 0 || docCount > state.segmentInfo.maxDoc()) { // #docs with field must be <= #docs
                            throw new CorruptIndexException(
                                "invalid docCount: " + docCount + " maxDoc: " + state.segmentInfo.maxDoc(),
                                metaIn
                            );
                        }
                        if (sumDocFreq < docCount) { // #postings must be >= #docs with field
                            throw new CorruptIndexException("invalid sumDocFreq: " + sumDocFreq + " docCount: " + docCount, metaIn);
                        }
                        if (sumTotalTermFreq < sumDocFreq) { // #positions must be >= #postings
                            throw new CorruptIndexException(
                                "invalid sumTotalTermFreq: " + sumTotalTermFreq + " sumDocFreq: " + sumDocFreq,
                                metaIn
                            );
                        }
                        final long indexStartFP = metaIn.readVLong();
                        FieldReader previous = fieldMap.put(
                            fieldInfo.number,
                            new FieldReader(
                                this,
                                fieldInfo,
                                numTerms,
                                rootCode,
                                sumTotalTermFreq,
                                sumDocFreq,
                                docCount,
                                indexStartFP,
                                metaIn,
                                indexIn,
                                minTerm,
                                maxTerm
                            )
                        );
                        if (previous != null) {
                            throw new CorruptIndexException("duplicate field: " + fieldInfo.name, metaIn);
                        }
                    }
                    indexLength = metaIn.readLong();
                    termsLength = metaIn.readLong();
                } catch (Throwable exception) {
                    priorE = exception;
                } finally {
                    if (metaIn != null) {
                        CodecUtil.checkFooter(metaIn, priorE);
                    } else if (priorE != null) {
                        IOUtils.rethrowAlways(priorE);
                    }
                }
            }
            // At this point the checksum of the meta file has been verified so the lengths are likely
            // correct
            CodecUtil.retrieveChecksum(indexIn, indexLength);
            CodecUtil.retrieveChecksum(termsIn, termsLength);
            fieldInfos = state.fieldInfos;
            this.fieldMap = fieldMap;
            this.fieldList = sortFieldNames(fieldMap, state.fieldInfos);
            success = true;
        } finally {
            if (success == false) {
                // this.close() will close in:
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    private static BytesRef readBytesRef(IndexInput in) throws IOException {
        int numBytes = in.readVInt();
        if (numBytes < 0) {
            throw new CorruptIndexException("invalid bytes length: " + numBytes, in);
        }

        BytesRef bytes = new BytesRef(numBytes);
        bytes.length = numBytes;
        in.readBytes(bytes.bytes, 0, numBytes);

        return bytes;
    }

    private static List<String> sortFieldNames(IntObjectHashMap<FieldReader> fieldMap, FieldInfos fieldInfos) {
        List<String> fieldNames = new ArrayList<>(fieldMap.size());
        for (IntCursor fieldNumber : fieldMap.keys()) {
            fieldNames.add(fieldInfos.fieldInfo(fieldNumber.value).name);
        }
        fieldNames.sort(null);
        return Collections.unmodifiableList(fieldNames);
    }

    // for debugging
    // private static String toHex(int v) {
    // return "0x" + Integer.toHexString(v);
    // }

    @Override
    @SuppressForbidden(reason = "require usage of Lucene's IOUtils")
    public void close() throws IOException {
        try {
            IOUtils.close(indexIn, termsIn, postingsReader);
        } finally {
            // Clear so refs to terms index is GCable even if
            // app hangs onto us:
            fieldMap.clear();
        }
    }

    @Override
    public Iterator<String> iterator() {
        return fieldList.iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
        assert field != null;
        FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
        return fieldInfo == null ? null : fieldMap.get(fieldInfo.number);
    }

    @Override
    public int size() {
        return fieldMap.size();
    }

    @Override
    public void checkIntegrity() throws IOException {
        // terms index
        CodecUtil.checksumEntireFile(indexIn);

        // term dictionary
        CodecUtil.checksumEntireFile(termsIn);

        // postings
        postingsReader.checkIntegrity();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(fields=" + fieldMap.size() + ",delegate=" + postingsReader + ")";
    }
}

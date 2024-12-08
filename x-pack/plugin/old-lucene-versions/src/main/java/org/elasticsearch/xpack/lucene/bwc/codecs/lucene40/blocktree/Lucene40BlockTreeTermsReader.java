/*
 * @notice
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
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene40.blocktree;

import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst.ByteSequenceOutputs;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene70.fst.Outputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
 * <p>See {@code BlockTreeTermsWriter}.
 *
 */
public final class Lucene40BlockTreeTermsReader extends FieldsProducer {

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
    public static final int VERSION_START = 2;

    /** Auto-prefix terms have been superseded by points. */
    public static final int VERSION_AUTO_PREFIX_TERMS_REMOVED = 3;

    /** The long[] + byte[] metadata has been replaced with a single byte[]. */
    public static final int VERSION_META_LONGS_REMOVED = 4;

    /** Suffixes are compressed to save space. */
    public static final int VERSION_COMPRESSED_SUFFIXES = 5;

    /** Metadata is written to its own file. */
    public static final int VERSION_META_FILE = 6;

    /** Current terms format. */
    public static final int VERSION_CURRENT = VERSION_META_FILE;

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

    private final Map<String, FieldReader> fieldMap;
    private final List<String> fieldList;

    final String segment;

    final int version;

    /** Sole constructor. */
    public Lucene40BlockTreeTermsReader(PostingsReaderBase postingsReader, SegmentReadState state) throws IOException {
        boolean success = false;

        this.postingsReader = postingsReader;
        this.segment = state.segmentInfo.name;

        try {
            String termsName = IndexFileNames.segmentFileName(segment, state.segmentSuffix, TERMS_EXTENSION);
            termsIn = EndiannessReverserUtil.openInput(state.directory, termsName, state.context);
            version = CodecUtil.checkIndexHeader(
                termsIn,
                TERMS_CODEC_NAME,
                VERSION_START,
                VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            if (version < VERSION_AUTO_PREFIX_TERMS_REMOVED) {
                // pre-6.2 index, records whether auto-prefix terms are enabled in the header
                byte b = termsIn.readByte();
                if (b != 0) {
                    throw new CorruptIndexException("Index header pretends the index has auto-prefix terms: " + b, termsIn);
                }
            }

            String indexName = IndexFileNames.segmentFileName(segment, state.segmentSuffix, TERMS_INDEX_EXTENSION);
            indexIn = EndiannessReverserUtil.openInput(state.directory, indexName, state.context);
            CodecUtil.checkIndexHeader(indexIn, TERMS_INDEX_CODEC_NAME, version, version, state.segmentInfo.getId(), state.segmentSuffix);

            if (version < VERSION_META_FILE) {
                // Have PostingsReader init itself
                postingsReader.init(termsIn, state);

                // Verifying the checksum against all bytes would be too costly, but for now we at least
                // verify proper structure of the checksum footer. This is cheap and can detect some forms
                // of corruption such as file truncation.
                CodecUtil.retrieveChecksum(indexIn);
                CodecUtil.retrieveChecksum(termsIn);
            }

            // Read per-field details
            String metaName = IndexFileNames.segmentFileName(segment, state.segmentSuffix, TERMS_META_EXTENSION);
            Map<String, FieldReader> fieldMap = null;
            Throwable priorE = null;
            long indexLength = -1, termsLength = -1;
            try (
                ChecksumIndexInput metaIn = version >= VERSION_META_FILE
                    ? EndiannessReverserUtil.openChecksumInput(state.directory, metaName, state.context)
                    : null
            ) {
                try {
                    final IndexInput indexMetaIn, termsMetaIn;
                    if (version >= VERSION_META_FILE) {
                        CodecUtil.checkIndexHeader(
                            metaIn,
                            TERMS_META_CODEC_NAME,
                            version,
                            version,
                            state.segmentInfo.getId(),
                            state.segmentSuffix
                        );
                        indexMetaIn = termsMetaIn = metaIn;
                        postingsReader.init(metaIn, state);
                    } else {
                        seekDir(termsIn);
                        seekDir(termsIn);
                        seekDir(indexIn);
                        indexMetaIn = indexIn;
                        termsMetaIn = termsIn;
                    }

                    final int numFields = termsMetaIn.readVInt();
                    if (numFields < 0) {
                        throw new CorruptIndexException("invalid numFields: " + numFields, termsMetaIn);
                    }
                    fieldMap = new HashMap<>((int) (numFields / 0.75f) + 1);
                    for (int i = 0; i < numFields; ++i) {
                        final int field = termsMetaIn.readVInt();
                        final long numTerms = termsMetaIn.readVLong();
                        if (numTerms <= 0) {
                            throw new CorruptIndexException("Illegal numTerms for field number: " + field, termsMetaIn);
                        }
                        final BytesRef rootCode = readBytesRef(termsMetaIn);
                        final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
                        if (fieldInfo == null) {
                            throw new CorruptIndexException("invalid field number: " + field, termsMetaIn);
                        }
                        final long sumTotalTermFreq = termsMetaIn.readVLong();
                        // when frequencies are omitted, sumDocFreq=sumTotalTermFreq and only one value is
                        // written.
                        final long sumDocFreq = fieldInfo.getIndexOptions() == IndexOptions.DOCS
                            ? sumTotalTermFreq
                            : termsMetaIn.readVLong();
                        final int docCount = termsMetaIn.readVInt();
                        if (version < VERSION_META_LONGS_REMOVED) {
                            final int longsSize = termsMetaIn.readVInt();
                            if (longsSize < 0) {
                                throw new CorruptIndexException(
                                    "invalid longsSize for field: " + fieldInfo.name + ", longsSize=" + longsSize,
                                    termsMetaIn
                                );
                            }
                        }
                        BytesRef minTerm = readBytesRef(termsMetaIn);
                        BytesRef maxTerm = readBytesRef(termsMetaIn);
                        if (docCount < 0 || docCount > state.segmentInfo.maxDoc()) { // #docs with field must be <= #docs
                            throw new CorruptIndexException(
                                "invalid docCount: " + docCount + " maxDoc: " + state.segmentInfo.maxDoc(),
                                termsMetaIn
                            );
                        }
                        if (sumDocFreq < docCount) { // #postings must be >= #docs with field
                            throw new CorruptIndexException("invalid sumDocFreq: " + sumDocFreq + " docCount: " + docCount, termsMetaIn);
                        }
                        if (sumTotalTermFreq < sumDocFreq) { // #positions must be >= #postings
                            throw new CorruptIndexException(
                                "invalid sumTotalTermFreq: " + sumTotalTermFreq + " sumDocFreq: " + sumDocFreq,
                                termsMetaIn
                            );
                        }
                        final long indexStartFP = indexMetaIn.readVLong();
                        FieldReader previous = fieldMap.put(
                            fieldInfo.name,
                            new FieldReader(
                                this,
                                fieldInfo,
                                numTerms,
                                rootCode,
                                sumTotalTermFreq,
                                sumDocFreq,
                                docCount,
                                indexStartFP,
                                indexMetaIn,
                                indexIn,
                                minTerm,
                                maxTerm
                            )
                        );
                        if (previous != null) {
                            throw new CorruptIndexException("duplicate field: " + fieldInfo.name, termsMetaIn);
                        }
                    }
                    if (version >= VERSION_META_FILE) {
                        indexLength = metaIn.readLong();
                        termsLength = metaIn.readLong();
                    }
                } catch (Throwable exception) {
                    priorE = exception;
                } finally {
                    if (metaIn != null) {
                        CodecUtil.checkFooter(metaIn, priorE);
                    } else if (priorE != null) {
                        rethrowAlways(priorE);
                    }
                }
            }
            if (version >= VERSION_META_FILE) {
                // At this point the checksum of the meta file has been verified so the lengths are likely
                // correct
                CodecUtil.retrieveChecksum(indexIn, indexLength);
                CodecUtil.retrieveChecksum(termsIn, termsLength);
            } else {
                assert indexLength == -1 : indexLength;
                assert termsLength == -1 : termsLength;
            }
            List<String> fieldList = new ArrayList<>(fieldMap.keySet());
            fieldList.sort(null);
            this.fieldMap = fieldMap;
            this.fieldList = Collections.unmodifiableList(fieldList);
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

        BytesRef bytes = new BytesRef();
        bytes.length = numBytes;
        bytes.bytes = new byte[numBytes];
        in.readBytes(bytes.bytes, 0, numBytes);

        return bytes;
    }

    /** Seek {@code input} to the directory offset. */
    private static void seekDir(IndexInput input) throws IOException {
        input.seek(input.length() - CodecUtil.footerLength() - 8);
        long offset = input.readLong();
        input.seek(offset);
    }

    @SuppressForbidden(reason = "Lucene class")
    private static Error rethrowAlways(Throwable th) throws IOException, RuntimeException {
        return org.apache.lucene.util.IOUtils.rethrowAlways(th);
    }

    // for debugging
    // private static String toHex(int v) {
    // return "0x" + Integer.toHexString(v);
    // }

    @Override
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
        return fieldMap.get(field);
    }

    @Override
    public int size() {
        return fieldMap.size();
    }

    // for debugging
    String brToString(BytesRef b) {
        if (b == null) {
            return "null";
        } else {
            try {
                return b.utf8ToString() + " " + b;
            } catch (@SuppressWarnings("unused") Throwable t) {
                // If BytesRef isn't actually UTF8, or it's eg a
                // prefix of UTF8 that ends mid-unicode-char, we
                // fallback to hex:
                return b.toString();
            }
        }
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

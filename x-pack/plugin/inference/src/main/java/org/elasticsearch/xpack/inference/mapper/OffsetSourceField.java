/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.index.IndexVersions.SEMANTIC_FIELD_TYPE;

/**
 * Represents a {@link Field} that stores a {@link Term} along with either its start and end offsets
 * or an input index.
 * Offsets and input index are mutually exclusive: exactly one is stored per posting.
 * <p>
 * Offsets are written via the standard {@link OffsetAttribute}. The input index, when present, is
 * encoded as the token's absolute position (via {@link PositionIncrementAttribute}); in that case
 * the offsets are left at their default {@code (0, 0)}, which serves as the on-disk sentinel for
 * "this posting carries an input index, not offsets." Callers must therefore never write
 * {@code (0, 0)} as a legitimate offset pair.
 * <p>
 * Note: The {@link Charset} used to calculate the offsets is not associated with this field.
 * It is the responsibility of the consumer to handle the appropriate {@link Charset}.
 */
public final class OffsetSourceField extends Field {
    private static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    }

    private int startOffset;
    private int endOffset;
    private Integer inputIndex;

    public OffsetSourceField(String fieldName, String sourceFieldName, int startOffset, int endOffset) {
        this(fieldName, sourceFieldName, startOffset, endOffset, null);
    }

    public OffsetSourceField(String fieldName, String sourceFieldName, @Nullable Integer inputIndex) {
        this(fieldName, sourceFieldName, 0, 0, inputIndex);
    }

    private OffsetSourceField(String fieldName, String sourceFieldName, int startOffset, int endOffset, @Nullable Integer inputIndex) {
        super(fieldName, sourceFieldName, FIELD_TYPE);
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.inputIndex = inputIndex;
    }

    public void setValues(String fieldName, int startOffset, int endOffset) {
        this.fieldsData = fieldName;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.inputIndex = null;
    }

    public void setValues(String fieldName, int inputIndex) {
        this.fieldsData = fieldName;
        this.startOffset = 0;
        this.endOffset = 0;
        this.inputIndex = inputIndex;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
        OffsetTokenStream stream;
        if (reuse instanceof OffsetTokenStream) {
            stream = (OffsetTokenStream) reuse;
        } else {
            stream = new OffsetTokenStream();
        }

        stream.setValues((String) fieldsData, startOffset, endOffset, inputIndex);
        return stream;
    }

    public static OffsetSourceLoader loader(Terms terms) throws IOException {
        return new OffsetSourceLoader(terms);
    }

    private static final class OffsetTokenStream extends TokenStream {
        private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
        private final OffsetAttribute offsetAttribute = addAttribute(OffsetAttribute.class);
        private final PositionIncrementAttribute positionIncrementAttribute = addAttribute(PositionIncrementAttribute.class);
        private boolean used = true;
        private String term = null;
        private int startOffset = 0;
        private int endOffset = 0;
        private Integer inputIndex = null;

        private OffsetTokenStream() {}

        /** Sets the values */
        void setValues(String term, int startOffset, int endOffset, @Nullable Integer inputIndex) {
            this.term = term;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.inputIndex = inputIndex;
        }

        @Override
        public boolean incrementToken() {
            if (used) {
                return false;
            }
            clearAttributes();
            termAttribute.append(term);
            if (inputIndex != null) {
                // Temporary logic to ensure input index is not used in release builds
                if (SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled() == false) {
                    throw new UnsupportedOperationException("Input index is not supported yet");
                }

                // Leave offsets at the default (0, 0) sentinel; encode inputIndex as the absolute
                // position. PositionIncrementAttribute is cumulative and Lucene's initial position
                // is -1, so increment = inputIndex + 1 yields an absolute position of inputIndex.
                assert startOffset == 0 && endOffset == 0;
                positionIncrementAttribute.setPositionIncrement(inputIndex + 1);
            } else {
                offsetAttribute.setOffset(startOffset, endOffset);
            }
            used = true;
            return true;
        }

        @Override
        public void reset() {
            used = false;
        }

        @Override
        public void close() {
            term = null;
        }
    }

    public static class OffsetSourceLoader {
        private final Map<String, PostingsEnum> postingsEnums = new LinkedHashMap<>();

        private OffsetSourceLoader(Terms terms) throws IOException {
            var termsEnum = terms.iterator();
            while (termsEnum.next() != null) {
                var postings = termsEnum.postings(null, PostingsEnum.OFFSETS);
                if (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    postingsEnums.put(termsEnum.term().utf8ToString(), postings);
                }
            }
        }

        public OffsetSourceFieldMapper.OffsetSource advanceTo(int doc, IndexVersion indexVersion) throws IOException {
            for (var it = postingsEnums.entrySet().iterator(); it.hasNext();) {
                var entry = it.next();
                var postings = entry.getValue();
                if (postings.docID() < doc) {
                    if (postings.advance(doc) == DocIdSetIterator.NO_MORE_DOCS) {
                        it.remove();
                        continue;
                    }
                }
                if (postings.docID() == doc) {
                    assert postings.freq() == 1;
                    int position = postings.nextPosition();
                    int startOffset = postings.startOffset();
                    int endOffset = postings.endOffset();

                    if (indexVersion.onOrAfter(SEMANTIC_FIELD_TYPE) && startOffset == 0 && endOffset == 0) {
                        // Temporary logic to ensure input index is not used in release builds
                        if (SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled() == false) {
                            throw new UnsupportedOperationException("Input index is not supported yet");
                        }

                        // Sentinel for inputIndex form; the absolute position carries the value.
                        // Gate sentinel usage on the index version because there was a time period where empty chunks (which could
                        // legitimately have start and end offset == 0) could be in the chunk map. This was fixed in
                        // https://github.com/elastic/elasticsearch/pull/123763. The index version gate ensures that indices that could
                        // contain empty chunks are mutually exclusive from those that recognize the sentinel value.
                        return new OffsetSourceFieldMapper.OffsetSource(entry.getKey(), position);
                    }
                    return new OffsetSourceFieldMapper.OffsetSource(entry.getKey(), startOffset, endOffset);
                }
            }
            return null;
        }
    }
}

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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

public final class OffsetField extends Field {

    private static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    }

    private int startOffset;
    private int endOffset;

    public OffsetField(String fieldName, String sourceFieldName, int startOffset, int endOffset) {
        super(fieldName, sourceFieldName, FIELD_TYPE);
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public void setOffsets(int startOffset, int endOffset) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
        OffsetTokenStream stream;
        if (reuse instanceof OffsetTokenStream) {
            stream = (OffsetTokenStream) reuse;
        } else {
            stream = new OffsetTokenStream();
        }

        stream.setValues((String) fieldsData, startOffset, endOffset);
        return stream;
    }

    private static final class OffsetTokenStream extends TokenStream {
        private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
        private final OffsetAttribute offsetAttribute = addAttribute(OffsetAttribute.class);
        private boolean used = true;
        private String value = null;
        private int startOffset = 0;
        private int endOffset = 0;

        private OffsetTokenStream() {}

        /** Sets the values */
        void setValues(String value, int startOffset, int endOffset) {
            this.value = value;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        @Override
        public boolean incrementToken() {
            if (used) {
                return false;
            }
            clearAttributes();
            termAttribute.append(value);
            offsetAttribute.setOffset(startOffset, endOffset);
            used = true;
            return true;
        }

        @Override
        public void reset() {
            used = false;
        }

        @Override
        public void close() {
            value = null;
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

/**
 * Custom field that allows storing an integer value as a term frequency in lucene.
 */
public final class CustomTermFreqField extends Field {

    private static final FieldType FIELD_TYPE = new FieldType();
    static {
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    }

    private final int fieldValue;

    public CustomTermFreqField(String fieldName, CharSequence term, int fieldValue) {
        super(fieldName, term, FIELD_TYPE);
        this.fieldValue = fieldValue;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
        CustomTermFreqTokenStream stream;
        if (reuse instanceof CustomTermFreqTokenStream) {
            stream = (CustomTermFreqTokenStream) reuse;
        } else {
            stream = new CustomTermFreqTokenStream();
        }
        stream.setValues((String) fieldsData, fieldValue);
        return stream;
    }

    private static final class CustomTermFreqTokenStream extends TokenStream {
        private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
        private final TermFrequencyAttribute freqAttribute = addAttribute(TermFrequencyAttribute.class);
        private boolean used = true;
        private String value = null;
        private int freq = 0;

        private CustomTermFreqTokenStream() {}

        /** Sets the values */
        void setValues(String value, int freq) {
            this.value = value;
            this.freq = freq;
        }

        @Override
        public boolean incrementToken() {
            if (used) {
                return false;
            }
            clearAttributes();
            termAttribute.append(value);
            freqAttribute.setTermFrequency(freq);
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

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
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

/**
 * This class is forked from the Lucene {@link FeatureField} implementation to enable support for storing term vectors.
 * It should be removed once apache/lucene#14034 becomes available.
 */
public final class XFeatureField extends Field {
    private static final FieldType FIELD_TYPE = new FieldType();
    private static final FieldType FIELD_TYPE_STORE_TERM_VECTORS = new FieldType();

    static {
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS);

        FIELD_TYPE_STORE_TERM_VECTORS.setTokenized(false);
        FIELD_TYPE_STORE_TERM_VECTORS.setOmitNorms(true);
        FIELD_TYPE_STORE_TERM_VECTORS.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
        FIELD_TYPE_STORE_TERM_VECTORS.setStoreTermVectors(true);
    }

    private float featureValue;

    /**
     * Create a feature.
     *
     * @param fieldName The name of the field to store the information into. All features may be
     *     stored in the same field.
     * @param featureName The name of the feature, eg. 'pagerank`. It will be indexed as a term.
     * @param featureValue The value of the feature, must be a positive, finite, normal float.
     */
    public XFeatureField(String fieldName, String featureName, float featureValue) {
        this(fieldName, featureName, featureValue, false);
    }

    /**
     * Create a feature.
     *
     * @param fieldName    The name of the field to store the information into. All features may be
     *                     stored in the same field.
     * @param featureName  The name of the feature, eg. 'pagerank`. It will be indexed as a term.
     * @param featureValue The value of the feature, must be a positive, finite, normal float.
     */
    public XFeatureField(String fieldName, String featureName, float featureValue, boolean storeTermVectors) {
        super(fieldName, featureName, storeTermVectors ? FIELD_TYPE_STORE_TERM_VECTORS : FIELD_TYPE);
        setFeatureValue(featureValue);
    }

    /**
     * Update the feature value of this field.
     */
    public void setFeatureValue(float featureValue) {
        if (Float.isFinite(featureValue) == false) {
            throw new IllegalArgumentException(
                "featureValue must be finite, got: " + featureValue + " for feature " + fieldsData + " on field " + name
            );
        }
        if (featureValue < Float.MIN_NORMAL) {
            throw new IllegalArgumentException(
                "featureValue must be a positive normal float, got: "
                    + featureValue
                    + " for feature "
                    + fieldsData
                    + " on field "
                    + name
                    + " which is less than the minimum positive normal float: "
                    + Float.MIN_NORMAL
            );
        }
        this.featureValue = featureValue;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
        FeatureTokenStream stream;
        if (reuse instanceof FeatureTokenStream) {
            stream = (FeatureTokenStream) reuse;
        } else {
            stream = new FeatureTokenStream();
        }

        int freqBits = Float.floatToIntBits(featureValue);
        stream.setValues((String) fieldsData, freqBits >>> 15);
        return stream;
    }

    /**
     * This is useful if you have multiple features sharing a name and you want to take action to
     * deduplicate them.
     *
     * @return the feature value of this field.
     */
    public float getFeatureValue() {
        return featureValue;
    }

    private static final class FeatureTokenStream extends TokenStream {
        private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
        private final TermFrequencyAttribute freqAttribute = addAttribute(TermFrequencyAttribute.class);
        private boolean used = true;
        private String value = null;
        private int freq = 0;

        private FeatureTokenStream() {}

        /**
         * Sets the values
         */
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

    static final int MAX_FREQ = Float.floatToIntBits(Float.MAX_VALUE) >>> 15;

    static float decodeFeatureValue(float freq) {
        if (freq > MAX_FREQ) {
            // This is never used in practice but callers of the SimScorer API might
            // occasionally call it on eg. Float.MAX_VALUE to compute the max score
            // so we need to be consistent.
            return Float.MAX_VALUE;
        }
        int tf = (int) freq; // lossless
        int featureBits = tf << 15;
        return Float.intBitsToFloat(featureBits);
    }
}

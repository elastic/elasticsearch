/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat;

import java.io.IOException;
import java.util.Map;

public final class SyntheticIdField extends Field {

    public static final String NAME = IdFieldMapper.NAME;

    private static final String ENABLED_ATTRIBUTE_KEY = SyntheticIdField.class.getSimpleName() + ".enabled";
    private static final String ENABLED_ATTRIBUTE_VALUE = Boolean.TRUE.toString();

    private static final TokenStream EMPTY_TOKE_STREAM = new TokenStream() {
        @Override
        public boolean incrementToken() throws IOException {
            return false;
        }
    };

    private static final FieldType TYPE;

    static {
        TYPE = new FieldType();
        TYPE.putAttribute(ENABLED_ATTRIBUTE_KEY, ENABLED_ATTRIBUTE_VALUE);
        TYPE.putAttribute(PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY, TSDBSyntheticIdPostingsFormat.FORMAT_NAME);
        TYPE.putAttribute(PerFieldPostingsFormat.PER_FIELD_SUFFIX_KEY, TSDBSyntheticIdPostingsFormat.SUFFIX);

        // Even if the field is marked as indexed, we'll just skip 99% of the
        // work to build the inverted index since we provide an empty TokenStream.
        // We cannot use IndexOptions.NONE and later during field infos parsing change
        // it to IndexOptions.DOCS since Lucene ensures that the schema is consistent
        // and if it sees a new document with different index options it'll reject it
        // during indexing.
        TYPE.setIndexOptions(IndexOptions.DOCS);
        TYPE.setTokenized(false);
        TYPE.setOmitNorms(true);
        // The field is marked as stored, but storage on disk might be skipped
        TYPE.setStored(true);
        TYPE.freeze();
    }

    public SyntheticIdField(BytesRef bytes) {
        super(NAME, bytes, TYPE);
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
        return EMPTY_TOKE_STREAM;
    }

    @Override
    public void setTokenStream(TokenStream tokenStream) {
        assert false : "this should never be called";
        throw new UnsupportedOperationException();
    }

    public static boolean hasSyntheticIdAttributes(Map<String, String> attributes) {
        return attributes != null
            && SyntheticIdField.ENABLED_ATTRIBUTE_VALUE.equals(attributes.get(ENABLED_ATTRIBUTE_KEY))
            && TSDBSyntheticIdPostingsFormat.FORMAT_NAME.equals(attributes.get(PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY))
            && TSDBSyntheticIdPostingsFormat.SUFFIX.equals(attributes.get(PerFieldPostingsFormat.PER_FIELD_SUFFIX_KEY));
    }
}

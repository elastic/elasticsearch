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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;

import java.util.Map;

public final class SyntheticIdField extends Field {

    public static final String NAME = IdFieldMapper.NAME;

    private static final String ENABLED_ATTRIBUTE_KEY = SyntheticIdField.class.getSimpleName() + ".enabled";
    private static final String ENABLED_ATTRIBUTE_VALUE = Boolean.TRUE.toString();

    private static final FieldType TYPE;
    static {
        TYPE = new FieldType();
        TYPE.putAttribute(ENABLED_ATTRIBUTE_KEY, ENABLED_ATTRIBUTE_VALUE);
        // Make sure the field is not indexed, but this option is changed at runtime
        // in FieldInfos so that the field can use terms and postings.
        TYPE.setIndexOptions(IndexOptions.NONE);
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
        assert false : "this should never be called";
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTokenStream(TokenStream tokenStream) {
        assert false : "this should never be called";
        throw new UnsupportedOperationException();
    }

    public static boolean hasSyntheticIdAttributes(Map<String, String> attributes) {
        if (attributes != null) {
            var attributeValue = attributes.get(SyntheticIdField.ENABLED_ATTRIBUTE_KEY);
            if (attributeValue != null) {
                return SyntheticIdField.ENABLED_ATTRIBUTE_VALUE.equals(attributeValue);
            }
        }
        return false;
    }
}

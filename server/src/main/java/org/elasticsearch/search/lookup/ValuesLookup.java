/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.lookup;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.DocValueFormat;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Per-document values, from source or formatted doc-values
 */
public interface ValuesLookup {

    /**
     * Returns a SourceLookup positioned on the current document
     */
    SourceLookup source();

    /**
     * Returns a list of formatted doc values for a given field on the current document
     */
    List<Object> docValues(String field, DocValueFormat format);

    /**
     * Override the source exposed by a ValuesLookup
     */
    static ValuesLookup wrapWithSource(ValuesLookup in, Map<String, Object> source, XContentType xContentType) {
        SourceLookup sourceLookup = new SourceLookup();
        sourceLookup.setSource(source);
        sourceLookup.setSourceContentType(Objects.requireNonNull(xContentType));
        return new ValuesLookup() {
            @Override
            public SourceLookup source() {
                return sourceLookup;
            }

            @Override
            public List<Object> docValues(String field, DocValueFormat format) {
                return in.docValues(field, format);
            }
        };
    }

    /**
     * Returns a source-only implementation of ValuesLookup
     */
    static ValuesLookup sourceOnly(BytesReference source) {
        SourceLookup sourceLookup = new SourceLookup();
        sourceLookup.setSource(source);
        return new ValuesLookup() {
            @Override
            public SourceLookup source() {
                return sourceLookup;
            }

            @Override
            public List<Object> docValues(String field, DocValueFormat format) {
                throw new UnsupportedOperationException("FieldData is not available from a source-only lookup");
            }
        };
    }

    /**
     * Returns a source-only implementation of ValuesLookup
     */
    static ValuesLookup sourceOnly(Map<String, Object> source) {
        SourceLookup sourceLookup = new SourceLookup();
        sourceLookup.setSource(source);
        return new ValuesLookup() {
            @Override
            public SourceLookup source() {
                return sourceLookup;
            }

            @Override
            public List<Object> docValues(String field, DocValueFormat format) {
                throw new UnsupportedOperationException("FieldData is not available from a source-only lookup");
            }
        };
    }
}

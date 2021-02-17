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

import java.util.Map;
import java.util.Objects;

public interface ValuesLookup {

    SourceLookup source();

    LeafDocLookup doc();

    static ValuesLookup wrapWithSource(ValuesLookup in, Map<String, Object> source, XContentType xContentType) {
        System.out.println("Building independent source: " + source);
        SourceLookup sourceLookup = new SourceLookup();
        sourceLookup.setSource(source);
        sourceLookup.setSourceContentType(Objects.requireNonNull(xContentType));
        return new ValuesLookup() {
            @Override
            public SourceLookup source() {
                return sourceLookup;
            }

            @Override
            public LeafDocLookup doc() {
                return in.doc();
            }
        };
    }

    static ValuesLookup sourceOnly(BytesReference source) {
        SourceLookup sourceLookup = new SourceLookup();
        sourceLookup.setSource(source);
        return new ValuesLookup() {
            @Override
            public SourceLookup source() {
                return sourceLookup;
            }

            @Override
            public LeafDocLookup doc() {
                throw new UnsupportedOperationException("FieldData is not available from a source-only lookup");
            }
        };
    }

    static ValuesLookup sourceOnly(Map<String, Object> source) {
        SourceLookup sourceLookup = new SourceLookup();
        sourceLookup.setSource(source);
        return new ValuesLookup() {
            @Override
            public SourceLookup source() {
                return sourceLookup;
            }

            @Override
            public LeafDocLookup doc() {
                throw new UnsupportedOperationException("FieldData is not available from a source-only lookup");
            }
        };
    }
}

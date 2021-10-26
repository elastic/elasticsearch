/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import java.io.IOException;

/**
 * Manages loading information about nested documents for a single index segment
 */
public interface LeafNestedDocuments {

    /**
     * Advance to a specific doc, and return its NestedIdentity, or {@code null} if not a child
     */
    SearchHit.NestedIdentity advance(int doc) throws IOException;

    /**
     * The current doc
     */
    int doc();

    /**
     * The ultimate parent of the current doc
     */
    int rootDoc();

    /**
     * The NestedIdentity of the current doc
     */
    SearchHit.NestedIdentity nestedIdentity();

    /**
     * An implementation of LeafNestedDocuments for use when there are no nested field mappers
     */
    LeafNestedDocuments NO_NESTED_MAPPERS = new LeafNestedDocuments() {
        @Override
        public SearchHit.NestedIdentity advance(int doc) {
            return null;
        }

        @Override
        public int doc() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int rootDoc() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SearchHit.NestedIdentity nestedIdentity() {
            throw new UnsupportedOperationException();
        }
    };
}

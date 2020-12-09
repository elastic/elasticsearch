/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

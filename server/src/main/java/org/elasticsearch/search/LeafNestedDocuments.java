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

public interface LeafNestedDocuments {

    boolean advance(int doc) throws IOException;

    int doc();

    int rootDoc();

    SearchHit.NestedIdentity nestedIdentity();

    boolean hasNonNestedParent(String path);

    LeafNestedDocuments NO_NESTED_MAPPERS = new LeafNestedDocuments() {
        @Override
        public boolean advance(int doc) {
            return false;
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

        @Override
        public boolean hasNonNestedParent(String path) {
            throw new UnsupportedOperationException();
        }
    };
}

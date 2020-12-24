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

package org.elasticsearch.index.fieldvisitor;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;

import java.io.IOException;

@FunctionalInterface
public interface StoredFieldsLoader {

    void loadStoredFields(int doc, StoredFieldVisitor fieldsVisitor) throws IOException;

    static StoredFieldsLoader getFieldsVisitor(LeafReader reader, boolean sequential) {
        if (reader instanceof SequentialStoredFieldsLeafReader && sequential) {
            // All the docs to fetch are adjacent but Lucene stored fields are optimized
            // for random access and don't optimize for sequential access - except for merging.
            // So we do a little hack here and pretend we're going to do merges in order to
            // get better sequential access.
            SequentialStoredFieldsLeafReader lf = (SequentialStoredFieldsLeafReader) reader;
            return lf.getSequentialStoredFieldsReader()::visitDocument;
        } else {
            return reader::document;
        }
    }

}

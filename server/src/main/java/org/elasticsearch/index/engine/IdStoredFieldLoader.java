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

package org.elasticsearch.index.engine;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;

/**
 * Loads IDs from stored fields using a stored fields reader optimized for sequential access
 */
final class IdStoredFieldLoader {

    private final CheckedBiConsumer<Integer, StoredFieldVisitor, IOException> reader;
    private final IdOnlyFieldVisitor visitor = new IdOnlyFieldVisitor();

    /**
     * Creates a new ID reader over a segment
     */
    IdStoredFieldLoader(LeafReader in) {
        this.reader = getStoredFieldsReader(in);
    }

    private static CheckedBiConsumer<Integer, StoredFieldVisitor, IOException> getStoredFieldsReader(LeafReader in) {
        if (in instanceof SequentialStoredFieldsLeafReader) {
            return (((SequentialStoredFieldsLeafReader)in).getSequentialStoredFieldsReader())::visitDocument;
        }
        throw new IllegalArgumentException("Requires a SequentialStoredFieldsReader, got " + in.getClass());
    }

    /**
     * Get the id for a given document
     */
    String id(int doc) throws IOException {
        visitor.reset();
        reader.accept(doc, visitor);
        return visitor.id;
    }

    private static class IdOnlyFieldVisitor extends StoredFieldVisitor {

        private String id = null;
        private boolean visited = false;

        @Override
        public Status needsField(FieldInfo fieldInfo) {
            if (visited) {
                return Status.STOP;
            }
            if (IdFieldMapper.NAME.equals(fieldInfo.name)) {
                visited = true;
                return Status.YES;
            } else {
                return Status.NO;
            }
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) {
            assert IdFieldMapper.NAME.equals(fieldInfo.name) : fieldInfo;
            id = Uid.decodeId(value);
        }

        void reset() {
            id = null;
            visited = false;
        }
    }

}

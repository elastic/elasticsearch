/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
            return (((SequentialStoredFieldsLeafReader) in).getSequentialStoredFieldsReader())::visitDocument;
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

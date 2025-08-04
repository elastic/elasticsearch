/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;

/**
 * Load {@code _source} into blocks.
 */
public final class SourceFieldBlockLoader implements BlockLoader {
    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public ColumnAtATimeReader columnAtATimeReader(LeafReaderContext context) {
        return null;
    }

    @Override
    public RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
        return new Source();
    }

    @Override
    public FieldsSpec rowStrideFieldSpec() {
        return new FieldsSpec(StoredFieldsSpec.NEEDS_SOURCE, IgnoredFieldsSpec.NONE);
    }

    @Override
    public boolean supportsOrdinals() {
        return false;
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) {
        throw new UnsupportedOperationException();
    }

    private static class Source extends BlockStoredFieldsReader {
        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            // TODO support appending BytesReference
            ((BytesRefBuilder) builder).appendBytesRef(storedFields.source().internalSourceRef().toBytesRef());
        }

        @Override
        public String toString() {
            return "BlockStoredFieldsReader.Source";
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.index;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;

/**
 * A {@link FilterLeafReader} that exposes a {@link StoredFieldsReader}
 * optimized for sequential access. This class should be used by custom
 * {@link FilterLeafReader} that are used at search time in order to
 * leverage sequential access when retrieving stored fields in queries,
 * aggregations or during the fetch phase.
 */
public abstract class SequentialStoredFieldsLeafReader extends FilterLeafReader {
    /**
     * <p>Construct a StoredFieldsFilterLeafReader based on the specified base reader.
     * <p>Note that base reader is closed if this FilterLeafReader is closed.</p>
     *
     * @param in specified base reader.
     */
    public SequentialStoredFieldsLeafReader(LeafReader in) {
        super(in);
    }

    /**
     * Implementations should return a {@link StoredFieldsReader} that wraps the provided <code>reader</code>
     * that is optimized for sequential access (adjacent doc ids).
     */
    protected abstract StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader);

    /**
     * Returns a {@link StoredFieldsReader} optimized for sequential access (adjacent doc ids).
     */
    public StoredFieldsReader getSequentialStoredFieldsReader() {
        if (in instanceof CodecReader reader) {
            return doGetSequentialStoredFieldsReader(reader.getFieldsReader().getMergeInstance());
        } else if (in instanceof SequentialStoredFieldsLeafReader reader) {
            return doGetSequentialStoredFieldsReader(reader.getSequentialStoredFieldsReader());
        } else {
            throw new IllegalStateException("requires a CodecReader or a SequentialStoredFieldsLeafReader, got " + in.getClass());
        }
    }

}

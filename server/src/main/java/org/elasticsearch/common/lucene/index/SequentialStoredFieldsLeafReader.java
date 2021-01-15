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
        if (in instanceof CodecReader) {
            CodecReader reader = (CodecReader) in;
            return doGetSequentialStoredFieldsReader(reader.getFieldsReader().getMergeInstance());
        } else if (in instanceof SequentialStoredFieldsLeafReader) {
            SequentialStoredFieldsLeafReader reader = (SequentialStoredFieldsLeafReader) in;
            return doGetSequentialStoredFieldsReader(reader.getSequentialStoredFieldsReader());
        } else {
            throw new IllegalStateException("requires a CodecReader or a SequentialStoredFieldsLeafReader, got " + in.getClass());
        }
    }

}

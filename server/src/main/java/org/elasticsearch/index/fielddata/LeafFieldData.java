/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.index.mapper.DocValueFetcher;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;

/**
 * The thread safe {@link org.apache.lucene.index.LeafReader} level cache of the data.
 */
public interface LeafFieldData extends Accountable, Releasable {

    /**
     * Returns field values for use in scripting.
     */
    ScriptDocValues<?> getScriptValues();

    /**
     * Return a String representation of the values.
     */
    SortedBinaryDocValues getBytesValues();

    /**
     * Return a value fetcher for this leaf implementation.
     */
    default DocValueFetcher.Leaf getLeafValueFetcher(DocValueFormat format) {
        SortedBinaryDocValues values = getBytesValues();
        return new DocValueFetcher.Leaf() {
            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public int docValueCount() throws IOException {
                return values.docValueCount();
            }

            @Override
            public Object nextValue() throws IOException {
                return format.format(values.nextValue());
            }
        };
    }
}

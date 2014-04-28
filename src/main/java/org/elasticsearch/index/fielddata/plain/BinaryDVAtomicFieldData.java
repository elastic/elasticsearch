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

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ScriptDocValues.Strings;

import java.io.IOException;

/** {@link AtomicFieldData} impl on top of Lucene's binary doc values. */
public class BinaryDVAtomicFieldData implements AtomicFieldData<ScriptDocValues.Strings> {

    private final AtomicReader reader;
    private final String field;

    public BinaryDVAtomicFieldData(AtomicReader reader, String field) {
        this.reader = reader;
        this.field = field;
    }

    @Override
    public boolean isMultiValued() {
        return false;
    }

    @Override
    public long getNumberUniqueValues() {
        // probably not accurate, but a good upper limit
        return reader.maxDoc();
    }

    @Override
    public long getMemorySizeInBytes() {
        // TODO: Lucene doesn't expose it right now
        return -1;
    }

    @Override
    public BytesValues getBytesValues(boolean needsHashes) {
        // if you want hashes to be cached, you should rather store them on disk alongside the values rather than loading them into memory
        // here - not supported for now, and probably not useful since this field data only applies to _id and _uid?
        final BinaryDocValues values;
        final Bits docsWithField;
        try {
            final BinaryDocValues v = reader.getBinaryDocValues(field);
            if (v == null) {
                // segment has no value
                values = DocValues.EMPTY_BINARY;
                docsWithField = new Bits.MatchNoBits(reader.maxDoc());
            } else {
                values = v;
                final Bits b = reader.getDocsWithField(field);
                docsWithField = b == null ? new Bits.MatchAllBits(reader.maxDoc()) : b;
            }
        } catch (IOException e) {
            throw new ElasticsearchIllegalStateException("Cannot load doc values", e);
        }

        return new BytesValues(false) {

            @Override
            public int setDocument(int docId) {
                this.docId = docId;
                return docsWithField.get(docId) ? 1 : 0;
            }

            @Override
            public BytesRef nextValue() {
                values.get(docId, scratch);
                return scratch;
            }
        };
    }

    @Override
    public Strings getScriptValues() {
        return new ScriptDocValues.Strings(getBytesValues(false));
    }

    @Override
    public void close() {
        // no-op
    }

}

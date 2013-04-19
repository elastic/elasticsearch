/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.util.ArrayList;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.settings.IndexSettings;

/**
 */
public class ConcreteBytesRefIndexFieldData extends AbstractBytesIndexFieldData<ConcreteBytesRefAtomicFieldData> {

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<ConcreteBytesRefAtomicFieldData> build(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType type, IndexFieldDataCache cache) {
            return new ConcreteBytesRefIndexFieldData(index, indexSettings, fieldNames, type, cache);
        }
    }

    public ConcreteBytesRefIndexFieldData(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType fieldDataType, IndexFieldDataCache cache) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
    }

    @Override
    public ConcreteBytesRefAtomicFieldData loadDirect(AtomicReaderContext context) throws Exception {
        AtomicReader reader = context.reader();

        Terms terms = reader.terms(getFieldNames().indexName());
        if (terms == null) {
            return ConcreteBytesRefAtomicFieldData.empty(reader.maxDoc());
        }

        long size = terms.size();
        if (size == -1) {
            size = 1024;
        }
        final ArrayList<BytesRef> values = new ArrayList<BytesRef>((int) size);
        values.add(null); // first "t" indicates null value
        OrdinalsBuilder builder = new OrdinalsBuilder(terms, reader.maxDoc());
        try {
            BytesRefIterator iter = builder.buildFromTerms(filter(terms, reader), reader.getLiveDocs());
            BytesRef term;
            while ((term = iter.next()) != null) {
                values.add(BytesRef.deepCopyOf(term));
            }
            return new ConcreteBytesRefAtomicFieldData(values.toArray(new BytesRef[values.size()]), builder.build(fieldDataType.getSettings()));
        } finally {
            builder.close();
        }
    }
}

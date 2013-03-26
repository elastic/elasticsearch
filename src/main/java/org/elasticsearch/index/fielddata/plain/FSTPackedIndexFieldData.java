/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.INPUT_TYPE;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AbstractIndexFieldData;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.settings.IndexSettings;

/**
 */
public class FSTPackedIndexFieldData extends AbstractIndexFieldData<FSTPackedBytesAtomicFieldData> implements IndexFieldData.WithOrdinals<FSTPackedBytesAtomicFieldData> {

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData build(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType type, IndexFieldDataCache cache) {
            return new FSTPackedIndexFieldData(index, indexSettings, fieldNames, type, cache);
        }
    }

    public FSTPackedIndexFieldData(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType fieldDataType, IndexFieldDataCache cache) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
    }

    @Override
    public boolean valuesOrdered() {
        return true;
    }

    @Override
    public FSTPackedBytesAtomicFieldData load(AtomicReaderContext context) {
        try {
            return cache.load(context, this);
        } catch (Throwable e) {
            if (e instanceof ElasticSearchException) {
                throw (ElasticSearchException) e;
            } else {
                throw new ElasticSearchException(e.getMessage(), e);
            }
        }
    }

    @Override
    public FSTPackedBytesAtomicFieldData loadDirect(AtomicReaderContext context) throws Exception {
        AtomicReader reader = context.reader();

        Terms terms = reader.terms(getFieldNames().indexName());
        if (terms == null) {
            return FSTPackedBytesAtomicFieldData.empty(reader.maxDoc());
        }
        PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);
        org.apache.lucene.util.fst.Builder<Long> fstBuilder = new org.apache.lucene.util.fst.Builder<Long>(INPUT_TYPE.BYTE1, outputs);
        final IntsRef scratch = new IntsRef();
        
        OrdinalsBuilder builder = new OrdinalsBuilder(terms, reader.maxDoc());
        try {
            
            // 0 is reserved for "unset"
            fstBuilder.add(Util.toIntsRef(new BytesRef(), scratch), 0l);
            TermsEnum termsEnum = terms.iterator(null);
            DocsEnum docsEnum = null;
            for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                final int termOrd = builder.nextOrdinal();
                fstBuilder.add(Util.toIntsRef(term, scratch), (long)termOrd);
                docsEnum = termsEnum.docs(reader.getLiveDocs(), docsEnum, DocsEnum.FLAG_NONE);
                for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                    builder.addDoc(docId);
                }
            }
            
            FST<Long> fst = fstBuilder.finish();

            final Ordinals ordinals = builder.build(fieldDataType.getSettings());

            return new FSTPackedBytesAtomicFieldData(fst, ordinals);
        } finally {
            builder.close();
        }
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, SortMode sortMode) {
        // TODO support "missingValue" for sortMissingValue options here...
        return new BytesRefFieldComparatorSource(this, sortMode);
    }
}

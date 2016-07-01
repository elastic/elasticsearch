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

package org.elasticsearch.index.mapper.size;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.uninverting.UninvertingReader;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.plain.AbstractIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Collections;

final class LongUninvertingIndexFieldData extends AbstractIndexFieldData<LongUninvertingAtomicFieldData> {
    public static class Builder implements IndexFieldData.Builder {
        private final UninvertingReader.Type type;

        public Builder(UninvertingReader.Type type) {
            this.type = type;
        }

        @Override
        public LongUninvertingIndexFieldData build(IndexSettings indexSettings, MappedFieldType fieldType,
                                    IndexFieldDataCache cache, CircuitBreakerService breakerService,
                                    MapperService mapperService) {
            return new LongUninvertingIndexFieldData(indexSettings, fieldType, cache, breakerService, type);
        }
    }


    private final String field;
    private final UninvertingReader.Type type;
    private final CircuitBreakerService breakerService;

    public LongUninvertingIndexFieldData(IndexSettings indexSettings, MappedFieldType fieldType,
                                         IndexFieldDataCache cache, CircuitBreakerService breakerService,
                                         UninvertingReader.Type type) {
        super(indexSettings, fieldType.name(), cache);
        this.field = fieldType.name();
        this.type = type;
        this.breakerService = breakerService;

    }

    @Override
    public String getFieldName() {
        return field;
    }

    @Override
    protected LongUninvertingAtomicFieldData empty(int maxDoc) {
        return LongUninvertingAtomicFieldData.empty(maxDoc);
    }

    @Override
    public LongUninvertingAtomicFieldData loadDirect(LeafReaderContext context) throws Exception {
        CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.FIELDDATA);
        long estimatedBytes = context.reader().maxDoc() * Integer.BYTES;
        breaker.addEstimateBytesAndMaybeBreak(estimatedBytes, field);
        UninvertingReader reader =
            new UninvertingReader(context.reader(),
                Collections.singletonMap(field, type));
        NumericDocValues numeric = reader.getNumericDocValues(field);
        Bits docsWithField = reader.getDocsWithField(field);
        LongUninvertingAtomicFieldData fd = new LongUninvertingAtomicFieldData(numeric, docsWithField);
        long actualUsed = fd.ramBytesUsed();
        breaker.addWithoutBreaking(-(estimatedBytes - actualUsed));
        return fd;
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, MultiValueMode sortMode,
                                                   XFieldComparatorSource.Nested nested) {
        return new XFieldComparatorSource() {
            @Override
            public SortField.Type reducedType() {
                return SortField.Type.LONG;
            }

            @Override
            public FieldComparator<?> newComparator(String fieldname,
                                                    int numHits, int sortPos, boolean reversed) throws IOException {
                final Long dMissingValue = (Long) missingObject(missingValue, reversed);
                // NOTE: it's important to pass null as a missing value in the constructor so that
                // the comparator doesn't check docsWithField since we replace missing values in select()
                return new FieldComparator.LongComparator(numHits, null, null) {
                    @Override
                    protected NumericDocValues getNumericDocValues(LeafReaderContext context,
                                                                   String field) throws IOException {
                        final SortedNumericDocValues values = load(context).getLongValues();
                        final NumericDocValues selectedValues;
                        if (nested == null) {
                            selectedValues = sortMode.select(values, dMissingValue);
                        } else {
                            final BitSet rootDocs = nested.rootDocs(context);
                            final DocIdSetIterator innerDocs = nested.innerDocs(context);
                            selectedValues = sortMode.select(values, dMissingValue, rootDocs,
                                innerDocs, context.reader().maxDoc());
                        }
                        return selectedValues;
                    }
                };
            }
        };
    }
}

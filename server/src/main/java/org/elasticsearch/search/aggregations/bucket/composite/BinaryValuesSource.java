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

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;

/**
 * A {@link SingleDimensionValuesSource} for binary source ({@link BytesRef}).
 */
class BinaryValuesSource extends SingleDimensionValuesSource<BytesRef> {
    private final CheckedFunction<LeafReaderContext, SortedBinaryDocValues, IOException> docValuesFunc;
    private final BytesRef[] values;
    private BytesRef currentValue;

    BinaryValuesSource(MappedFieldType fieldType, CheckedFunction<LeafReaderContext, SortedBinaryDocValues, IOException> docValuesFunc,
                       DocValueFormat format, Object missing, int size, int reverseMul) {
        super(format, fieldType, missing, size, reverseMul);
        this.docValuesFunc = docValuesFunc;
        this.values = new BytesRef[size];
    }

    @Override
    public void copyCurrent(int slot) {
        values[slot] = BytesRef.deepCopyOf(currentValue);
    }

    @Override
    public int compare(int from, int to) {
        return compareValues(values[from], values[to]);
    }

    @Override
    int compareCurrent(int slot) {
        return compareValues(currentValue, values[slot]);
    }

    @Override
    int compareCurrentWithAfter() {
        return compareValues(currentValue, afterValue);
    }

    int compareValues(BytesRef v1, BytesRef v2) {
        return v1.compareTo(v2) * reverseMul;
    }

    @Override
    void setAfter(Comparable<?> value) {
        if (value.getClass() == String.class) {
            afterValue = format.parseBytesRef(value.toString());
        } else {
            throw new IllegalArgumentException("invalid value, expected string, got " + value.getClass().getSimpleName());
        }
    }

    @Override
    BytesRef toComparable(int slot) {
        return values[slot];
    }

    @Override
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
        final SortedBinaryDocValues dvs = docValuesFunc.apply(context);
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (dvs.advanceExact(doc)) {
                    int num = dvs.docValueCount();
                    for (int i = 0; i < num; i++) {
                        currentValue = dvs.nextValue();
                        next.collect(doc, bucket);
                    }
                }
            }
        };
    }

    @Override
    LeafBucketCollector getLeafCollector(Comparable<?> value, LeafReaderContext context, LeafBucketCollector next) {
        if (value.getClass() != BytesRef.class) {
            throw new IllegalArgumentException("Expected BytesRef, got " + value.getClass());
        }
        currentValue = (BytesRef) value;
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                next.collect(doc, bucket);
            }
        };
    }

    @Override
    SortedDocsProducer createSortedDocsProducerOrNull(IndexReader reader, Query query) {
        if (checkIfSortedDocsIsApplicable(reader, fieldType) == false ||
                fieldType instanceof StringFieldType == false ||
                    (query != null && query.getClass() != MatchAllDocsQuery.class)) {
            return null;
        }
        return new TermsSortedDocsProducer(fieldType.name());
    }

    @Override
    public void close() {}
}

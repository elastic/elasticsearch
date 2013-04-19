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

import gnu.trove.iterator.TShortIterator;
import gnu.trove.list.array.TShortArrayList;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.ShortValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.settings.IndexSettings;

/**
 */
public class ShortArrayIndexFieldData extends AbstractIndexFieldData<AtomicNumericFieldData> implements IndexNumericFieldData<AtomicNumericFieldData> {

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData build(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType type, IndexFieldDataCache cache) {
            return new ShortArrayIndexFieldData(index, indexSettings, fieldNames, type, cache);
        }
    }

    public ShortArrayIndexFieldData(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType fieldDataType, IndexFieldDataCache cache) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
    }

    @Override
    public NumericType getNumericType() {
        return NumericType.SHORT;
    }

    @Override
    public boolean valuesOrdered() {
        // because we might have single values? we can dynamically update a flag to reflect that
        // based on the atomic field data loaded
        return false;
    }

    @Override
    public AtomicNumericFieldData load(AtomicReaderContext context) {
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
    public AtomicNumericFieldData loadDirect(AtomicReaderContext context) throws Exception {
        AtomicReader reader = context.reader();
        Terms terms = reader.terms(getFieldNames().indexName());
        if (terms == null) {
            return ShortArrayAtomicFieldData.EMPTY;
        }
        // TODO: how can we guess the number of terms? numerics end up creating more terms per value...
        final TShortArrayList values = new TShortArrayList();

        values.add((short) 0); // first "t" indicates null value
        OrdinalsBuilder builder = new OrdinalsBuilder(terms, reader.maxDoc());
        try {
            BytesRef term;
            short max = Short.MIN_VALUE;
            short min = Short.MAX_VALUE;
            BytesRefIterator iter = builder.buildFromTerms(builder.wrapNumeric32Bit(terms.iterator(null)), reader.getLiveDocs());
            while ((term = iter.next()) != null) {
                short value = (short) NumericUtils.prefixCodedToInt(term);
                values.add(value);
                if (value > max) {
                    max = value;
                }
                if (value < min) {
                    min = value;
                }
            }

            Ordinals build = builder.build(fieldDataType.getSettings());
            if (fieldDataType.getSettings().getAsBoolean("optimize_type", true)) {
                // if we can fit all our values in a byte we should do this!
                if (min >= Byte.MIN_VALUE && max <= Byte.MAX_VALUE) {
                    return ByteArrayIndexFieldData.build(reader, fieldDataType, builder, build, new ByteArrayIndexFieldData.BuilderBytes() {
                        @Override
                        public byte get(int index) {
                            return (byte) values.get(index);
                        }

                        @Override
                        public byte[] toArray() {
                            byte[] bValues = new byte[values.size()];
                            int i = 0;
                            for (TShortIterator it = values.iterator(); it.hasNext(); ) {
                                bValues[i++] = (byte) it.next();
                            }
                            return bValues;
                        }
                    });
                }
            }

            return build(reader, fieldDataType, builder, build, new BuilderShorts() {
                @Override
                public short get(int index) {
                    return values.get(index);
                }

                @Override
                public short[] toArray() {
                    return values.toArray();
                }

                @Override
                public int size() {
                    return values.size();
                }
            });
        } finally {
            builder.close();
        }
    }

    static interface BuilderShorts {
        short get(int index);

        short[] toArray();

        int size();
    }

    static ShortArrayAtomicFieldData build(AtomicReader reader, FieldDataType fieldDataType, OrdinalsBuilder builder, Ordinals build, BuilderShorts values) {
        if (!build.isMultiValued() && CommonSettings.removeOrdsOnSingleValue(fieldDataType)) {
            Docs ordinals = build.ordinals();
            final FixedBitSet set = builder.buildDocsWithValuesSet();

            // there's sweatspot where due to low unique value count, using ordinals will consume less memory
            long singleValuesArraySize = reader.maxDoc() * RamUsage.NUM_BYTES_SHORT + (set == null ? 0 : set.getBits().length * RamUsage.NUM_BYTES_LONG + RamUsage.NUM_BYTES_INT);
            long uniqueValuesArraySize = values.size() * RamUsage.NUM_BYTES_SHORT;
            long ordinalsSize = build.getMemorySizeInBytes();
            if (uniqueValuesArraySize + ordinalsSize < singleValuesArraySize) {
                return new ShortArrayAtomicFieldData.WithOrdinals(values.toArray(), reader.maxDoc(), build);
            }

            short[] sValues = new short[reader.maxDoc()];
            int maxDoc = reader.maxDoc();
            for (int i = 0; i < maxDoc; i++) {
                sValues[i] = values.get(ordinals.getOrd(i));
            }

            if (set == null) {
                return new ShortArrayAtomicFieldData.Single(sValues, reader.maxDoc());
            } else {
                return new ShortArrayAtomicFieldData.SingleFixedSet(sValues, reader.maxDoc(), set);
            }
        } else {
            return new ShortArrayAtomicFieldData.WithOrdinals(values.toArray(), reader.maxDoc(), build);
        }
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, SortMode sortMode) {
        return new ShortValuesComparatorSource(this, missingValue, sortMode);
    }
}

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

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.AbstractRandomAccessOrds;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

import java.io.IOException;

/**
 * Utility class that allows to return views of {@link ValuesSource}s that
 * replace the missing value with a configured value.
 */
public enum MissingValues {
    ;

    // TODO: we could specialize the single value case

    public static ValuesSource.Bytes replaceMissing(final ValuesSource.Bytes valuesSource, final BytesRef missing) {
        return new ValuesSource.Bytes() {
            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                SortedBinaryDocValues values = valuesSource.bytesValues(context);
                return replaceMissing(values, missing);
            }
        };
    }

    static SortedBinaryDocValues replaceMissing(final SortedBinaryDocValues values, final BytesRef missing) {
        return new SortedBinaryDocValues() {

            private int count;

            @Override
            public BytesRef valueAt(int index) {
                if (count > 0) {
                    return values.valueAt(index);
                } else if (index == 0) {
                    return missing;
                } else {
                    throw new IndexOutOfBoundsException();
                }
            }

            @Override
            public void setDocument(int docId) {
                values.setDocument(docId);
                count = values.count();
            }

            @Override
            public int count() {
                return count == 0 ? 1 : count;
            }
        };
    }

    public static ValuesSource.Numeric replaceMissing(final ValuesSource.Numeric valuesSource, final Number missing) {
        final boolean missingIsFloat = missing.longValue() != (long) missing.doubleValue();
        final boolean isFloatingPoint = valuesSource.isFloatingPoint() || missingIsFloat;
        return new ValuesSource.Numeric() {

            @Override
            public boolean isFloatingPoint() {
                return isFloatingPoint;
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return replaceMissing(valuesSource.bytesValues(context), new BytesRef(missing.toString()));
            }

            @Override
            public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
                final SortedNumericDocValues values = valuesSource.longValues(context);
                return replaceMissing(values, missing.longValue());
            }

            @Override
            public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
                final SortedNumericDoubleValues values = valuesSource.doubleValues(context);
                return replaceMissing(values, missing.doubleValue());
            }
        };
    }

    static SortedNumericDocValues replaceMissing(final SortedNumericDocValues values, final long missing) {
        return new SortedNumericDocValues() {

            private int count;

            @Override
            public void setDocument(int doc) {
                values.setDocument(doc);
                count = values.count();
            }

            @Override
            public long valueAt(int index) {
                if (count > 0) {
                    return values.valueAt(index);
                } else if (index == 0) {
                    return missing;
                } else {
                    throw new IndexOutOfBoundsException();
                }
            }

            @Override
            public int count() {
                return count == 0 ? 1 : count;
            }

        };
    }

    static SortedNumericDoubleValues replaceMissing(final SortedNumericDoubleValues values, final double missing) {
        return new SortedNumericDoubleValues() {

            private int count;

            @Override
            public void setDocument(int doc) {
                values.setDocument(doc);
                count = values.count();
            }

            @Override
            public double valueAt(int index) {
                if (count > 0) {
                    return values.valueAt(index);
                } else if (index == 0) {
                    return missing;
                } else {
                    throw new IndexOutOfBoundsException();
                }
            }

            @Override
            public int count() {
                return count == 0 ? 1 : count;
            }

        };
    }

    public static ValuesSource.Bytes replaceMissing(final ValuesSource.Bytes.WithOrdinals valuesSource, final BytesRef missing) {
        return new ValuesSource.Bytes.WithOrdinals() {
            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                SortedBinaryDocValues values = valuesSource.bytesValues(context);
                return replaceMissing(values, missing);
            }

            @Override
            public RandomAccessOrds ordinalsValues(LeafReaderContext context) {
                RandomAccessOrds values = valuesSource.ordinalsValues(context);
                return replaceMissing(values, missing);
            }

            @Override
            public RandomAccessOrds globalOrdinalsValues(LeafReaderContext context) {
                RandomAccessOrds values = valuesSource.globalOrdinalsValues(context);
                return replaceMissing(values, missing);
            }
        };
    }

    static RandomAccessOrds replaceMissing(final RandomAccessOrds values, final BytesRef missing) {
        final long missingOrd = values.lookupTerm(missing);
        if (missingOrd >= 0) {
            // The value already exists
            return replaceMissingOrd(values, missingOrd);
        } else {
            final long insertedOrd = -1 - missingOrd;
            return insertOrd(values, insertedOrd, missing);
        }
    }

    static RandomAccessOrds replaceMissingOrd(final RandomAccessOrds values, final long missingOrd) {
        return new AbstractRandomAccessOrds() {

            private int cardinality = 0;

            @Override
            public void doSetDocument(int docID) {
                values.setDocument(docID);
                cardinality = values.cardinality();
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                return values.lookupOrd(ord);
            }

            @Override
            public long getValueCount() {
                return values.getValueCount();
            }

            @Override
            public long ordAt(int index) {
                if (cardinality > 0) {
                    return values.ordAt(index);
                } else if (index == 0) {
                    return missingOrd;
                } else {
                    throw new IndexOutOfBoundsException();
                }
            }

            @Override
            public int cardinality() {
                return cardinality == 0 ? 1 : cardinality;
            }
        };
    }

    static RandomAccessOrds insertOrd(final RandomAccessOrds values, final long insertedOrd, final BytesRef missingValue) {
        return new AbstractRandomAccessOrds() {

            private int cardinality = 0;

            @Override
            public void doSetDocument(int docID) {
                values.setDocument(docID);
                cardinality = values.cardinality();
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                if (ord < insertedOrd) {
                    return values.lookupOrd(ord);
                } else if (ord > insertedOrd) {
                    return values.lookupOrd(ord - 1);
                } else {
                    return missingValue;
                }
            }

            @Override
            public long getValueCount() {
                return 1 + values.getValueCount();
            }

            @Override
            public long ordAt(int index) {
                if (cardinality > 0) {
                    final long ord = values.ordAt(index);
                    if (ord < insertedOrd) {
                        return ord;
                    } else {
                        return ord + 1;
                    }
                } else if (index == 0) {
                    return insertedOrd;
                } else {
                    throw new IndexOutOfBoundsException();
                }
            }

            @Override
            public int cardinality() {
                return cardinality == 0 ? 1 : cardinality;
            }
        };
    }

    public static ValuesSource.GeoPoint replaceMissing(final ValuesSource.GeoPoint valuesSource, final GeoPoint missing) {
        return new ValuesSource.GeoPoint() {

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return replaceMissing(valuesSource.bytesValues(context), new BytesRef(missing.toString()));
            }

            @Override
            public MultiGeoPointValues geoPointValues(LeafReaderContext context) {
                final MultiGeoPointValues values = valuesSource.geoPointValues(context);
                return replaceMissing(values, missing);
            }
        };
    }

    static MultiGeoPointValues replaceMissing(final MultiGeoPointValues values, final GeoPoint missing) {
        return new MultiGeoPointValues() {

            private int count;

            @Override
            public GeoPoint valueAt(int index) {
                if (count > 0) {
                    return values.valueAt(index);
                } else if (index == 0) {
                    return missing;
                } else {
                    throw new IndexOutOfBoundsException();
                }
            }

            @Override
            public void setDocument(int docId) {
                values.setDocument(docId);
                count = values.count();
            }

            @Override
            public int count() {
                return count == 0 ? 1 : count;
            }
        };
    }
}

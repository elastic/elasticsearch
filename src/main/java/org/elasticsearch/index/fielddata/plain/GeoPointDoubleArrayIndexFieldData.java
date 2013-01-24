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

import gnu.trove.list.array.TDoubleArrayList;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.ArrayList;

/**
 */
public class GeoPointDoubleArrayIndexFieldData extends AbstractIndexFieldData<GeoPointDoubleArrayAtomicFieldData> implements IndexGeoPointFieldData<GeoPointDoubleArrayAtomicFieldData> {

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData build(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType type, IndexFieldDataCache cache) {
            return new GeoPointDoubleArrayIndexFieldData(index, indexSettings, fieldNames, type, cache);
        }
    }

    public GeoPointDoubleArrayIndexFieldData(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType fieldDataType, IndexFieldDataCache cache) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
    }

    @Override
    public boolean valuesOrdered() {
        // because we might have single values? we can dynamically update a flag to reflect that
        // based on the atomic field data loaded
        return false;
    }

    @Override
    public GeoPointDoubleArrayAtomicFieldData load(AtomicReaderContext context) {
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
    public GeoPointDoubleArrayAtomicFieldData loadDirect(AtomicReaderContext context) throws Exception {
        AtomicReader reader = context.reader();

        Terms terms = reader.terms(getFieldNames().indexName());
        if (terms == null) {
            return GeoPointDoubleArrayAtomicFieldData.EMPTY;
        }

        // TODO: how can we guess the number of terms? numerics end up creating more terms per value...
        final TDoubleArrayList lat = new TDoubleArrayList();
        final TDoubleArrayList lon = new TDoubleArrayList();
        ArrayList<int[]> ordinals = new ArrayList<int[]>();
        int[] idx = new int[reader.maxDoc()];
        ordinals.add(new int[reader.maxDoc()]);

        lat.add(0); // first "t" indicates null value
        lon.add(0); // first "t" indicates null value
        int termOrd = 1;  // current term number

        TermsEnum termsEnum = terms.iterator(null);
        try {
            DocsEnum docsEnum = null;
            for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {

                String location = term.utf8ToString();
                int comma = location.indexOf(',');
                lat.add(Double.parseDouble(location.substring(0, comma)));
                lon.add(Double.parseDouble(location.substring(comma + 1)));

                docsEnum = termsEnum.docs(reader.getLiveDocs(), docsEnum, 0);
                for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                    int[] ordinal;
                    if (idx[docId] >= ordinals.size()) {
                        ordinal = new int[reader.maxDoc()];
                        ordinals.add(ordinal);
                    } else {
                        ordinal = ordinals.get(idx[docId]);
                    }
                    ordinal[docId] = termOrd;
                    idx[docId]++;
                }
                termOrd++;
            }
        } catch (RuntimeException e) {
            if (e.getClass().getName().endsWith("StopFillCacheException")) {
                // all is well, in case numeric parsers are used.
            } else {
                throw e;
            }
        }

        if (ordinals.size() == 1) {
            int[] nativeOrdinals = ordinals.get(0);
            FixedBitSet set = new FixedBitSet(reader.maxDoc());
            double[] sLat = new double[reader.maxDoc()];
            double[] sLon = new double[reader.maxDoc()];
            boolean allHaveValue = true;
            for (int i = 0; i < nativeOrdinals.length; i++) {
                int nativeOrdinal = nativeOrdinals[i];
                if (nativeOrdinal == 0) {
                    allHaveValue = false;
                } else {
                    set.set(i);
                    sLat[i] = lat.get(nativeOrdinal);
                    sLon[i] = lon.get(nativeOrdinal);
                }
            }
            if (allHaveValue) {
                return new GeoPointDoubleArrayAtomicFieldData.Single(sLon, sLat, reader.maxDoc());
            } else {
                return new GeoPointDoubleArrayAtomicFieldData.SingleFixedSet(sLon, sLat, reader.maxDoc(), set);
            }
        } else {
            int[][] nativeOrdinals = new int[ordinals.size()][];
            for (int i = 0; i < nativeOrdinals.length; i++) {
                nativeOrdinals[i] = ordinals.get(i);
            }
            return new GeoPointDoubleArrayAtomicFieldData.WithOrdinals(
                    lon.toArray(new double[lon.size()]),
                    lat.toArray(new double[lat.size()]),
                    reader.maxDoc(),
                    Ordinals.Factories.createFromFlatOrdinals(nativeOrdinals, termOrd, fieldDataType.getSettings())
            );
        }
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue) {
        throw new ElasticSearchIllegalArgumentException("can't sort on geo_point field without using specific sorting feature, like geo_distance");
    }
}
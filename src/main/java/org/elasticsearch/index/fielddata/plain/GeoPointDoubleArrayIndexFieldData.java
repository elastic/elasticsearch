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
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.*;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigDoubleArrayList;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.settings.IndexSettings;

/**
 */
public class GeoPointDoubleArrayIndexFieldData extends AbstractIndexFieldData<GeoPointDoubleArrayAtomicFieldData> implements IndexGeoPointFieldData<GeoPointDoubleArrayAtomicFieldData> {

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<?> build(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType type, IndexFieldDataCache cache) {
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
    public GeoPointDoubleArrayAtomicFieldData loadDirect(AtomicReaderContext context) throws Exception {
        AtomicReader reader = context.reader();

        Terms terms = reader.terms(getFieldNames().indexName());
        if (terms == null) {
            return GeoPointDoubleArrayAtomicFieldData.empty(reader.maxDoc());
        }
        // TODO: how can we guess the number of terms? numerics end up creating more terms per value...
        final BigDoubleArrayList lat = new BigDoubleArrayList();
        final BigDoubleArrayList lon = new BigDoubleArrayList();
        lat.add(0); // first "t" indicates null value
        lon.add(0); // first "t" indicates null value
        final float acceptableTransientOverheadRatio = fieldDataType.getSettings().getAsFloat("acceptable_transient_overhead_ratio", OrdinalsBuilder.DEFAULT_ACCEPTABLE_OVERHEAD_RATIO);
        OrdinalsBuilder builder = new OrdinalsBuilder(terms.size(), reader.maxDoc(), acceptableTransientOverheadRatio);
        final CharsRef spare = new CharsRef();
        try {
            BytesRefIterator iter = builder.buildFromTerms(terms.iterator(null));
            BytesRef term;
            while ((term = iter.next()) != null) {
                UnicodeUtil.UTF8toUTF16(term, spare);
                boolean parsed = false;
                for (int i = spare.offset; i < spare.length; i++) {
                    if (spare.chars[i] == ',') { // safes a string creation 
                        lat.add(Double.parseDouble(new String(spare.chars, spare.offset, (i - spare.offset))));
                        lon.add(Double.parseDouble(new String(spare.chars, (spare.offset + (i + 1)), spare.length - ((i + 1) - spare.offset))));
                        parsed = true;
                        break;
                    }
                }
                assert parsed;
            }

            Ordinals build = builder.build(fieldDataType.getSettings());
            if (!build.isMultiValued() && CommonSettings.removeOrdsOnSingleValue(fieldDataType)) {
                Docs ordinals = build.ordinals();
                int maxDoc = reader.maxDoc();
                BigDoubleArrayList sLat = new BigDoubleArrayList(reader.maxDoc());
                BigDoubleArrayList sLon = new BigDoubleArrayList(reader.maxDoc());
                for (int i = 0; i < maxDoc; i++) {
                    long nativeOrdinal = ordinals.getOrd(i);
                    sLat.add(lat.get(nativeOrdinal));
                    sLon.add(lon.get(nativeOrdinal));
                }
                FixedBitSet set = builder.buildDocsWithValuesSet();
                if (set == null) {
                    return new GeoPointDoubleArrayAtomicFieldData.Single(sLon, sLat, reader.maxDoc(), ordinals.getNumOrds());
                } else {
                    return new GeoPointDoubleArrayAtomicFieldData.SingleFixedSet(sLon, sLat, reader.maxDoc(), set, ordinals.getNumOrds());
                }
            } else {
                return new GeoPointDoubleArrayAtomicFieldData.WithOrdinals(
                        lon, lat,
                        reader.maxDoc(), build);
            }
        } finally {
            builder.close();
        }

    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, SortMode sortMode) {
        throw new ElasticSearchIllegalArgumentException("can't sort on geo_point field without using specific sorting feature, like geo_distance");
    }
}
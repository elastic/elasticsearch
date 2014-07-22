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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AtomicGeoPointFieldData;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.FieldMapper.Names;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;

abstract class AbstractIndexGeoPointFieldData extends AbstractIndexFieldData<AtomicGeoPointFieldData> implements IndexGeoPointFieldData {

    protected static class GeoPointEnum {

        private final BytesRefIterator termsEnum;
        private final GeoPoint next;
        private final CharsRef spare;

        protected GeoPointEnum(BytesRefIterator termsEnum) {
            this.termsEnum = termsEnum;
            next = new GeoPoint();
            spare = new CharsRef();
        }

        public GeoPoint next() throws IOException {
            final BytesRef term = termsEnum.next();
            if (term == null) {
                return null;
            }
            UnicodeUtil.UTF8toUTF16(term, spare);
            int commaIndex = -1;
            for (int i = 0; i < spare.length; i++) {
                if (spare.chars[spare.offset + i] == ',') { // saves a string creation
                    commaIndex = i;
                    break;
                }
            }
            if (commaIndex == -1) {
                assert false;
                return next.reset(0, 0);
            }
            final double lat = Double.parseDouble(new String(spare.chars, spare.offset, (commaIndex - spare.offset)));
            final double lon = Double.parseDouble(new String(spare.chars, (spare.offset + (commaIndex + 1)), spare.length - ((commaIndex + 1) - spare.offset)));
            return next.reset(lat, lon);
        }

    }

    public AbstractIndexGeoPointFieldData(Index index, Settings indexSettings, Names fieldNames, FieldDataType fieldDataType, IndexFieldDataCache cache) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
    }

    @Override
    public final XFieldComparatorSource comparatorSource(@Nullable Object missingValue, MultiValueMode sortMode) {
        throw new ElasticsearchIllegalArgumentException("can't sort on geo_point field without using specific sorting feature, like geo_distance");
    }

}

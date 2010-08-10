/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.search.facets.range;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.facets.Facet;
import org.elasticsearch.search.facets.FacetPhaseExecutionException;
import org.elasticsearch.search.facets.support.AbstractFacetCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class KeyValueRangeFacetCollector extends AbstractFacetCollector {

    private final String keyFieldName;
    private final String keyIndexFieldName;

    private final String valueFieldName;
    private final String valueIndexFieldName;

    private final FieldDataCache fieldDataCache;

    private final FieldData.Type keyFieldDataType;
    private NumericFieldData keyFieldData;

    private final FieldData.Type valueFieldDataType;
    private NumericFieldData valueFieldData;

    private final RangeFacet.Entry[] entries;

    public KeyValueRangeFacetCollector(String facetName, String keyFieldName, String valueFieldName, RangeFacet.Entry[] entries, SearchContext context) {
        super(facetName);
        this.keyFieldName = keyFieldName;
        this.valueFieldName = valueFieldName;
        this.entries = entries;
        this.fieldDataCache = context.fieldDataCache();

        MapperService.SmartNameFieldMappers smartMappers = context.mapperService().smartName(keyFieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + keyFieldName + "]");
        }

        // add type filter if there is exact doc mapper associated with it
        if (smartMappers.hasDocMapper()) {
            setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
        }

        keyIndexFieldName = smartMappers.mapper().names().indexName();
        keyFieldDataType = smartMappers.mapper().fieldDataType();

        FieldMapper mapper = context.mapperService().smartNameFieldMapper(valueFieldName);
        if (mapper == null) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for value_field [" + valueFieldName + "]");
        }
        valueIndexFieldName = mapper.names().indexName();
        valueFieldDataType = mapper.fieldDataType();
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        keyFieldData = (NumericFieldData) fieldDataCache.cache(keyFieldDataType, reader, keyIndexFieldName);
        valueFieldData = (NumericFieldData) fieldDataCache.cache(valueFieldDataType, reader, valueIndexFieldName);
    }

    @Override protected void doCollect(int doc) throws IOException {
        if (keyFieldData.multiValued()) {
            if (valueFieldData.multiValued()) {
                // both multi valued, intersect based on the minimum size
                double[] keys = keyFieldData.doubleValues(doc);
                double[] values = valueFieldData.doubleValues(doc);
                int size = Math.min(keys.length, values.length);
                for (int i = 0; i < size; i++) {
                    double key = keys[i];
                    for (RangeFacet.Entry entry : entries) {
                        if (key >= entry.getFrom() && key < entry.getTo()) {
                            entry.count++;
                            entry.total += values[i];
                        }
                    }
                }
            } else {
                // key multi valued, value is a single value
                double value = valueFieldData.doubleValue(doc);
                for (double key : keyFieldData.doubleValues(doc)) {
                    for (RangeFacet.Entry entry : entries) {
                        if (key >= entry.getFrom() && key < entry.getTo()) {
                            entry.count++;
                            entry.total += value;
                        }
                    }
                }
            }
        } else {
            double key = keyFieldData.doubleValue(doc);
            if (valueFieldData.multiValued()) {
                for (RangeFacet.Entry entry : entries) {
                    if (key >= entry.getFrom() && key < entry.getTo()) {
                        entry.count++;
                        for (double value : valueFieldData.doubleValues(doc)) {
                            entry.total += value;
                        }
                    }
                }
            } else {
                // both key and value are not multi valued
                double value = valueFieldData.doubleValue(doc);
                for (RangeFacet.Entry entry : entries) {
                    if (key >= entry.getFrom() && key < entry.getTo()) {
                        entry.count++;
                        entry.total += value;
                    }
                }
            }
        }
    }

    @Override public Facet facet() {
        return new InternalRangeDistanceFacet(facetName, keyFieldName, valueFieldName, entries);
    }
}

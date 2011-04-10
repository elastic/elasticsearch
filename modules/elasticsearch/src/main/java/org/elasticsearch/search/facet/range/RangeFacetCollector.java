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

package org.elasticsearch.search.facet.range;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class RangeFacetCollector extends AbstractFacetCollector {

    private final String indexFieldName;

    private final FieldDataCache fieldDataCache;

    private final FieldDataType fieldDataType;

    private NumericFieldData fieldData;

    private final RangeFacet.Entry[] entries;

    private final RangeProc rangeProc;

    public RangeFacetCollector(String facetName, String fieldName, RangeFacet.Entry[] entries, SearchContext context) {
        super(facetName);
        this.fieldDataCache = context.fieldDataCache();
        this.entries = entries;

        MapperService.SmartNameFieldMappers smartMappers = context.mapperService().smartName(fieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + fieldName + "]");
        }

        // add type filter if there is exact doc mapper associated with it
        if (smartMappers.hasDocMapper()) {
            setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
        }

        indexFieldName = smartMappers.mapper().names().indexName();
        fieldDataType = smartMappers.mapper().fieldDataType();

        rangeProc = new RangeProc(entries);
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        fieldData = (NumericFieldData) fieldDataCache.cache(fieldDataType, reader, indexFieldName);
    }

    @Override protected void doCollect(int doc) throws IOException {
        for (RangeFacet.Entry entry : entries) {
            entry.foundInDoc = false;
        }
        fieldData.forEachValueInDoc(doc, rangeProc);
    }

    @Override public Facet facet() {
        return new InternalRangeFacet(facetName, entries);
    }

    public static class RangeProc implements NumericFieldData.DoubleValueInDocProc {

        private final RangeFacet.Entry[] entries;

        public RangeProc(RangeFacet.Entry[] entries) {
            this.entries = entries;
        }

        @Override public void onValue(int docId, double value) {
            for (RangeFacet.Entry entry : entries) {
                if (entry.foundInDoc) {
                    continue;
                }
                if (value >= entry.getFrom() && value < entry.getTo()) {
                    entry.foundInDoc = true;
                    entry.count++;
                    entry.totalCount++;
                    entry.total += value;
                    if (value < entry.min) {
                        entry.min = value;
                    }
                    if (value > entry.max) {
                        entry.max = value;
                    }
                }
            }
        }
    }
}

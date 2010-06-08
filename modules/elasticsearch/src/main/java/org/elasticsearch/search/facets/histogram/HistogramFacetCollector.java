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

package org.elasticsearch.search.facets.histogram;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.index.cache.field.FieldDataCache;
import org.elasticsearch.index.field.FieldData;
import org.elasticsearch.index.field.NumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.facets.Facet;
import org.elasticsearch.search.facets.FacetPhaseExecutionException;
import org.elasticsearch.search.facets.support.AbstractFacetCollector;
import org.elasticsearch.util.gnu.trove.TLongDoubleHashMap;
import org.elasticsearch.util.gnu.trove.TLongLongHashMap;

import java.io.IOException;

import static org.elasticsearch.index.field.FieldDataOptions.*;

/**
 * @author kimchy (shay.banon)
 */
public class HistogramFacetCollector extends AbstractFacetCollector {

    private final String fieldName;

    private final long interval;

    private final HistogramFacet.ComparatorType comparatorType;

    private final FieldDataCache fieldDataCache;

    private final FieldData.Type fieldDataType;

    private NumericFieldData fieldData;

    private final HistogramProc histoProc;

    public HistogramFacetCollector(String facetName, String fieldName, long interval, HistogramFacet.ComparatorType comparatorType, FieldDataCache fieldDataCache, MapperService mapperService) {
        super(facetName);
        this.fieldName = fieldName;
        this.interval = interval;
        this.comparatorType = comparatorType;
        this.fieldDataCache = fieldDataCache;

        FieldMapper mapper = mapperService.smartNameFieldMapper(fieldName);
        if (mapper == null) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + fieldName + "]");
        }
        fieldDataType = mapper.fieldDataType();

        histoProc = new HistogramProc(interval);
    }

    @Override public void collect(int doc) throws IOException {
        fieldData.forEachValueInDoc(doc, histoProc);
    }

    @Override public void setNextReader(IndexReader reader, int docBase) throws IOException {
        fieldData = (NumericFieldData) fieldDataCache.cache(fieldDataType, reader, fieldName, fieldDataOptions().withFreqs(false));
    }

    @Override public Facet facet() {
        return new InternalHistogramFacet(facetName, fieldName, interval, comparatorType, histoProc.counts(), histoProc.totals());
    }

    public static class HistogramProc implements NumericFieldData.DoubleValueInDocProc {

        private final long interval;

        private final TLongLongHashMap counts = new TLongLongHashMap();

        private final TLongDoubleHashMap totals = new TLongDoubleHashMap();

        public HistogramProc(long interval) {
            this.interval = interval;
        }

        @Override public void onValue(int docId, double value) {
            long bucket = (((long) (value / interval)) * interval);
            counts.adjustOrPutValue(bucket, 1, 1);
            totals.adjustOrPutValue(bucket, value, value);
        }

        public TLongLongHashMap counts() {
            return counts;
        }

        public TLongDoubleHashMap totals() {
            return totals;
        }
    }
}
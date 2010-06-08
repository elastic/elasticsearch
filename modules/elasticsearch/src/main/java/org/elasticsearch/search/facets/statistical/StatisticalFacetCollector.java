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

package org.elasticsearch.search.facets.statistical;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.index.cache.field.FieldDataCache;
import org.elasticsearch.index.field.FieldData;
import org.elasticsearch.index.field.NumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.facets.Facet;
import org.elasticsearch.search.facets.FacetPhaseExecutionException;
import org.elasticsearch.search.facets.support.AbstractFacetCollector;

import java.io.IOException;

import static org.elasticsearch.index.field.FieldDataOptions.*;

/**
 * @author kimchy (shay.banon)
 */
public class StatisticalFacetCollector extends AbstractFacetCollector {

    private final String fieldName;

    private final FieldDataCache fieldDataCache;

    private final FieldData.Type fieldDataType;

    private NumericFieldData fieldData;

    private final StatsProc statsProc = new StatsProc();

    public StatisticalFacetCollector(String facetName, String fieldName, FieldDataCache fieldDataCache, MapperService mapperService) {
        super(facetName);
        this.fieldName = fieldName;
        this.fieldDataCache = fieldDataCache;

        FieldMapper mapper = mapperService.smartNameFieldMapper(fieldName);
        if (mapper == null) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + fieldName + "]");
        }
        fieldDataType = mapper.fieldDataType();
    }

    @Override public void collect(int doc) throws IOException {
        fieldData.forEachValueInDoc(doc, statsProc);
    }

    @Override public void setNextReader(IndexReader reader, int docBase) throws IOException {
        fieldData = (NumericFieldData) fieldDataCache.cache(fieldDataType, reader, fieldName, fieldDataOptions().withFreqs(false));
    }

    @Override public Facet facet() {
        return new InternalStatisticalFacet(facetName, statsProc.min(), statsProc.max(), statsProc.total(), statsProc.sumOfSquares(), statsProc.count());
    }

    public static class StatsProc implements NumericFieldData.DoubleValueInDocProc {

        private double min = Double.NaN;

        private double max = Double.NaN;

        private double total = 0;

        private double sumOfSquares = 0.0;

        private long count;

        @Override public void onValue(int docId, double value) {
            if (value < min || Double.isNaN(min)) {
                min = value;
            }
            if (value > max || Double.isNaN(max)) {
                max = value;
            }
            sumOfSquares += value * value;
            total += value;
            count++;
        }

        public final double min() {
            return min;
        }

        public final double max() {
            return max;
        }

        public final double total() {
            return total;
        }

        public final long count() {
            return count;
        }

        public final double sumOfSquares() {
            return sumOfSquares;
        }
    }
}

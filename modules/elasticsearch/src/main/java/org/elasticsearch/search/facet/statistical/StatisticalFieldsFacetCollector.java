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

package org.elasticsearch.search.facet.statistical;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class StatisticalFieldsFacetCollector extends AbstractFacetCollector {

    private final String[] indexFieldsNames;

    private final FieldDataCache fieldDataCache;

    private final FieldDataType[] fieldsDataType;

    private NumericFieldData[] fieldsData;

    private final StatsProc statsProc = new StatsProc();

    public StatisticalFieldsFacetCollector(String facetName, String[] fieldsNames, SearchContext context) {
        super(facetName);
        this.fieldDataCache = context.fieldDataCache();

        fieldsDataType = new FieldDataType[fieldsNames.length];
        fieldsData = new NumericFieldData[fieldsNames.length];
        indexFieldsNames = new String[fieldsNames.length];


        for (int i = 0; i < fieldsNames.length; i++) {
            FieldMapper mapper = context.mapperService().smartNameFieldMapper(fieldsNames[i]);
            if (mapper == null) {
                throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + fieldsNames[i] + "]");
            }
            indexFieldsNames[i] = mapper.names().indexName();
            fieldsDataType[i] = mapper.fieldDataType();
        }
    }

    @Override protected void doCollect(int doc) throws IOException {
        for (NumericFieldData fieldData : fieldsData) {
            fieldData.forEachValueInDoc(doc, statsProc);
        }
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        for (int i = 0; i < indexFieldsNames.length; i++) {
            fieldsData[i] = (NumericFieldData) fieldDataCache.cache(fieldsDataType[i], reader, indexFieldsNames[i]);
        }
    }

    @Override public Facet facet() {
        return new InternalStatisticalFacet(facetName, statsProc.min(), statsProc.max(), statsProc.total(), statsProc.sumOfSquares(), statsProc.count());
    }

    public static class StatsProc implements NumericFieldData.MissingDoubleValueInDocProc {

        double min = Double.POSITIVE_INFINITY;

        double max = Double.NEGATIVE_INFINITY;

        double total = 0;

        double sumOfSquares = 0.0;

        long count;

        int missing;

        @Override public void onValue(int docId, double value) {
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
            sumOfSquares += value * value;
            total += value;
            count++;
        }

        @Override public void onMissing(int docId) {
            missing++;
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

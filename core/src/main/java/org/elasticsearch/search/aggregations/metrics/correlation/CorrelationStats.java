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
package org.elasticsearch.search.aggregations.metrics.correlation;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Descriptive stats gathered per shard. Coordinating node computes final pearson product coefficient
 * based on these descriptive stats
 *
 * @internal
 */
class CorrelationStats implements Streamable {
    /** count of observations (same number of observations per field) */
    protected long count;
    /** per field sum of observations */
    protected HashMap<String, Double> fieldSum;
    /** per field sum of observations squared */
    protected HashMap<String, Double> fieldSqSum;
    /** upper triangular matrix sum of products */
    protected HashMap<String, HashMap<String, Double>> fieldSumProd;
    /** pearson product correlation coefficients */
    protected HashMap<String, HashMap<String, Double>> correlation;

    /** cache flag to avoid recompute if nothing has been added */
    private boolean dirty;

    /**
     * Base ctor
     */
    public CorrelationStats() {
        count = 0;
        fieldSum = new HashMap<>();
        fieldSqSum = new HashMap<>();
        fieldSumProd = new HashMap<>();
        dirty = true;
    }

    /**
     * adds a documents fields to the running statistics
     **/
    public void add(Map<String, Double> docFields) {
        // update cache flag
        dirty = true;

        // increment count
        count++;

        /* the following is used for sum of products */
        // deep copy of hash keys (field names)
        ArrayList<String> cFieldNames = new ArrayList(docFields.keySet());

        for (Map.Entry<String, Double> field : docFields.entrySet()) {
            final String fieldName = field.getKey();
            final double fieldValue = field.getValue();
            // add field sum
            if (fieldSum.containsKey(fieldName)) {
                fieldSum.put(fieldName, fieldSum.get(fieldName) + fieldValue);
            } else {
                fieldSum.put(fieldName, fieldValue);
            }

            // add sum of squares
            if (fieldSqSum.containsKey(fieldName)) {
                fieldSqSum.put(fieldName, fieldSqSum.get(fieldName) + fieldValue * fieldValue);
            } else {
                fieldSqSum.put(fieldName, fieldValue * fieldValue);
            }

            // add sum of products
            HashMap<String, Double> sumProdVals;
            if (fieldSumProd.containsKey(fieldName)) {
                sumProdVals = fieldSumProd.get(fieldName);
            } else {
                sumProdVals = new HashMap<>();
            }

            cFieldNames.remove(fieldName);  // removes diagonal value (corr of 1)
            for (String cFieldName : cFieldNames) {
                final double cFieldValue = docFields.get(cFieldName);
                if (sumProdVals.containsKey(cFieldName) == true) {
                    sumProdVals.put(cFieldName, sumProdVals.get(cFieldName) + fieldValue * cFieldValue);
                } else {
                    sumProdVals.put(cFieldName, fieldValue * cFieldValue);
                }
            }
            fieldSumProd.put(fieldName, sumProdVals);
        }
    }

    /**
     * Merges the descriptive statistics of a second data set (e.g., per shard)
     */
    public void merge(CorrelationStats other) {
        // update cache flag
        dirty = true;
        // merge count
        count += other.count;

        // across fields
        for (Map.Entry<String, Double> fs : other.fieldSum.entrySet()) {
            final String xFieldName = fs.getKey();
            // merge field sum
            fieldSum.put(xFieldName, fieldSum.get(xFieldName) + other.fieldSum.get(xFieldName));
            // merge fieldSqSum
            fieldSqSum.put(xFieldName, fieldSqSum.get(xFieldName) + other.fieldSqSum.get(xFieldName));
            // merge fieldSumProd
            Map<String, Double> row = fieldSumProd.get(xFieldName);
            for (Map.Entry<String, Double> col : row.entrySet()) {
                String colFieldName = col.getKey();
                double newVal = row.get(colFieldName);
                if (other.fieldSumProd.containsKey(xFieldName) && other.fieldSumProd.get(xFieldName).containsKey(colFieldName)) {
                    // val is in same row / col pair
                    newVal += other.fieldSumProd.get(xFieldName).get(colFieldName);
                } else {
                    // val is swapped
                    newVal += other.fieldSumProd.get(colFieldName).get(xFieldName);
                }
                row.put(colFieldName, newVal);
            }
        }
    }

    /**
     * computes Pearsons Correlation Coefficients using the descriptive stats
     **/
    public HashMap<String, HashMap<String, Double>> computeCorrelation() {
        // if nothing has been added we don't need to recompute
        if (dirty == false) {
            return this.correlation;
        }

        if (this.correlation == null) {
            this.correlation = new HashMap<>();
        }

        // compute the correlation as a function of total shard stats
        double num;
        double den;
        // first field
        for (Map.Entry sumProdPerField : fieldSumProd.entrySet()) {
            final String fieldName = (String)(sumProdPerField.getKey());
            HashMap<String, Double> fieldXCorr = new HashMap();
            // second field
            for (Map.Entry<String, Double> sumProd : ((HashMap<String, Double>)sumProdPerField.getValue()).entrySet()) {
                final String secFieldName = sumProd.getKey();
                final double xySum = sumProd.getValue();
                final double xSum = fieldSum.get(fieldName);
                final double xSqSum = fieldSqSum.get(fieldName);
                final double ySum = fieldSum.get(secFieldName);
                final double ySqSum = fieldSqSum.get(secFieldName);
                // compute the correlation
                num = count * xySum - xSum * ySum;
                den = Math.sqrt((count * xSqSum) - (xSum * xSum)) * Math.sqrt((count * ySqSum) - (ySum * ySum));
                fieldXCorr.put(secFieldName, num/den);
            }
            if (fieldXCorr.size() > 0) {
                correlation.put(fieldName, fieldXCorr);
            }
        }
        dirty = false;
        return this.correlation;
    }

    /**
     * Unmarshalls CorrelationStats
     * NOTE: correlation doesn't need to be serialized since its never passed between shards
     */
    @Override
    public void readFrom(StreamInput in) throws IOException {
        // read fieldSum
        if (in.readBoolean()) {
            fieldSum = (HashMap<String, Double>)(in.readGenericValue());
        } else {
            fieldSum = null;
        }
        // read fieldSqSum
        if (in.readBoolean()) {
            fieldSqSum = (HashMap<String, Double>)(in.readGenericValue());
        } else {
            fieldSqSum = null;
        }
        // read fieldSumProd
        if (in.readBoolean()) {
            fieldSumProd = (HashMap<String, HashMap<String, Double>>) (in.readGenericValue());
        } else {
            fieldSumProd = null;
        }
        // read correlation
        if (in.readBoolean()) {
            correlation = (HashMap<String, HashMap<String, Double>>) (in.readGenericValue());
        } else {
            correlation = null;
        }
    }

    /**
     * Marshalls CorrelationStats
     * NOTE: correlation doesn't need to be serialized since its never passed between shards
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // marshall fieldSum
        if (fieldSum != null) {
            out.writeBoolean(true);
            out.writeGenericValue(fieldSum);
        } else {
            out.writeBoolean(false);
        }
        // marshall fieldSqSum
        if (fieldSqSum != null) {
            out.writeBoolean(true);
            out.writeGenericValue(fieldSqSum);
        } else {
            out.writeBoolean(false);
        }
        // marshall fieldSumProd
        if (fieldSumProd != null) {
            out.writeBoolean(true);
            out.writeGenericValue(fieldSumProd);
        } else {
            out.writeBoolean(false);
        }
        // marshall correlation
        if (correlation != null) {
            out.writeBoolean(true);
            out.writeGenericValue(correlation);
        } else {
            out.writeBoolean(false);
        }
    }
}

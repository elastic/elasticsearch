/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.aggregations.pca;

import org.ejml.data.Complex_F64;
import org.ejml.data.DMatrixRMaj;
import org.ejml.dense.row.factory.DecompositionFactory_DDRM;
import org.ejml.interfaces.decomposition.EigenDecomposition_F64;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsResults;
import org.elasticsearch.search.aggregations.matrix.stats.RunningStats;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PCAStatsResults extends MatrixStatsResults {
    /** eigen values from PCA */
    protected final Map<String, Complex_F64> eigenVals;
    /** eigen vectors from PCA */
    protected final Map<String, DMatrixRMaj> eigenVectors;
    /** use the covariance matrix, instead of correlation matrix (default) to compute PCA **/
    protected boolean useCovariance;

    PCAStatsResults() {
        super();
        this.eigenVals = new HashMap<>();
        this.eigenVectors = new HashMap<>();
        this.useCovariance = false;
    }

    PCAStatsResults(RunningStats stats, boolean useCovariance) {
        super(stats);
        this.eigenVals = new HashMap<>();
        this.eigenVectors = new HashMap<>();
        this.useCovariance = useCovariance;
        this.compute();
    }

    @SuppressWarnings("unchecked")
    protected PCAStatsResults(StreamInput in) {
        super(in);
        try {
            // read eigen values
            int mapSize = in.readVInt();
            this.eigenVals = new HashMap<>(mapSize);
            for (int i = 0; i < mapSize; ++i) {
                String field = in.readString();
                double real = in.readDouble();
                double imag = in.readDouble();
                this.eigenVals.put(field, new Complex_F64(real, imag));
            }

            // read eigen vectors
            mapSize = in.readVInt();
            this.eigenVectors = new HashMap<>(mapSize);
            for (int i = 0; i < mapSize; ++i) {
                String field = in.readString();
                double[] matrixAsArray = in.readDoubleArray();
                this.eigenVectors.put(field, new DMatrixRMaj(matrixAsArray));
            }
        } catch (IOException e) {
            throw new ElasticsearchException("Error trying to create pca stats results from stream input", e);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        // marshall eigen values
        writeEigenVals(out);
        // marshall eigen vectors
        writeEigenVectors(out);
    }

    private void writeEigenVals(StreamOutput out) {
        try {
            out.writeVInt(eigenVals.size());
            Complex_F64 eigenValue;
            for (Map.Entry<String, Complex_F64> entry : eigenVals.entrySet()) {
                out.writeString(entry.getKey());
                eigenValue = entry.getValue();
                out.writeDouble(eigenValue.real);
                out.writeDouble(eigenValue.imaginary);
            }
        } catch (IOException e) {
            throw new ElasticsearchException("Error trying to write eigen values to stream output", e);
        }
    }

    private void writeEigenVectors(StreamOutput out) {
        try {
            out.writeVInt(eigenVectors.size());
            for (Map.Entry<String, DMatrixRMaj> entry : eigenVectors.entrySet()) {
                out.writeString(entry.getKey());
                out.writeDoubleArray(entry.getValue().data);
            }
        } catch (IOException e) {
            throw new ElasticsearchException("Error trying to read eigen values from stream input", e);
        }
    }

    public Complex_F64 getEigenValue(String field) {
        checkField(field, eigenVals);
        return eigenVals.get(field);
    }

    public double[] getEigenVector(String row) {
        checkField(row, eigenVectors);
        return eigenVectors.get(row).getData();
    }

    /** can override in follow on aggregations to compute additional statistics derived from those computed here */
    protected void compute() {
        // convert covariance matrix to a matrix object suitable for eigen decomposition
        int n = results.getCounts().size();
        DMatrixRMaj A = new DMatrixRMaj(n, n);
        Iterator<Map.Entry<String, Double>> rowNames = results.getMeans().entrySet().iterator();
        for (int r = 0; r < n; ++r) {
            String rowName = rowNames.next().getKey();
            Iterator<Map.Entry<String, Double>> colNames = results.getMeans().entrySet().iterator();
            for (int c = 0; c < n; ++c) {
                String colName = colNames.next().getKey();
                double val = useCovariance ? getCovariance(rowName, colName) : getCorrelation(rowName, colName);
                if (Double.isNaN(val) == true) {
                    String errorValue = useCovariance ? "Covariance" : "Correlation";
                    throw new ElasticsearchException("Unable to compute PCA. " + errorValue + " for fields ["
                        +  rowName + "], [" + colName + "] is [NaN]");
                }
                A.set(r, c, val);
            }
        }

        // compute PCA as an eigendecomposition of the covariance matrix
        EigenDecomposition_F64<DMatrixRMaj> decomp = DecompositionFactory_DDRM.eig(A.numRows, true);
        if( !decomp.decompose(A) ) {
            throw new ElasticsearchException("Unable to dompute PCA. Eigen decomposition failed.");
        }

        int i = 0;
        for (String fieldName : results.getMeans().keySet()) {
            eigenVals.put(fieldName, decomp.getEigenvalue(i));
            eigenVectors.put(fieldName, decomp.getEigenVector(i++));
        }
    }

//    @Override
//    protected boolean doEquals(Object obj) {
//
//    }
}

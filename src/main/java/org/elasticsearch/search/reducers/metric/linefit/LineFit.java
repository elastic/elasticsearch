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

package org.elasticsearch.search.reducers.metric.linefit;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.reducers.ReductionExecutionException;
import org.elasticsearch.search.reducers.metric.MetricOp;
import org.elasticsearch.search.reducers.metric.MetricsBuilder;

import java.io.IOException;

public class LineFit extends MetricOp {

    public static final String TYPE = "linefit";

    public LineFit() {
        super(TYPE);
    }

    boolean outputFittedLine;

    public LineFitResult evaluate(Object[] bucketProperties) throws ReductionExecutionException {


        int xDim = 1;
        int yDim = bucketProperties.length;
        RealMatrix Theta = MatrixUtils.createRealMatrix(xDim + 1, yDim); // holds the xes, metric values or count of the buckets
        Theta = Theta.scalarAdd(1.0);
        RealMatrix t = MatrixUtils.createRealMatrix(yDim, 1); // holds the measured values (metric value that is to be predicted)

        for (int i = 0; i < bucketProperties.length; i++) {
            Theta.setEntry(1, i, 1.0);
        }
        for (int i = 0; i < bucketProperties.length; i++) {
            Theta.setEntry(0, i, i); // NOCOMMIT - we do not have the x axis values yet. once we have it this must be filled in here
        }
        for (int i = 0; i < bucketProperties.length; i++) {
            t.setEntry(i, 0, ((Number) bucketProperties[i]).doubleValue());
        }

        RealMatrix penroseMoorePseudoInverse = Theta.multiply(Theta.transpose());
        penroseMoorePseudoInverse = MatrixUtils.inverse(penroseMoorePseudoInverse);
        penroseMoorePseudoInverse = penroseMoorePseudoInverse.multiply(Theta);
        RealMatrix w = penroseMoorePseudoInverse.multiply(t);
        double[] modelValues = null;
        if (outputFittedLine) {
            modelValues = new double[bucketProperties.length];
            for (int i = 0; i < bucketProperties.length; i++) {
                modelValues[i] = Theta.getEntry(0, i) * w.getEntry(0, 0) + w.getEntry(1, 0);
            }
        }

        return new LineFitResult(w.getEntry(0, 0), w.getEntry(1, 0), modelValues);
    }

    protected boolean parseParameter(String currentFieldName, XContentParser parser) throws IOException {
        if (currentFieldName.equals("output_fitted_line")) {
            outputFittedLine = parser.booleanValue();
            return true;
        }
        return false;
    }

    public void readFrom(StreamInput in) throws IOException {
        this.outputFittedLine = in.readBoolean();
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(outputFittedLine);
    }

    public static class LineFitBuilder extends MetricsBuilder {

        boolean outputFittedLine = false;

        public LineFitBuilder(String name) {
            super(name, TYPE);
        }

        public LineFitBuilder outputFittedLine(boolean outPutFittedLine) {
            this.outputFittedLine = outPutFittedLine;
            return this;
        }

        @Override
        protected XContentBuilder buildCustomParameters(XContentBuilder builder) throws IOException {
            builder.field("output_fitted_line", outputFittedLine);
            return builder;
        }
    }
}

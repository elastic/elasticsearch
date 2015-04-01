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

package org.elasticsearch.search.aggregations.reducers.acf;

import com.google.common.base.Function;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.reducers.*;
import org.elasticsearch.search.aggregations.reducers.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;
import org.jtransforms.fft.DoubleFFT_1D;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.reducers.BucketHelpers.resolveBucketValue;


public class AcfReducer extends SiblingReducer {

    public final static Type TYPE = new Type("acf");

    public final static ReducerStreams.Stream STREAM = new ReducerStreams.Stream() {
        @Override
        public AcfReducer readResult(StreamInput in) throws IOException {
            AcfReducer result = new AcfReducer();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        ReducerStreams.registerStream(STREAM, TYPE.stream());
    }


    private ValueFormatter formatter;
    private GapPolicy gapPolicy;
    private AcfSettings settings;

    public AcfReducer() {
    }

    public AcfReducer(String name, String[] bucketsPaths, @Nullable ValueFormatter formatter, GapPolicy gapPolicy,
                      AcfSettings settings,  Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
        this.settings = settings;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAggregation doReduce(Aggregations aggregations, ReduceContext context) {

        List<String> bucketsPath = AggregationPath.parse(bucketsPaths()[0]).getPathElementsAsStringList();

        int window = settings.getWindow();

        for (Aggregation aggregation : aggregations) {
            if (aggregation.getName().equals(bucketsPath.get(0))) {

                bucketsPath = bucketsPath.subList(1, bucketsPath.size());

                InternalHistogram histo = (InternalHistogram) aggregation;
                List<? extends InternalHistogram.Bucket> buckets = histo.getBuckets();

                double[] values;
                if (settings.isZeroPad()) {
                    // we need to pad with `n` zeros so that it doesn't wrap
                    // But ideally we pad with a power of 2 for performance
                    // (although JTransforms supports non-power 2... TODO benchmark if it matters?)
                    int pad = Integer.highestOneBit((window * 2) - 1);
                    values = new double[pad << 1];
                } else {
                    values = new double[window];
                }


                int counter = 0;
                for (int i = buckets.size() - window; i < window; i++) {
                    values[counter] = resolveBucketValue(histo, buckets.get(i), bucketsPath.get(0), gapPolicy);
                    counter += 1;
                }

                // Consumes values and updates in place
                fftAutoCorrelate(values);

                if (settings.isZeroPad()) {
                    values = Arrays.copyOfRange(values, 0, settings.getWindow());
                }

                return new InternalAcfValues(name(), values, formatter, Collections.EMPTY_LIST, metaData());

            }
        }
        return null;
    }

    /**
     * Find the autocorrelation for a time series.  Assumes no gaps in the data!
     *
     * @param values Data in time-domain.  Values are consumed and updated in-place! The
     *               autocorrelation result will be in <code>values</code> after the method returns.
     */
    private void fftAutoCorrelate(double[] values) {

        DoubleFFT_1D fft = new DoubleFFT_1D(values.length);
        int window = settings.getWindow();

        // FFT returns complex numbers in the original array, layout depends on odd/even-ness of length
        fft.realForward(values);

        // calculate the power spectral density.  Autocovariance is the inverse FFT of the PSD
        computePSD(values);

        // invert to get back to time-domain
        fft.realInverse(values, true);

        // If we zero-padded, we need to calculate a mask to correct the AC.
        // Mask is set to 1 where original signal was, 0 for padding
        // The mask goes through the same FFT->PSD->iFFT process
        if (settings.isZeroPad()) {
            double[] mask = new double[values.length];
            Arrays.fill(mask, 0, window, 1.0);

            fft.realForward(mask);
            computePSD(mask);
            fft.realInverse(mask, true);

            for (int i = 0; i < window; i++) {
                values[i] /= mask[i];
            }
        }

        // Normalization converts the autocovariance into an autocorrelation
        if (settings.isNormalize()) {
            // Divide by variance.  Use window as size, since we may have padded
            for (int i = 1; i < window; i++) {
                values[i] /= values[0];
            }
            values[0] = 1;
        }
    }

    /**
     * Computer power spectral density of the FFT.
     *
     * @param values array containing a freq-domain series from an FFT.
     *               This is consumed and updated in-place!
     */
    private void computePSD(double[] values){
        int length = values.length;

        // First FFT bin is 0Hz.
        // zeroing out the first bin is equivalent to "centering" the series.  E.g. dividing by the mean
        if (settings.isZeroMean()) {
            values[0] = 0;
        } else {
            // Calculate the PSD if we don't want to zero it out
            values[0] = Math.pow(values[0], 2);
        }

        if (length % 2 == 0) {
            values[1] = Math.pow(values[1], 2);
            //values[1] = 0;

            for (int i = 2; i < length; i += 2) {
                // PSD is the magnitude of the complex number, squared
                // psd = (sqrt(r^2 + i^2))^2
                //     = r^2 + i^2
                values[i] = Math.pow(values[i], 2) + Math.pow(values[i+1], 2);
                values[i+1] = 0;
            }
        } else {
            int i = 2;
            for (; i < length - 1; i += 2) {
                values[i] = Math.pow(values[i], 2) + Math.pow(values[i+1], 2);
                values[i+1] = 0;
            }

            // Last real component is at end of array, but corresponding imaginary is at second index position
            values[i] = Math.pow(values[i], 2) + Math.pow(values[1], 2);
            values[1] = 0;
        }
    }


    @Override
    public void doReadFrom(StreamInput in) throws IOException {
        formatter = ValueFormatterStreams.readOptional(in);
        gapPolicy = GapPolicy.readFrom(in);
        settings = AcfSettings.doReadFrom(in);
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(formatter, out);
        gapPolicy.writeTo(out);
        settings.writeTo(out);
    }

    public static class Factory extends ReducerFactory {

        private final ValueFormatter formatter;
        private GapPolicy gapPolicy;
        private AcfSettings settings;

        public Factory(String name, String[] bucketsPaths, @Nullable ValueFormatter formatter, GapPolicy gapPolicy,
                        AcfSettings settings) {
            super(name, TYPE.name(), bucketsPaths);
            this.formatter = formatter;
            this.gapPolicy = gapPolicy;
            this.settings = settings;
        }

        @Override
        protected Reducer createInternal(Map<String, Object> metaData) throws IOException {
            return new AcfReducer(name, bucketsPaths, formatter, gapPolicy, settings, metaData);
        }

        @Override
        public void doValidate(AggregatorFactory parent, AggregatorFactory[] aggFactories, List<ReducerFactory> reducerFactories) {
            //nocommit
            //TODO this never seems to get called?
        }

    }
}

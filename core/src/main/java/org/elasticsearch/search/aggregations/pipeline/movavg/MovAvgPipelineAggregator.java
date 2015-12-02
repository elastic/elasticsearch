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

package org.elasticsearch.search.aggregations.pipeline.movavg;

import org.elasticsearch.common.collect.EvictingQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorStreams;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModelStreams;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.SimpleModel;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class MovAvgPipelineAggregator extends PipelineAggregator {

    public final static Type TYPE = new Type("moving_avg");

    public final static PipelineAggregatorStreams.Stream STREAM = new PipelineAggregatorStreams.Stream() {
        @Override
        public MovAvgPipelineAggregator readResult(StreamInput in) throws IOException {
            MovAvgPipelineAggregator result = new MovAvgPipelineAggregator();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        PipelineAggregatorStreams.registerStream(STREAM, TYPE.stream());
    }

    private ValueFormatter formatter;
    private GapPolicy gapPolicy;
    private int window;
    private MovAvgModel model;
    private int predict;
    private boolean minimize;

    public MovAvgPipelineAggregator() {
    }

    public MovAvgPipelineAggregator(String name, String[] bucketsPaths, ValueFormatter formatter, GapPolicy gapPolicy,
                         int window, int predict, MovAvgModel model, boolean minimize, Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
        this.window = window;
        this.model = model;
        this.predict = predict;
        this.minimize = minimize;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalHistogram histo = (InternalHistogram) aggregation;
        List<? extends InternalHistogram.Bucket> buckets = histo.getBuckets();
        InternalHistogram.Factory<? extends InternalHistogram.Bucket> factory = histo.getFactory();

        List newBuckets = new ArrayList<>();
        EvictingQueue<Double> values = new EvictingQueue<>(this.window);

        long lastValidKey = 0;
        int lastValidPosition = 0;
        int counter = 0;

        // Do we need to fit the model parameters to the data?
        if (minimize) {
            assert (model.canBeMinimized());
            model = minimize(buckets, histo, model);
        }

        for (InternalHistogram.Bucket bucket : buckets) {
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPaths()[0], gapPolicy);

            // Default is to reuse existing bucket.  Simplifies the rest of the logic,
            // since we only change newBucket if we can add to it
            InternalHistogram.Bucket newBucket = bucket;

            if (!(thisBucketValue == null || thisBucketValue.equals(Double.NaN))) {

                // Some models (e.g. HoltWinters) have certain preconditions that must be met
                if (model.hasValue(values.size())) {
                    double movavg = model.next(values);

                    List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false).map((p) -> {
                        return (InternalAggregation) p;
                    }).collect(Collectors.toList());
                    aggs.add(new InternalSimpleValue(name(), movavg, formatter, new ArrayList<PipelineAggregator>(), metaData()));
                    newBucket = factory.createBucket(bucket.getKey(), bucket.getDocCount(), new InternalAggregations(
                            aggs), bucket.getKeyed(), bucket.getFormatter());
                }

                if (predict > 0) {
                    if (bucket.getKey() instanceof Number) {
                        lastValidKey  = ((Number) bucket.getKey()).longValue();
                    } else if (bucket.getKey() instanceof DateTime) {
                        lastValidKey = ((DateTime) bucket.getKey()).getMillis();
                    } else {
                        throw new AggregationExecutionException("Expected key of type Number or DateTime but got [" + lastValidKey + "]");
                    }
                    lastValidPosition = counter;
                }

                values.offer(thisBucketValue);
            }
            counter += 1;
            newBuckets.add(newBucket);

        }

        if (buckets.size() > 0 && predict > 0) {

            boolean keyed;
            ValueFormatter formatter;
            keyed = buckets.get(0).getKeyed();
            formatter = buckets.get(0).getFormatter();

            double[] predictions = model.predict(values, predict);
            for (int i = 0; i < predictions.length; i++) {

                List<InternalAggregation> aggs;
                long newKey = histo.getRounding().nextRoundingValue(lastValidKey);

                if (lastValidPosition + i + 1 < newBuckets.size()) {
                    InternalHistogram.Bucket bucket = (InternalHistogram.Bucket) newBuckets.get(lastValidPosition + i + 1);

                    // Get the existing aggs in the bucket so we don't clobber data
                    aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false).map((p) -> {
                        return (InternalAggregation) p;
                    }).collect(Collectors.toList());
                    aggs.add(new InternalSimpleValue(name(), predictions[i], formatter, new ArrayList<PipelineAggregator>(), metaData()));

                    InternalHistogram.Bucket newBucket = factory.createBucket(newKey, 0, new InternalAggregations(
                            aggs), keyed, formatter);

                    // Overwrite the existing bucket with the new version
                    newBuckets.set(lastValidPosition + i + 1, newBucket);

                } else {
                    // Not seen before, create fresh
                    aggs = new ArrayList<>();
                    aggs.add(new InternalSimpleValue(name(), predictions[i], formatter, new ArrayList<PipelineAggregator>(), metaData()));

                    InternalHistogram.Bucket newBucket = factory.createBucket(newKey, 0, new InternalAggregations(
                            aggs), keyed, formatter);

                    // Since this is a new bucket, simply append it
                    newBuckets.add(newBucket);
                }
                lastValidKey = newKey;
            }
        }

        return factory.create(newBuckets, histo);
    }

    private MovAvgModel minimize(List<? extends InternalHistogram.Bucket> buckets, InternalHistogram histo, MovAvgModel model) {

        int counter = 0;
        EvictingQueue<Double> values = new EvictingQueue<>(this.window);

        double[] test = new double[window];
        ListIterator<? extends InternalHistogram.Bucket> iter = buckets.listIterator(buckets.size());

        // We have to walk the iterator backwards because we don't know if/how many buckets are empty.
        while (iter.hasPrevious() && counter < window) {

            Double thisBucketValue = resolveBucketValue(histo, iter.previous(), bucketsPaths()[0], gapPolicy);

            if (!(thisBucketValue == null || thisBucketValue.equals(Double.NaN))) {
                test[window - counter - 1] = thisBucketValue;
                counter += 1;
            }
        }

        // If we didn't fill the test set, we don't have enough data to minimize.
        // Just return the model with the starting coef
        if (counter < window) {
            return model;
        }

        //And do it again, for the train set.  Unfortunately we have to fill an array and then
        //fill an evicting queue backwards :(

        counter = 0;
        double[] train = new double[window];

        while (iter.hasPrevious() && counter < window) {

            Double thisBucketValue = resolveBucketValue(histo, iter.previous(), bucketsPaths()[0], gapPolicy);

            if (!(thisBucketValue == null || thisBucketValue.equals(Double.NaN))) {
                train[window - counter - 1] = thisBucketValue;
                counter += 1;
            }
        }

        // If we didn't fill the train set, we don't have enough data to minimize.
        // Just return the model with the starting coef
        if (counter < window) {
            return model;
        }

        for (double v : train) {
            values.add(v);
        }

        return SimulatedAnealingMinimizer.minimize(model, values, test);
    }

    @Override
    public void doReadFrom(StreamInput in) throws IOException {
        formatter = ValueFormatterStreams.readOptional(in);
        gapPolicy = GapPolicy.readFrom(in);
        window = in.readVInt();
        predict = in.readVInt();
        model = MovAvgModelStreams.read(in);
        minimize = in.readBoolean();

    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(formatter, out);
        gapPolicy.writeTo(out);
        out.writeVInt(window);
        out.writeVInt(predict);
        model.writeTo(out);
        out.writeBoolean(minimize);

    }

    public static class Factory extends PipelineAggregatorFactory {

        private String format;
        private GapPolicy gapPolicy = GapPolicy.SKIP;
        private int window = 5;
        private MovAvgModel model = new SimpleModel();
        private int predict = 0;
        private Boolean minimize;

        public Factory(String name, String[] bucketsPaths) {
            super(name, TYPE.name(), bucketsPaths);
        }

        /**
         * Sets the format to use on the output of this aggregation.
         */
        public void format(String format) {
            this.format = format;
        }

        /**
         * Gets the format to use on the output of this aggregation.
         */
        public String format() {
            return format;
        }

        /**
         * Sets the GapPolicy to use on the output of this aggregation.
         */
        public void gapPolicy(GapPolicy gapPolicy) {
            this.gapPolicy = gapPolicy;
        }

        /**
         * Gets the GapPolicy to use on the output of this aggregation.
         */
        public GapPolicy gapPolicy() {
            return gapPolicy;
        }

        protected ValueFormatter formatter() {
            if (format != null) {
                return ValueFormat.Patternable.Number.format(format).formatter();
            } else {
                return ValueFormatter.RAW;
            }
        }

        /**
         * Sets the window size for the moving average. This window will "slide"
         * across the series, and the values inside that window will be used to
         * calculate the moving avg value
         *
         * @param window
         *            Size of window
         */
        public void window(int window) {
            this.window = window;
        }

        /**
         * Gets the window size for the moving average. This window will "slide"
         * across the series, and the values inside that window will be used to
         * calculate the moving avg value
         */
        public int window() {
            return window;
        }

        /**
         * Sets a MovAvgModel for the Moving Average. The model is used to
         * define what type of moving average you want to use on the series
         *
         * @param model
         *            A MovAvgModel which has been prepopulated with settings
         */
        public void model(MovAvgModel model) {
            this.model = model;
        }

        /**
         * Gets a MovAvgModel for the Moving Average. The model is used to
         * define what type of moving average you want to use on the series
         */
        public MovAvgModel model() {
            return model;
        }

        /**
         * Sets the number of predictions that should be returned. Each
         * prediction will be spaced at the intervals specified in the
         * histogram. E.g "predict: 2" will return two new buckets at the end of
         * the histogram with the predicted values.
         *
         * @param predict
         *            Number of predictions to make
         */
        public void predict(int predict) {
            this.predict = predict;
        }

        /**
         * Gets the number of predictions that should be returned. Each
         * prediction will be spaced at the intervals specified in the
         * histogram. E.g "predict: 2" will return two new buckets at the end of
         * the histogram with the predicted values.
         */
        public int predict() {
            return predict;
        }

        /**
         * Sets whether the model should be fit to the data using a cost
         * minimizing algorithm.
         *
         * @param minimize
         *            If the model should be fit to the underlying data
         */
        public void minimize(boolean minimize) {
            this.minimize = minimize;
        }

        /**
         * Gets whether the model should be fit to the data using a cost
         * minimizing algorithm.
         */
        public Boolean minimize() {
            return minimize;
        }

        @Override
        protected PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException {
            // If the user doesn't set a preference for cost minimization, ask
            // what the model prefers
            boolean minimize = this.minimize == null ? model.minimizeByDefault() : this.minimize;
            return new MovAvgPipelineAggregator(name, bucketsPaths, formatter(), gapPolicy, window, predict, model, minimize, metaData);
        }

        @Override
        public void doValidate(AggregatorFactory parent, AggregatorFactory[] aggFactories,
                List<PipelineAggregatorFactory> pipelineAggregatoractories) {
            if (minimize != null && minimize && !model.canBeMinimized()) {
                // If the user asks to minimize, but this model doesn't support
                // it, throw exception
                throw new IllegalStateException("The [" + model + "] model cannot be minimized for aggregation [" + name + "]");
            }
            if (bucketsPaths.length != 1) {
                throw new IllegalStateException(PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                        + " must contain a single entry for aggregation [" + name + "]");
            }
            if (!(parent instanceof HistogramAggregator.Factory)) {
                throw new IllegalStateException("moving average aggregation [" + name
                        + "] must have a histogram or date_histogram as parent");
            } else {
                HistogramAggregator.Factory histoParent = (HistogramAggregator.Factory) parent;
                if (histoParent.minDocCount() != 0) {
                    throw new IllegalStateException("parent histogram of moving average aggregation [" + name
                            + "] must have min_doc_count of 0");
                }
            }
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
            if (format != null) {
                builder.field(MovAvgParser.FORMAT.getPreferredName(), format);
            }
            builder.field(MovAvgParser.GAP_POLICY.getPreferredName(), gapPolicy.getName());
            model.toXContent(builder, params);
            builder.field(MovAvgParser.WINDOW.getPreferredName(), window);
            if (predict > 0) {
                builder.field(MovAvgParser.PREDICT.getPreferredName(), predict);
            }
            if (minimize != null) {
                builder.field(MovAvgParser.MINIMIZE.getPreferredName(), minimize);
            }
            return builder;
        }

        @Override
        protected PipelineAggregatorFactory doReadFrom(String name, String[] bucketsPaths, StreamInput in) throws IOException {
            Factory factory = new Factory(name, bucketsPaths);
            factory.format = in.readOptionalString();
            factory.gapPolicy = GapPolicy.readFrom(in);
            factory.window = in.readVInt();
            factory.model = MovAvgModelStreams.read(in);
            factory.predict = in.readVInt();
            factory.minimize = in.readOptionalBoolean();
            return factory;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeOptionalString(format);
            gapPolicy.writeTo(out);
            out.writeVInt(window);
            model.writeTo(out);
            out.writeVInt(predict);
            out.writeOptionalBoolean(minimize);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(format, gapPolicy, window, model, predict, minimize);
        }

        @Override
        protected boolean doEquals(Object obj) {
            Factory other = (Factory) obj;
            return Objects.equals(format, other.format)
                    && Objects.equals(gapPolicy, other.gapPolicy)
                    && Objects.equals(window, other.window)
                    && Objects.equals(model, other.model)
                    && Objects.equals(predict, other.predict)
                    && Objects.equals(minimize, other.minimize);
        }

    }
}

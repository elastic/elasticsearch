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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.histogram.AbstractHistogramAggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModelBuilder;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModelStreams;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.SimpleModel;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MovAvgPipelineAggregatorBuilder extends PipelineAggregatorBuilder<MovAvgPipelineAggregatorBuilder> {

    static final MovAvgPipelineAggregatorBuilder PROTOTYPE = new MovAvgPipelineAggregatorBuilder("", "");

    private String format;
    private GapPolicy gapPolicy = GapPolicy.SKIP;
    private int window = 5;
    private MovAvgModel model = new SimpleModel();
    private int predict = 0;
    private Boolean minimize;

    public MovAvgPipelineAggregatorBuilder(String name, String bucketsPath) {
        this(name, new String[] { bucketsPath });
    }

    private MovAvgPipelineAggregatorBuilder(String name, String[] bucketsPaths) {
        super(name, MovAvgPipelineAggregator.TYPE.name(), bucketsPaths);
    }

    /**
     * Sets the format to use on the output of this aggregation.
     */
    public MovAvgPipelineAggregatorBuilder format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("[format] must not be null: [" + name + "]");
        }
        this.format = format;
        return this;
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
    public MovAvgPipelineAggregatorBuilder gapPolicy(GapPolicy gapPolicy) {
        if (gapPolicy == null) {
            throw new IllegalArgumentException("[gapPolicy] must not be null: [" + name + "]");
        }
        this.gapPolicy = gapPolicy;
        return this;
    }

    /**
     * Gets the GapPolicy to use on the output of this aggregation.
     */
    public GapPolicy gapPolicy() {
        return gapPolicy;
    }

    protected DocValueFormat formatter() {
        if (format != null) {
            return new DocValueFormat.Decimal(format);
        } else {
            return DocValueFormat.RAW;
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
    public MovAvgPipelineAggregatorBuilder window(int window) {
        if (window <= 0) {
            throw new IllegalArgumentException("[window] must be a positive integer: [" + name + "]");
        }
        this.window = window;
        return this;
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
    public MovAvgPipelineAggregatorBuilder modelBuilder(MovAvgModelBuilder model) {
        if (model == null) {
            throw new IllegalArgumentException("[model] must not be null: [" + name + "]");
        }
        this.model = model.build();
        return this;
    }

    /**
     * Sets a MovAvgModel for the Moving Average. The model is used to
     * define what type of moving average you want to use on the series
     *
     * @param model
     *            A MovAvgModel which has been prepopulated with settings
     */
    public MovAvgPipelineAggregatorBuilder model(MovAvgModel model) {
        if (model == null) {
            throw new IllegalArgumentException("[model] must not be null: [" + name + "]");
        }
        this.model = model;
        return this;
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
    public MovAvgPipelineAggregatorBuilder predict(int predict) {
        if (predict <= 0) {
            throw new IllegalArgumentException("predict must be greater than 0. Found [" + predict + "] in [" + name + "]");
        }
        this.predict = predict;
        return this;
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
    public MovAvgPipelineAggregatorBuilder minimize(boolean minimize) {
        this.minimize = minimize;
        return this;
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
    public void doValidate(AggregatorFactory<?> parent, AggregatorFactory<?>[] aggFactories,
            List<PipelineAggregatorBuilder<?>> pipelineAggregatoractories) {
        if (minimize != null && minimize && !model.canBeMinimized()) {
            // If the user asks to minimize, but this model doesn't support
            // it, throw exception
            throw new IllegalStateException("The [" + model + "] model cannot be minimized for aggregation [" + name + "]");
        }
        if (bucketsPaths.length != 1) {
            throw new IllegalStateException(PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                    + " must contain a single entry for aggregation [" + name + "]");
        }
        if (!(parent instanceof AbstractHistogramAggregatorFactory<?>)) {
            throw new IllegalStateException("moving average aggregation [" + name
                    + "] must have a histogram or date_histogram as parent");
        } else {
            AbstractHistogramAggregatorFactory<?> histoParent = (AbstractHistogramAggregatorFactory<?>) parent;
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
    protected MovAvgPipelineAggregatorBuilder doReadFrom(String name, String[] bucketsPaths, StreamInput in) throws IOException {
        MovAvgPipelineAggregatorBuilder factory = new MovAvgPipelineAggregatorBuilder(name, bucketsPaths);
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
        MovAvgPipelineAggregatorBuilder other = (MovAvgPipelineAggregatorBuilder) obj;
        return Objects.equals(format, other.format)
                && Objects.equals(gapPolicy, other.gapPolicy)
                && Objects.equals(window, other.window)
                && Objects.equals(model, other.model)
                && Objects.equals(predict, other.predict)
                && Objects.equals(minimize, other.minimize);
    }

}
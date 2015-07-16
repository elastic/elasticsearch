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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModelBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * A builder to create MovingAvg pipeline aggregations
 */
public class MovAvgBuilder extends PipelineAggregatorBuilder<MovAvgBuilder> {

    private String format;
    private GapPolicy gapPolicy;
    private MovAvgModelBuilder modelBuilder;
    private Integer window;
    private Integer predict;
    private Boolean minimize;
    private Map<String, Object> settings;

    public MovAvgBuilder(String name) {
        super(name, MovAvgPipelineAggregator.TYPE.name());
    }

    public MovAvgBuilder format(String format) {
        this.format = format;
        return this;
    }

    /**
     * Defines what should be done when a gap in the series is discovered
     *
     * @param gapPolicy A GapPolicy enum defining the selected policy
     * @return Returns the builder to continue chaining
     */
    public MovAvgBuilder gapPolicy(GapPolicy gapPolicy) {
        this.gapPolicy = gapPolicy;
        return this;
    }

    /**
     * Sets a MovAvgModelBuilder for the Moving Average.  The model builder is used to
     * define what type of moving average you want to use on the series
     *
     * @param modelBuilder A MovAvgModelBuilder which has been prepopulated with settings
     * @return Returns the builder to continue chaining
     */
    public MovAvgBuilder modelBuilder(MovAvgModelBuilder modelBuilder) {
        this.modelBuilder = modelBuilder;
        return this;
    }

    /**
     * Sets the window size for the moving average.  This window will "slide" across the
     * series, and the values inside that window will be used to calculate the moving avg value
     *
     * @param window Size of window
     * @return Returns the builder to continue chaining
     */
    public MovAvgBuilder window(int window) {
        this.window = window;
        return this;
    }

    /**
     * Sets the number of predictions that should be returned.  Each prediction will be spaced at
     * the intervals specified in the histogram.  E.g "predict: 2" will return two new buckets at the
     * end of the histogram with the predicted values.
     *
     * @param numPredictions Number of predictions to make
     * @return Returns the builder to continue chaining
     */
    public MovAvgBuilder predict(int numPredictions) {
        this.predict = numPredictions;
        return this;
    }

    /**
     * Determines if the model should be fit to the data using a cost
     * minimizing algorithm.
     *
     * @param minimize If the model should be fit to the underlying data
     * @return Returns the builder to continue chaining
     */
    public MovAvgBuilder minimize(boolean minimize) {
        this.minimize = minimize;
        return this;
    }

    /**
     * The hash of settings that should be provided to the model when it is
     * instantiated
     *
     * @param settings
     * @return
     */
    public MovAvgBuilder settings(Map<String, Object> settings) {
        this.settings = settings;
        return this;
    }


    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(MovAvgParser.FORMAT.getPreferredName(), format);
        }
        if (gapPolicy != null) {
            builder.field(MovAvgParser.GAP_POLICY.getPreferredName(), gapPolicy.getName());
        }
        if (modelBuilder != null) {
            modelBuilder.toXContent(builder, params);
        }
        if (window != null) {
            builder.field(MovAvgParser.WINDOW.getPreferredName(), window);
        }
        if (predict != null) {
            builder.field(MovAvgParser.PREDICT.getPreferredName(), predict);
        }
        if (minimize != null) {
            builder.field(MovAvgParser.MINIMIZE.getPreferredName(), minimize);
        }
        if (settings != null) {
            builder.field(MovAvgParser.SETTINGS.getPreferredName(), settings);
        }
        return builder;
    }

}

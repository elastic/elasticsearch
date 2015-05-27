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

package org.elasticsearch.search.aggregations.pipeline.movavg.models;


import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.*;

/**
 * Calculate a triple exponential weighted moving average
 */
public class HoltWintersModel extends MovAvgModel {

    protected static final ParseField NAME_FIELD = new ParseField("holt_winters");

    public enum SeasonalityType {
        ADDITIVE((byte) 0, "add"), MULTIPLICATIVE((byte) 1, "mult");

        /**
         * Parse a string SeasonalityType into the byte enum
         *
         * @param text    SeasonalityType in string format (e.g. "add")
         * @return        SeasonalityType enum
         */
        @Nullable
        public static SeasonalityType parse(String text) {
            if (text == null) {
                return null;
            }
            SeasonalityType result = null;
            for (SeasonalityType policy : values()) {
                if (policy.parseField.match(text)) {
                    if (result == null) {
                        result = policy;
                    } else {
                        throw new IllegalStateException("Text can be parsed to 2 different seasonality types: text=[" + text
                                + "], " + "policies=" + Arrays.asList(result, policy));
                    }
                }
            }
            if (result == null) {
                final List<String> validNames = new ArrayList<>();
                for (SeasonalityType policy : values()) {
                    validNames.add(policy.getName());
                }
                throw new ElasticsearchParseException("Invalid seasonality type: [" + text + "], accepted values: " + validNames);
            }
            return result;
        }

        private final byte id;
        private final ParseField parseField;

        SeasonalityType(byte id, String name) {
            this.id = id;
            this.parseField = new ParseField(name);
        }

        /**
         * Serialize the SeasonalityType to the output stream
         *
         * @param out
         * @throws IOException
         */
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }

        /**
         * Deserialize the SeasonalityType from the input stream
         *
         * @param in
         * @return    SeasonalityType Enum
         * @throws IOException
         */
        public static SeasonalityType readFrom(StreamInput in) throws IOException {
            byte id = in.readByte();
            for (SeasonalityType seasonalityType : values()) {
                if (id == seasonalityType.id) {
                    return seasonalityType;
                }
            }
            throw new IllegalStateException("Unknown Seasonality Type with id [" + id + "]");
        }

        /**
         * Return the english-formatted name of the SeasonalityType
         *
         * @return English representation of SeasonalityType
         */
        public String getName() {
            return parseField.getPreferredName();
        }
    }


    /**
     * Controls smoothing of data. Alpha = 1 retains no memory of past values
     * (e.g. random walk), while alpha = 0 retains infinite memory of past values (e.g.
     * mean of the series).  Useful values are somewhere in between
     */
    private double alpha;

    /**
     * Equivalent to <code>alpha</code>, but controls the smoothing of the trend instead of the data
     */
    private double beta;

    private double gamma;

    private int period;

    private SeasonalityType seasonalityType;

    private boolean pad;
    private double padding;

    public HoltWintersModel(double alpha, double beta, double gamma, int period, SeasonalityType seasonalityType, boolean pad) {
        this.alpha = alpha;
        this.beta = beta;
        this.gamma = gamma;
        this.period = period;
        this.seasonalityType = seasonalityType;
        this.pad = pad;

        // Only pad if we are multiplicative and padding is enabled
        // The padding amount is not currently user-configurable...i dont see a reason to expose it?
        this.padding = seasonalityType.equals(SeasonalityType.MULTIPLICATIVE) && pad ? 0.0000000001 : 0;
    }


    @Override
    public boolean hasValue(int windowLength) {
        // We need at least (period * 2) data-points (e.g. two "seasons")
        return windowLength >= period * 2;
    }

    /**
     * Predicts the next `n` values in the series, using the smoothing model to generate new values.
     * Unlike the other moving averages, HoltWinters has forecasting/prediction built into the algorithm.
     * Prediction is more than simply adding the next prediction to the window and repeating.  HoltWinters
     * will extrapolate into the future by applying the trend and seasonal information to the smoothed data.
     *
     * @param values            Collection of numerics to movingAvg, usually windowed
     * @param numPredictions    Number of newly generated predictions to return
     * @param <T>               Type of numeric
     * @return                  Returns an array of doubles, since most smoothing methods operate on floating points
     */
    @Override
    public <T extends Number> double[] predict(Collection<T> values, int numPredictions) {
        return next(values, numPredictions);
    }

    @Override
    public <T extends Number> double next(Collection<T> values) {
        return next(values, 1)[0];
    }

    /**
     * Calculate a doubly exponential weighted moving average
     *
     * @param values Collection of values to calculate avg for
     * @param numForecasts number of forecasts into the future to return
     *
     * @param <T>    Type T extending Number
     * @return       Returns a Double containing the moving avg for the window
     */
    public <T extends Number> double[] next(Collection<T> values, int numForecasts) {

        if (values.size() < period * 2) {
            // We need at least two full "seasons" to use HW
            // This should have been caught earlier, we can't do anything now...bail
            throw new AggregationExecutionException("Holt-Winters aggregation requires at least (2 * period == 2 * "
                    + period + " == "+(2 * period)+") data-points to function.  Only [" + values.size() + "] were provided.");
        }

        // Smoothed value
        double s = 0;
        double last_s = 0;

        // Trend value
        double b = 0;
        double last_b = 0;

        // Seasonal value
        double[] seasonal = new double[values.size()];

        int counter = 0;
        double[] vs = new double[values.size()];
        for (T v : values) {
            vs[counter] = v.doubleValue() + padding;
            counter += 1;
        }

        // Initial level value is average of first season
        // Calculate the slopes between first and second season for each period
        for (int i = 0; i < period; i++) {
            s += vs[i];
            b += (vs[i] - vs[i + period]) / 2;
        }
        s /= (double) period;
        b /= (double) period;
        last_s = s;
        last_b = b;

        // Calculate first seasonal
        if (Double.compare(s, 0.0) == 0 || Double.compare(s, -0.0) == 0) {
            Arrays.fill(seasonal, 0.0);
        } else {
            for (int i = 0; i < period; i++) {
                seasonal[i] = vs[i] / s;
            }
        }

        for (int i = period; i < vs.length; i++) {
            // TODO if perf is a problem, we can specialize a subclass to avoid conditionals on each iteration
            if (seasonalityType.equals(SeasonalityType.MULTIPLICATIVE)) {
                s = alpha * (vs[i] / seasonal[i - period]) + (1.0d - alpha) * (last_s + last_b);
            } else {
                s = alpha * (vs[i] - seasonal[i - period]) + (1.0d - alpha) * (last_s + last_b);
            }

            b = beta * (s - last_s) + (1 - beta) * last_b;

            if (seasonalityType.equals(SeasonalityType.MULTIPLICATIVE)) {
                seasonal[i] = gamma * (vs[i] / (last_s + last_b )) + (1 - gamma) * seasonal[i - period];
            } else {
                seasonal[i] = gamma * (vs[i] - (last_s + last_b )) + (1 - gamma) * seasonal[i - period];
            }

            last_s = s;
            last_b = b;
        }

        double[] forecastValues = new double[numForecasts];
        int seasonCounter = (values.size() - 1) - period;

        for (int i = 0; i < numForecasts; i++) {

            // TODO perhaps pad out seasonal to a power of 2 and use a mask instead of modulo?
            if (seasonalityType.equals(SeasonalityType.MULTIPLICATIVE)) {
                forecastValues[i] = s + (i * b) * seasonal[seasonCounter % values.size()];
            } else {
                forecastValues[i] = s + (i * b) + seasonal[seasonCounter % values.size()];
            }

            seasonCounter += 1;
        }

        return forecastValues;
    }

    public static final MovAvgModelStreams.Stream STREAM = new MovAvgModelStreams.Stream() {
        @Override
        public MovAvgModel readResult(StreamInput in) throws IOException {
            double alpha = in.readDouble();
            double beta = in.readDouble();
            double gamma = in.readDouble();
            int period = in.readVInt();
            SeasonalityType type = SeasonalityType.readFrom(in);
            boolean pad = in.readBoolean();

            return new HoltWintersModel(alpha, beta, gamma, period, type, pad);
        }

        @Override
        public String getName() {
            return NAME_FIELD.getPreferredName();
        }
    };

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(STREAM.getName());
        out.writeDouble(alpha);
        out.writeDouble(beta);
        out.writeDouble(gamma);
        out.writeVInt(period);
        seasonalityType.writeTo(out);
        out.writeBoolean(pad);
    }

    public static class HoltWintersModelParser extends AbstractModelParser {

        @Override
        public String getName() {
            return NAME_FIELD.getPreferredName();
        }

        @Override
        public MovAvgModel parse(@Nullable Map<String, Object> settings, String pipelineName, SearchContext context, int windowSize) {

            double alpha = parseDoubleParam(context, settings, "alpha", 0.5);
            double beta = parseDoubleParam(context, settings, "beta", 0.5);
            double gamma = parseDoubleParam(context, settings, "gamma", 0.5);
            int period = parseIntegerParam(context, settings, "period", 1);

            if (windowSize < 2 * period) {
                throw new SearchParseException(context, "Field [window] must be at least twice as large as the period when " +
                        "using Holt-Winters.  Value provided was [" + windowSize + "], which is less than (2*period) == "
                        + (2 * period), null);
            }

            SeasonalityType seasonalityType = SeasonalityType.ADDITIVE;

            if (settings != null) {
                Object value = settings.get("type");
                if (value != null) {
                    if (value instanceof String) {
                        seasonalityType = SeasonalityType.parse((String)value);
                    } else {
                        throw new SearchParseException(context, "Parameter [type] must be a String, type `"
                                + value.getClass().getSimpleName() + "` provided instead", null);
                    }
                }
            }

            boolean pad = parseBoolParam(context, settings, "pad", seasonalityType.equals(SeasonalityType.MULTIPLICATIVE));

            return new HoltWintersModel(alpha, beta, gamma, period, seasonalityType, pad);
        }
    }

    public static class HoltWintersModelBuilder implements MovAvgModelBuilder {

        private double alpha = 0.5;
        private double beta = 0.5;
        private double gamma = 0.5;
        private int period = 1;
        private SeasonalityType seasonalityType = SeasonalityType.ADDITIVE;
        private boolean pad = true;

        /**
         * Alpha controls the smoothing of the data.  Alpha = 1 retains no memory of past values
         * (e.g. a random walk), while alpha = 0 retains infinite memory of past values (e.g.
         * the series mean).  Useful values are somewhere in between.  Defaults to 0.5.
         *
         * @param alpha A double between 0-1 inclusive, controls data smoothing
         *
         * @return The builder to continue chaining
         */
        public HoltWintersModelBuilder alpha(double alpha) {
            this.alpha = alpha;
            return this;
        }

        /**
         * Equivalent to <code>alpha</code>, but controls the smoothing of the trend instead of the data
         *
         * @param beta a double between 0-1 inclusive, controls trend smoothing
         *
         * @return The builder to continue chaining
         */
        public HoltWintersModelBuilder beta(double beta) {
            this.beta = beta;
            return this;
        }

        public HoltWintersModelBuilder gamma(double gamma) {
            this.gamma = gamma;
            return this;
        }

        public HoltWintersModelBuilder period(int period) {
            this.period = period;
            return this;
        }

        public HoltWintersModelBuilder seasonalityType(SeasonalityType type) {
            this.seasonalityType = type;
            return this;
        }

        public HoltWintersModelBuilder pad(boolean pad) {
            this.pad = pad;
            return this;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(MovAvgParser.MODEL.getPreferredName(), NAME_FIELD.getPreferredName());
            builder.startObject(MovAvgParser.SETTINGS.getPreferredName());
            builder.field("alpha", alpha);
            builder.field("beta", beta);
            builder.field("gamma", gamma);
            builder.field("period", period);
            builder.field("type", seasonalityType.getName());
            builder.field("pad", pad);
            builder.endObject();
            return builder;
        }
    }
}


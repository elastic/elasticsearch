/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Calculate a triple exponential weighted moving average
 */
public class HoltWintersModel extends MovAvgModel {
    public static final String NAME = "holt_winters";

    private static final double DEFAULT_ALPHA = 0.3;
    private static final double DEFAULT_BETA = 0.1;
    private static final double DEFAULT_GAMMA = 0.3;
    private static final int DEFAULT_PERIOD = 1;
    private static final SeasonalityType DEFAULT_SEASONALITY_TYPE = SeasonalityType.ADDITIVE;
    private static final boolean DEFAULT_PAD = false;

    public enum SeasonalityType {
        ADDITIVE((byte) 0, "add"),
        MULTIPLICATIVE((byte) 1, "mult");

        /**
         * Parse a string SeasonalityType into the byte enum
         *
         * @param text                SeasonalityType in string format (e.g. "add")
         * @return                    SeasonalityType enum
         */
        @Nullable
        public static SeasonalityType parse(String text) {
            if (text == null) {
                return null;
            }
            SeasonalityType result = null;
            for (SeasonalityType policy : values()) {
                if (policy.parseField.match(text, LoggingDeprecationHandler.INSTANCE)) {
                    result = policy;
                    break;
                }
            }
            if (result == null) {
                final List<String> validNames = new ArrayList<>();
                for (SeasonalityType policy : values()) {
                    validNames.add(policy.getName());
                }
                throw new ElasticsearchParseException("failed to parse seasonality type [{}]. accepted values are [{}]", text, validNames);
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
         */
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }

        /**
         * Deserialize the SeasonalityType from the input stream
         *
         * @param in  the input stream
         * @return    SeasonalityType Enum
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
     * Controls smoothing of data.  Also known as "level" value.
     * Alpha = 1 retains no memory of past values
     * (e.g. random walk), while alpha = 0 retains infinite memory of past values (e.g.
     * mean of the series).
     */
    private final double alpha;

    /**
     * Controls smoothing of trend.
     * Beta = 1 retains no memory of past values
     * (e.g. random walk), while alpha = 0 retains infinite memory of past values (e.g.
     * mean of the series).
     */
    private final double beta;

    /**
     * Controls smoothing of seasonality.
     * Gamma = 1 retains no memory of past values
     * (e.g. random walk), while alpha = 0 retains infinite memory of past values (e.g.
     * mean of the series).
     */
    private final double gamma;

    /**
     * Periodicity of the data
     */
    private final int period;

    /**
     * Whether this is a multiplicative or additive HW
     */
    private final SeasonalityType seasonalityType;

    /**
     * Padding is used to add a very small amount to values, so that zeroes do not interfere
     * with multiplicative seasonality math (e.g. division by zero)
     */
    private final boolean pad;
    private final double padding;

    public HoltWintersModel() {
        this(DEFAULT_ALPHA, DEFAULT_BETA, DEFAULT_GAMMA, DEFAULT_PERIOD, DEFAULT_SEASONALITY_TYPE, DEFAULT_PAD);
    }

    public HoltWintersModel(double alpha, double beta, double gamma, int period, SeasonalityType seasonalityType, boolean pad) {
        this.alpha = alpha;
        this.beta = beta;
        this.gamma = gamma;
        this.period = period;
        this.seasonalityType = seasonalityType;
        this.pad = pad;
        this.padding = inferPadding();
    }

    /**
     * Read from a stream.
     */
    public HoltWintersModel(StreamInput in) throws IOException {
        alpha = in.readDouble();
        beta = in.readDouble();
        gamma = in.readDouble();
        period = in.readVInt();
        seasonalityType = SeasonalityType.readFrom(in);
        pad = in.readBoolean();
        this.padding = inferPadding();
    }

    /**
     * Only pad if we are multiplicative and padding is enabled. the padding amount is not currently user-configurable.
     */
    private double inferPadding() {
        return seasonalityType.equals(SeasonalityType.MULTIPLICATIVE) && pad ? 0.0000000001 : 0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(alpha);
        out.writeDouble(beta);
        out.writeDouble(gamma);
        out.writeVInt(period);
        seasonalityType.writeTo(out);
        out.writeBoolean(pad);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean minimizeByDefault() {
        return true;
    }

    @Override
    public boolean canBeMinimized() {
        return true;
    }

    @Override
    public MovAvgModel neighboringModel() {
        double newValue = Math.random();
        switch ((int) (Math.random() * 3)) {
            case 0:
                return new HoltWintersModel(newValue, beta, gamma, period, seasonalityType, pad);
            case 1:
                return new HoltWintersModel(alpha, newValue, gamma, period, seasonalityType, pad);
            case 2:
                return new HoltWintersModel(alpha, beta, newValue, period, seasonalityType, pad);
            default:
                assert (false) : "Random value fell outside of range [0-2]";
                return new HoltWintersModel(newValue, beta, gamma, period, seasonalityType, pad); // This should never technically happen...
        }
    }

    @Override
    public MovAvgModel clone() {
        return new HoltWintersModel(alpha, beta, gamma, period, seasonalityType, pad);
    }

    @Override
    public boolean hasValue(int valuesAvailable) {
        // We need at least (period * 2) data-points (e.g. two "seasons")
        return valuesAvailable >= period * 2;
    }

    /**
     * Predicts the next `n` values in the series, using the smoothing model to generate new values.
     * Unlike the other moving averages, HoltWinters has forecasting/prediction built into the algorithm.
     * Prediction is more than simply adding the next prediction to the window and repeating.  HoltWinters
     * will extrapolate into the future by applying the trend and seasonal information to the smoothed data.
     *
     * @param values            Collection of numerics to movingAvg, usually windowed
     * @param numPredictions    Number of newly generated predictions to return
     * @return                  Returns an array of doubles, since most smoothing methods operate on floating points
     */
    @Override
    protected double[] doPredict(Collection<Double> values, int numPredictions) {
        return next(values, numPredictions);
    }

    @Override
    public double next(Collection<Double> values) {
        return next(values, 1)[0];
    }

    /**
     * Calculate a doubly exponential weighted moving average
     *
     * @param values Collection of values to calculate avg for
     * @param numForecasts number of forecasts into the future to return
     *
     * @return       Returns a Double containing the moving avg for the window
     */
    public double[] next(Collection<Double> values, int numForecasts) {
        return MovingFunctions.holtWintersForecast(
            values.stream().mapToDouble(Double::doubleValue).toArray(),
            alpha,
            beta,
            gamma,
            period,
            padding,
            seasonalityType.equals(SeasonalityType.MULTIPLICATIVE),
            numForecasts
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(MovAvgPipelineAggregationBuilder.MODEL.getPreferredName(), NAME);
        builder.startObject(MovAvgPipelineAggregationBuilder.SETTINGS.getPreferredName());
        builder.field("alpha", alpha);
        builder.field("beta", beta);
        builder.field("gamma", gamma);
        builder.field("period", period);
        builder.field("pad", pad);
        builder.field("type", seasonalityType.getName());
        builder.endObject();
        return builder;
    }

    public static final AbstractModelParser PARSER = new AbstractModelParser() {
        @Override
        public MovAvgModel parse(@Nullable Map<String, Object> settings, String pipelineName, int windowSize) throws ParseException {

            double alpha = parseDoubleParam(settings, "alpha", DEFAULT_ALPHA);
            double beta = parseDoubleParam(settings, "beta", DEFAULT_BETA);
            double gamma = parseDoubleParam(settings, "gamma", DEFAULT_GAMMA);
            int period = parseIntegerParam(settings, "period", DEFAULT_PERIOD);

            SeasonalityType seasonalityType = DEFAULT_SEASONALITY_TYPE;

            if (settings != null) {
                Object value = settings.get("type");
                if (value != null) {
                    if (value instanceof String) {
                        seasonalityType = SeasonalityType.parse((String) value);
                        settings.remove("type");
                    } else {
                        throw new ParseException(
                            "Parameter [type] must be a String, type `" + value.getClass().getSimpleName() + "` provided instead",
                            0
                        );
                    }
                }
            }

            boolean pad = parseBoolParam(settings, "pad", seasonalityType.equals(SeasonalityType.MULTIPLICATIVE));

            checkUnrecognizedParams(settings);
            return new HoltWintersModel(alpha, beta, gamma, period, seasonalityType, pad);
        }
    };

    /**
     * If the model is a HoltWinters, we need to ensure the window and period are compatible.
     * This is verified in the XContent parsing, but transport clients need these checks since they
     * skirt XContent parsing
     */
    @Override
    protected void validate(long window, String aggregationName) {
        super.validate(window, aggregationName);
        if (window < 2 * period) {
            throw new IllegalArgumentException(
                "Field [window] must be at least twice as large as the period when "
                    + "using Holt-Winters.  Value provided was ["
                    + window
                    + "], which is less than (2*period) == "
                    + (2 * period)
            );
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(alpha, beta, gamma, period, seasonalityType, pad);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        HoltWintersModel other = (HoltWintersModel) obj;
        return Objects.equals(alpha, other.alpha)
            && Objects.equals(beta, other.beta)
            && Objects.equals(gamma, other.gamma)
            && Objects.equals(period, other.period)
            && Objects.equals(seasonalityType, other.seasonalityType)
            && Objects.equals(pad, other.pad);
    }

    public static class HoltWintersModelBuilder implements MovAvgModelBuilder {

        private double alpha = DEFAULT_ALPHA;
        private double beta = DEFAULT_BETA;
        private double gamma = DEFAULT_GAMMA;
        private int period = DEFAULT_PERIOD;
        private SeasonalityType seasonalityType = DEFAULT_SEASONALITY_TYPE;
        private Boolean pad = null;

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
            builder.field(MovAvgPipelineAggregationBuilder.MODEL.getPreferredName(), NAME);
            builder.startObject(MovAvgPipelineAggregationBuilder.SETTINGS.getPreferredName());
            builder.field("alpha", alpha);
            builder.field("beta", beta);
            builder.field("gamma", gamma);
            builder.field("period", period);
            if (pad != null) {
                builder.field("pad", pad);
            }
            builder.field("type", seasonalityType.getName());

            builder.endObject();
            return builder;
        }

        @Override
        public MovAvgModel build() {
            boolean pad = this.pad == null ? (seasonalityType == SeasonalityType.MULTIPLICATIVE) : this.pad;
            return new HoltWintersModel(alpha, beta, gamma, period, seasonalityType, pad);
        }
    }
}

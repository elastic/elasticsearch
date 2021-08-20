/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ParseFieldRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.BUCKETS_PATH;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.FORMAT;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.GAP_POLICY;

public class MovAvgPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<MovAvgPipelineAggregationBuilder> {
    public static final String NAME = "moving_avg";

    static final ParseField MODEL = new ParseField("model");
    private static final ParseField WINDOW = new ParseField("window");
    public static final ParseField SETTINGS = new ParseField("settings");
    private static final ParseField PREDICT = new ParseField("predict");
    private static final ParseField MINIMIZE = new ParseField("minimize");
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(MovAvgPipelineAggregationBuilder.class);

    private String format;
    private GapPolicy gapPolicy = GapPolicy.SKIP;
    private int window = 5;
    private MovAvgModel model = new SimpleModel();
    private int predict = 0;
    private Boolean minimize;

    public MovAvgPipelineAggregationBuilder(String name, String bucketsPath) {
        super(name, NAME, new String[] { bucketsPath });
    }

    /**
     * Read from a stream.
     */
    public MovAvgPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        format = in.readOptionalString();
        gapPolicy = GapPolicy.readFrom(in);
        window = in.readVInt();
        model = in.readNamedWriteable(MovAvgModel.class);
        predict = in.readVInt();
        minimize = in.readOptionalBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(format);
        gapPolicy.writeTo(out);
        out.writeVInt(window);
        out.writeNamedWriteable(model);
        out.writeVInt(predict);
        out.writeOptionalBoolean(minimize);
    }

    /**
     * Sets the format to use on the output of this aggregation.
     */
    public MovAvgPipelineAggregationBuilder format(String format) {
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
    public MovAvgPipelineAggregationBuilder gapPolicy(GapPolicy gapPolicy) {
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
    public MovAvgPipelineAggregationBuilder window(int window) {
        if (window <= 0) {
            throw new IllegalArgumentException("[window] must be a positive integer: [" + name + "]");
        }
        // If we have a model we can validate the window now
        if (model != null) {
            model.validate(window, name);
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
    public MovAvgPipelineAggregationBuilder modelBuilder(MovAvgModelBuilder model) {
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
    public MovAvgPipelineAggregationBuilder model(MovAvgModel model) {
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
    public MovAvgPipelineAggregationBuilder predict(int predict) {
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
    public MovAvgPipelineAggregationBuilder minimize(boolean minimize) {
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
    protected PipelineAggregator createInternal(Map<String, Object> metadata) {
        // If the user doesn't set a preference for cost minimization, ask
        // what the model prefers
        boolean minimize = this.minimize == null ? model.minimizeByDefault() : this.minimize;
        return new MovAvgPipelineAggregator(name, bucketsPaths, formatter(), gapPolicy, window, predict, model, minimize, metadata);
    }

    @Override
    protected void validate(ValidationContext context) {
        if (minimize != null && minimize && model.canBeMinimized() == false) {
            // If the user asks to minimize, but this model doesn't support
            // it, throw exception
            context.addValidationError("The [" + model + "] model cannot be minimized for aggregation [" + name + "]");
        }
        if (bucketsPaths.length != 1) {
            context.addBucketPathValidationError("must contain a single entry for aggregation [" + name + "]");
            return;
        }
        // Validate any model-specific window requirements
        model.validate(window, name);
        context.validateParentAggSequentiallyOrdered(NAME, name);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(FORMAT.getPreferredName(), format);
        }
        builder.field(GAP_POLICY.getPreferredName(), gapPolicy.getName());
        model.toXContent(builder, params);
        builder.field(WINDOW.getPreferredName(), window);
        if (predict > 0) {
            builder.field(PREDICT.getPreferredName(), predict);
        }
        if (minimize != null) {
            builder.field(MINIMIZE.getPreferredName(), minimize);
        }
        return builder;
    }

    public static MovAvgPipelineAggregationBuilder parse(
        ParseFieldRegistry<MovAvgModel.AbstractModelParser> movingAverageMdelParserRegistry,
        String pipelineAggregatorName,
        XContentParser parser
    ) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        String[] bucketsPaths = null;
        String format = null;

        GapPolicy gapPolicy = null;
        Integer window = null;
        Map<String, Object> settings = null;
        String model = null;
        Integer predict = null;
        Boolean minimize = null;

        DEPRECATION_LOGGER.deprecate(
            DeprecationCategory.AGGREGATIONS,
            "moving_avg_aggregation",
            "The moving_avg aggregation has been deprecated in favor of the moving_fn aggregation."
        );

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (WINDOW.match(currentFieldName, parser.getDeprecationHandler())) {
                    window = parser.intValue();
                    if (window <= 0) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "["
                                + currentFieldName
                                + "] value must be a positive, "
                                + "non-zero integer.  Value supplied was ["
                                + predict
                                + "] in ["
                                + pipelineAggregatorName
                                + "]."
                        );
                    }
                } else if (PREDICT.match(currentFieldName, parser.getDeprecationHandler())) {
                    predict = parser.intValue();
                    if (predict <= 0) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "["
                                + currentFieldName
                                + "] value must be a positive integer."
                                + "  Value supplied was ["
                                + predict
                                + "] in ["
                                + pipelineAggregatorName
                                + "]."
                        );
                    }
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + pipelineAggregatorName + "]: [" + currentFieldName + "]."
                    );
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (FORMAT.match(currentFieldName, parser.getDeprecationHandler())) {
                    format = parser.text();
                } else if (BUCKETS_PATH.match(currentFieldName, parser.getDeprecationHandler())) {
                    bucketsPaths = new String[] { parser.text() };
                } else if (GAP_POLICY.match(currentFieldName, parser.getDeprecationHandler())) {
                    gapPolicy = GapPolicy.parse(parser.text(), parser.getTokenLocation());
                } else if (MODEL.match(currentFieldName, parser.getDeprecationHandler())) {
                    model = parser.text();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + pipelineAggregatorName + "]: [" + currentFieldName + "]."
                    );
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (BUCKETS_PATH.match(currentFieldName, parser.getDeprecationHandler())) {
                    List<String> paths = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String path = parser.text();
                        paths.add(path);
                    }
                    bucketsPaths = paths.toArray(new String[paths.size()]);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + pipelineAggregatorName + "]: [" + currentFieldName + "]."
                    );
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SETTINGS.match(currentFieldName, parser.getDeprecationHandler())) {
                    settings = parser.map();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + pipelineAggregatorName + "]: [" + currentFieldName + "]."
                    );
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if (MINIMIZE.match(currentFieldName, parser.getDeprecationHandler())) {
                    minimize = parser.booleanValue();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + pipelineAggregatorName + "]: [" + currentFieldName + "]."
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Unexpected token " + token + " in [" + pipelineAggregatorName + "]."
                );
            }
        }

        if (bucketsPaths == null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Missing required field ["
                    + BUCKETS_PATH.getPreferredName()
                    + "] for movingAvg aggregation ["
                    + pipelineAggregatorName
                    + "]"
            );
        }

        MovAvgPipelineAggregationBuilder factory = new MovAvgPipelineAggregationBuilder(pipelineAggregatorName, bucketsPaths[0]);
        if (format != null) {
            factory.format(format);
        }
        if (gapPolicy != null) {
            factory.gapPolicy(gapPolicy);
        }
        if (window != null) {
            factory.window(window);
        }
        if (predict != null) {
            factory.predict(predict);
        }
        if (model != null) {
            MovAvgModel.AbstractModelParser modelParser = movingAverageMdelParserRegistry.lookup(
                model,
                parser.getTokenLocation(),
                parser.getDeprecationHandler()
            );
            MovAvgModel movAvgModel;
            try {
                movAvgModel = modelParser.parse(settings, pipelineAggregatorName, factory.window());
            } catch (ParseException exception) {
                throw new ParsingException(parser.getTokenLocation(), "Could not parse settings for model [" + model + "].", exception);
            }
            factory.model(movAvgModel);
        }
        if (minimize != null) {
            factory.minimize(minimize);
        }
        return factory;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), format, gapPolicy, window, model, predict, minimize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        MovAvgPipelineAggregationBuilder other = (MovAvgPipelineAggregationBuilder) obj;
        return Objects.equals(format, other.format)
            && Objects.equals(gapPolicy, other.gapPolicy)
            && Objects.equals(window, other.window)
            && Objects.equals(model, other.model)
            && Objects.equals(predict, other.predict)
            && Objects.equals(minimize, other.minimize);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}

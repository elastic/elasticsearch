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

package org.elasticsearch.search.aggregations.pipeline.moving;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ParseFieldRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.moving.models.MovModelBuilder;
import org.elasticsearch.search.aggregations.pipeline.moving.models.MovModel;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.BUCKETS_PATH;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.FORMAT;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.GAP_POLICY;

public class MovFunctionPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<MovFunctionPipelineAggregationBuilder> {
    public static final String NAME = "moving_fn";

    private String format;
    private GapPolicy gapPolicy = GapPolicy.SKIP;
    private int window = 5;
    private MovModel function = null;
    private Script script = null;

    public MovFunctionPipelineAggregationBuilder(String name, String bucketsPath, Script script) {
        super(name, NAME, new String[] { bucketsPath });
        this.script = script;
    }

    /**
     * Read from a stream.
     */
    public MovFunctionPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        format = in.readOptionalString();
        gapPolicy = GapPolicy.readFrom(in);
        window = in.readVInt();
        function = in.readOptionalNamedWriteable(MovModel.class);
        script = in.readOptionalWriteable(Script::new);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(format);
        gapPolicy.writeTo(out);
        out.writeVInt(window);
        out.writeOptionalNamedWriteable(function);
        out.writeOptionalWriteable(script);
    }

    /**
     * Sets the format to use on the output of this aggregation.
     */
    public MovFunctionPipelineAggregationBuilder format(String format) {
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
    public MovFunctionPipelineAggregationBuilder gapPolicy(GapPolicy gapPolicy) {
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
     * Sets the window size for the moving fn. This window will "slide"
     * across the series, and the values inside that window will be used to
     * calculate the moving function value
     *
     * @param window
     *            Size of window
     */
    public MovFunctionPipelineAggregationBuilder window(int window) {
        if (window <= 0) {
            throw new IllegalArgumentException("[window] must be a positive integer: [" + name + "]");
        }
        this.window = window;
        return this;
    }

    /**
     * Gets the window size for the moving function. This window will "slide"
     * across the series, and the values inside that window will be used to
     * calculate the moving function value
     */
    public int window() {
        return window;
    }

    /**
     * Sets a MovModel for the Moving Function. The function is used to
     * define what type of moving fn you want to use on the series
     *
     * @param function
     *            A MovModel which has been prepopulated with settings
     */
    public MovFunctionPipelineAggregationBuilder modelBuilder(MovModelBuilder function) {
        if (function == null) {
            throw new IllegalArgumentException("[function] must not be null: [" + name + "]");
        }
        this.function = function.build();
        return this;
    }

    /**
     * Sets a MovModel for the moving function. The function is used to
     * define what type of moving fn you want to use on the series
     *
     * @param function
     *            A MovModel which has been prepopulated with settings
     */
    public MovFunctionPipelineAggregationBuilder function(MovModel function) {
        if (function == null) {
            throw new IllegalArgumentException("[function] must not be null: [" + name + "]");
        }
        this.function = function;
        return this;
    }

    /**
     * Gets a MovModel for the moving function. The function is used to
     * define what type of moving fn you want to use on the series
     */
    public MovModel function() {
        return function;
    }

    public Script getScript() {
        return script;
    }

    public MovFunctionPipelineAggregationBuilder setScript(Script script) {
        this.script = script;
        return this;
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException {
        return new MovFunctionPipelineAggregator(name, bucketsPaths, formatter(), gapPolicy, window, function,
            script, metaData);
    }

    @Override
    public void doValidate(AggregatorFactory<?> parent, AggregatorFactory<?>[] aggFactories,
                           List<PipelineAggregationBuilder> pipelineAggregatoractories) {
        if (bucketsPaths.length != 1) {
            throw new IllegalStateException(PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                + " must contain a single entry for aggregation [" + name + "]");
        }
        if (parent instanceof HistogramAggregatorFactory) {
            HistogramAggregatorFactory histoParent = (HistogramAggregatorFactory) parent;
            if (histoParent.minDocCount() != 0) {
                throw new IllegalStateException("parent histogram of moving function aggregation [" + name
                    + "] must have min_doc_count of 0");
            }
        } else if (parent instanceof DateHistogramAggregatorFactory) {
            DateHistogramAggregatorFactory histoParent = (DateHistogramAggregatorFactory) parent;
            if (histoParent.minDocCount() != 0) {
                throw new IllegalStateException("parent histogram of moving function aggregation [" + name
                    + "] must have min_doc_count of 0");
            }
        } else {
            throw new IllegalStateException("moving function aggregation [" + name
                + "] must have a histogram or date_histogram as parent");
        }
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(FORMAT.getPreferredName(), format);
        }
        builder.field(GAP_POLICY.getPreferredName(), gapPolicy.getName());
        if (function != null) {
            function.toXContent(builder, params);
        }
        if (script != null) {
            builder.field(Script.SCRIPT_PARSE_FIELD.getPreferredName());
            script.toXContent(builder, params);
        }
        builder.field(MovModel.WINDOW.getPreferredName(), window);
        return builder;
    }

    public static MovFunctionPipelineAggregationBuilder parse(
        ParseFieldRegistry<MovModel.AbstractModelParser> movingFunctionModelParserRegistry,
        String pipelineAggregatorName, QueryParseContext context) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token;
        String currentFieldName = null;
        String[] bucketsPaths = null;
        String format = null;

        GapPolicy gapPolicy = null;
        Integer window = null;
        Map<String, Object> settings = null;
        String model = null;
        Script script = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (MovModel.WINDOW.match(currentFieldName)) {
                    window = parser.intValue();
                    if (window <= 0) {
                        throw new ParsingException(parser.getTokenLocation(), "[" + currentFieldName + "] value must be a positive, "
                            + "non-zero integer.  Value supplied was [" + window + "] in [" + pipelineAggregatorName + "].");
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + pipelineAggregatorName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (FORMAT.match(currentFieldName)) {
                    format = parser.text();
                } else if (BUCKETS_PATH.match(currentFieldName)) {
                    bucketsPaths = new String[] { parser.text() };
                } else if (GAP_POLICY.match(currentFieldName)) {
                    gapPolicy = GapPolicy.parse(context, parser.text(), parser.getTokenLocation());
                } else if (MovModel.FUNCTION.match(currentFieldName)) {
                    if (script != null) {
                        throw new ParsingException(parser.getTokenLocation(),
                            "A function and script cannot both be defined for MovingFunction aggregation.");
                    }
                    model = parser.text();
                } else if (Script.SCRIPT_PARSE_FIELD.match(currentFieldName)) {
                    if (model != null) {
                        throw new ParsingException(parser.getTokenLocation(),
                            "A function and script cannot both be defined for MovingFunction aggregation.");
                    }
                    script = Script.parse(parser);
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + pipelineAggregatorName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (BUCKETS_PATH.match(currentFieldName)) {
                    List<String> paths = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String path = parser.text();
                        paths.add(path);
                    }
                    bucketsPaths = paths.toArray(new String[paths.size()]);
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + pipelineAggregatorName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (MovModel.SETTINGS.match(currentFieldName)) {
                    settings = parser.map();
                } else if (Script.SCRIPT_PARSE_FIELD.match(currentFieldName)) {
                    if (model != null) {
                        throw new ParsingException(parser.getTokenLocation(),
                            "A function and script cannot both be defined for MovingFunction aggregation.");
                    }
                    script = Script.parse(parser);
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + pipelineAggregatorName + "]: [" + currentFieldName + "].");
                }
            }  else {
                throw new ParsingException(parser.getTokenLocation(),
                    "Unexpected token " + token + " in [" + pipelineAggregatorName + "].");
            }
        }

        if (bucketsPaths == null) {
            throw new ParsingException(parser.getTokenLocation(), "Missing required field [" + BUCKETS_PATH.getPreferredName()
                + "] for moving_function aggregation [" + pipelineAggregatorName + "]");
        }

        MovFunctionPipelineAggregationBuilder factory =
            new MovFunctionPipelineAggregationBuilder(pipelineAggregatorName, bucketsPaths[0], script);
        if (format != null) {
            factory.format(format);
        }
        if (gapPolicy != null) {
            factory.gapPolicy(gapPolicy);
        }
        if (window != null) {
            factory.window(window);
        }
        if (model == null && script == null) {
            throw new RuntimeException("A function or script must be defined for the MovingFunction aggregation.");
        }
        if (model != null) {
            MovModel.AbstractModelParser modelParser = movingFunctionModelParserRegistry.lookup(model, parser.getTokenLocation());
            MovModel movModel;
            try {
                movModel = modelParser.parse(settings, pipelineAggregatorName, factory.window());
            } catch (ParseException exception) {
                throw new ParsingException(parser.getTokenLocation(), "Could not parse settings for function [" + model + "].", exception);
            }
            factory.function(movModel);
        }

        return factory;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(format, gapPolicy, window, function);
    }

    @Override
    protected boolean doEquals(Object obj) {
        MovFunctionPipelineAggregationBuilder other = (MovFunctionPipelineAggregationBuilder) obj;
        return Objects.equals(format, other.format)
            && Objects.equals(gapPolicy, other.gapPolicy)
            && Objects.equals(window, other.window)
            && Objects.equals(function, other.function)
            && Objects.equals(script, other.script);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}

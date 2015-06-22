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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModel;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.MovAvgModelParserMapper;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MovAvgParser implements PipelineAggregator.Parser {

    public static final ParseField MODEL = new ParseField("model");
    public static final ParseField WINDOW = new ParseField("window");
    public static final ParseField SETTINGS = new ParseField("settings");
    public static final ParseField PREDICT = new ParseField("predict");
    public static final ParseField MINIMIZE = new ParseField("minimize");

    private final MovAvgModelParserMapper movAvgModelParserMapper;

    @Inject
    public MovAvgParser(MovAvgModelParserMapper movAvgModelParserMapper) {
        this.movAvgModelParserMapper = movAvgModelParserMapper;
    }

    @Override
    public String type() {
        return MovAvgPipelineAggregator.TYPE.name();
    }

    @Override
    public PipelineAggregatorFactory parse(String pipelineAggregatorName, XContentParser parser, SearchContext context) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        String[] bucketsPaths = null;
        String format = null;

        GapPolicy gapPolicy = GapPolicy.SKIP;
        int window = 5;
        Map<String, Object> settings = null;
        String model = "simple";
        int predict = 0;
        Boolean minimize = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (context.parseFieldMatcher().match(currentFieldName, WINDOW)) {
                    window = parser.intValue();
                    if (window <= 0) {
                        throw new SearchParseException(context, "[" + currentFieldName + "] value must be a positive, "
                                + "non-zero integer.  Value supplied was [" + predict + "] in [" + pipelineAggregatorName + "].",
                                parser.getTokenLocation());
                    }
                } else if (context.parseFieldMatcher().match(currentFieldName, PREDICT)) {
                    predict = parser.intValue();
                    if (predict <= 0) {
                        throw new SearchParseException(context, "[" + currentFieldName + "] value must be a positive, "
                                + "non-zero integer.  Value supplied was [" + predict + "] in [" + pipelineAggregatorName + "].",
                                parser.getTokenLocation());
                    }
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + pipelineAggregatorName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (context.parseFieldMatcher().match(currentFieldName, FORMAT)) {
                    format = parser.text();
                } else if (context.parseFieldMatcher().match(currentFieldName, BUCKETS_PATH)) {
                    bucketsPaths = new String[] { parser.text() };
                } else if (context.parseFieldMatcher().match(currentFieldName, GAP_POLICY)) {
                    gapPolicy = GapPolicy.parse(context, parser.text(), parser.getTokenLocation());
                } else if (context.parseFieldMatcher().match(currentFieldName, MODEL)) {
                    model = parser.text();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + pipelineAggregatorName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (context.parseFieldMatcher().match(currentFieldName, BUCKETS_PATH)) {
                    List<String> paths = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String path = parser.text();
                        paths.add(path);
                    }
                    bucketsPaths = paths.toArray(new String[paths.size()]);
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + pipelineAggregatorName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (context.parseFieldMatcher().match(currentFieldName, SETTINGS)) {
                    settings = parser.map();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + pipelineAggregatorName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if (context.parseFieldMatcher().match(currentFieldName, MINIMIZE)) {
                    minimize = parser.booleanValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + pipelineAggregatorName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + pipelineAggregatorName + "].",
                        parser.getTokenLocation());
            }
        }

        if (bucketsPaths == null) {
            throw new SearchParseException(context, "Missing required field [" + BUCKETS_PATH.getPreferredName()
                    + "] for movingAvg aggregation [" + pipelineAggregatorName + "]", parser.getTokenLocation());
        }

        ValueFormatter formatter = null;
        if (format != null) {
            formatter = ValueFormat.Patternable.Number.format(format).formatter();
        } else {
            formatter = ValueFormatter.RAW;
        }

        MovAvgModel.AbstractModelParser modelParser = movAvgModelParserMapper.get(model);
        if (modelParser == null) {
            throw new SearchParseException(context, "Unknown model [" + model + "] specified.  Valid options are:"
                    + movAvgModelParserMapper.getAllNames().toString(), parser.getTokenLocation());
        }

        MovAvgModel movAvgModel;
        try {
            movAvgModel = modelParser.parse(settings, pipelineAggregatorName, window, context.parseFieldMatcher());
        } catch (ParseException exception) {
            throw new SearchParseException(context, "Could not parse settings for model [" + model + "].", null, exception);
        }

        // If the user doesn't set a preference for cost minimization, ask what the model prefers
        if (minimize == null) {
            minimize = movAvgModel.minimizeByDefault();
        } else if (minimize && !movAvgModel.canBeMinimized()) {
            // If the user asks to minimize, but this model doesn't support it, throw exception
            throw new SearchParseException(context, "The [" + model + "] model cannot be minimized.", null);
        }


        return new MovAvgPipelineAggregator.Factory(pipelineAggregatorName, bucketsPaths, formatter, gapPolicy, window, predict,
                movAvgModel, minimize);
    }


}

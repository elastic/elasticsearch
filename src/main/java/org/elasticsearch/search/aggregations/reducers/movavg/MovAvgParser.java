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

package org.elasticsearch.search.aggregations.reducers.movavg;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.reducers.Reducer;
import org.elasticsearch.search.aggregations.reducers.ReducerFactory;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.reducers.BucketHelpers.GapPolicy;
import static org.elasticsearch.search.aggregations.reducers.movavg.MovAvgModel.Weighting;

public class MovAvgParser implements Reducer.Parser {

    public static final ParseField FORMAT = new ParseField("format");
    public static final ParseField GAP_POLICY = new ParseField("gap_policy");
    public static final ParseField WEIGHTING = new ParseField("weighting");
    public static final ParseField WINDOW = new ParseField("window");
    public static final ParseField SETTINGS = new ParseField("settings");

    @Override
    public String type() {
        return MovAvgReducer.TYPE.name();
    }

    @Override
    public ReducerFactory parse(String reducerName, XContentParser parser, SearchContext context) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        String[] bucketsPaths = null;
        String format = null;
        GapPolicy gapPolicy = GapPolicy.IGNORE;
        Weighting weighting = Weighting.SIMPLE;
        int window = 5;
        Map<String, Object> settings = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (WINDOW.match(currentFieldName)) {
                    window = parser.intValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: ["
                            + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (FORMAT.match(currentFieldName)) {
                    format = parser.text();
                } else if (BUCKETS_PATH.match(currentFieldName)) {
                    bucketsPaths = new String[] { parser.text() };
                } else if (GAP_POLICY.match(currentFieldName)) {
                    gapPolicy = GapPolicy.parse(context, parser.text());
                } else if (WEIGHTING.match(currentFieldName)) {
                    weighting = Weighting.parse(context, parser.text());
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: ["
                            + currentFieldName + "].");
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
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: ["
                            + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SETTINGS.match(currentFieldName)) {
                    settings = parser.map();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: ["
                            + currentFieldName + "].");
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + reducerName + "].");
            }
        }

        if (bucketsPaths == null) {
            throw new SearchParseException(context, "Missing required field [" + BUCKETS_PATH.getPreferredName()
                    + "] for movavg aggregation [" + reducerName + "]");
        }

        ValueFormatter formatter = null;
        if (format != null) {
            formatter = ValueFormat.Patternable.Number.format(format).formatter();
        }

        settings = getDefaultSettings(weighting, settings);

        return new MovAvgReducer.Factory(reducerName, bucketsPaths, formatter, gapPolicy, weighting, window, settings);
    }

    /**
     * Provide some default values depending on the weighting type being used
     *
     * @param weightingType     Weighting being used for the agg (simple, linear, etc)
     * @param settings          Map of settings provided by the user. May be null if no settings
     * @return                  Return a map of settings if valid, otherwise null
     */
    private @Nullable Map<String, Object> getDefaultSettings(Weighting weightingType, @Nullable Map<String, Object> settings) {
        // simple and linear have no settings
        if (weightingType.equals(MovAvgModel.Weighting.SIMPLE) || weightingType.equals(MovAvgModel.Weighting.LINEAR)) {
            return null;
        }

        Map<String, Object> newSettings = new HashMap<>(2);

        if (weightingType.equals(MovAvgModel.Weighting.DOUBLE_EXP)) {
            Double beta;
            if (settings == null || (beta = (Double)settings.get("beta")) == null) {
                beta = 0.5;
            }
            newSettings.put("beta", beta);
        }

        Double alpha;
        if (settings == null || (alpha = (Double)settings.get("alpha")) == null) {
            alpha = 0.5;
        }
        newSettings.put("alpha", alpha);

        return newSettings;
    }

}

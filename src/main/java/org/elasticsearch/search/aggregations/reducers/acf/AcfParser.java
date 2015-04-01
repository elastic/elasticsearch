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
import java.util.List;

import static org.elasticsearch.search.aggregations.reducers.BucketHelpers.GapPolicy;

public class AcfParser implements Reducer.Parser {

    public static final ParseField FORMAT = new ParseField("format");
    public static final ParseField GAP_POLICY = new ParseField("gap_policy");
    public static final ParseField ZERO_PAD = new ParseField("zero_pad");
    public static final ParseField NORMALIZE = new ParseField("normalize");
    public static final ParseField ZERO_MEAN = new ParseField("zero_mean");
    public static final ParseField WINDOW = new ParseField("window");



    @Override
    public String type() {
        return AcfReducer.TYPE.name();
    }

    @Override
    public ReducerFactory parse(String reducerName, XContentParser parser, SearchContext context) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        String[] bucketsPaths = null;
        String format = null;
        GapPolicy gapPolicy = GapPolicy.IGNORE;

        Boolean zeroPad = null;
        Boolean normalize = null;
        Boolean zeroMean = null;
        Integer window = null;


        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (FORMAT.match(currentFieldName)) {
                    format = parser.text();
                } else if (BUCKETS_PATH.match(currentFieldName)) {
                    bucketsPaths = new String[] { parser.text() };
                } else if (GAP_POLICY.match(currentFieldName)) {
                    gapPolicy = GapPolicy.parse(context, parser.text());
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: ["
                            + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if (ZERO_PAD.match(currentFieldName)) {
                    zeroPad = parser.booleanValue();
                } else if (ZERO_MEAN.match(currentFieldName)) {
                    zeroMean = parser.booleanValue();
                } else if (NORMALIZE.match(currentFieldName)) {
                    normalize = parser.booleanValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: ["
                            + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (WINDOW.match(currentFieldName)) {
                    window = parser.intValue();
                }  else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: ["
                            + currentFieldName + "].");
                }
            }else if (token == XContentParser.Token.START_ARRAY) {
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
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + reducerName + "].");
            }
        }

        if (bucketsPaths == null) {
            throw new SearchParseException(context, "Missing required field [" + BUCKETS_PATH.getPreferredName()
                    + "] for derivative aggregation [" + reducerName + "]");
        }

        ValueFormatter formatter = null;
        if (format != null) {
            formatter = ValueFormat.Patternable.Number.format(format).formatter();
        }

        AcfSettings acfSettings = new AcfSettings();

        // Then set individual flags if user specified
        if (zeroPad != null) {
            acfSettings.setZeroPad(zeroPad);
        }

        if (normalize != null) {
            acfSettings.setNormalize(normalize);
        }

        if (zeroMean != null) {
            acfSettings.setZeroMean(zeroMean);
        }

        if (window != null) {
            acfSettings.setWindow(window);
        }

        return new AcfReducer.Factory(reducerName, bucketsPaths, formatter, gapPolicy, acfSettings);
    }

}

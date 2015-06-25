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

package org.elasticsearch.search.aggregations.pipeline.having;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorFactory;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BucketSelectorParser implements PipelineAggregator.Parser {

    public static final ParseField FORMAT = new ParseField("format");
    public static final ParseField GAP_POLICY = new ParseField("gap_policy");
    public static final ParseField PARAMS_FIELD = new ParseField("params");

    @Override
    public String type() {
        return BucketSelectorPipelineAggregator.TYPE.name();
    }

    @Override
    public PipelineAggregatorFactory parse(String reducerName, XContentParser parser, SearchContext context) throws IOException {
        XContentParser.Token token;
        Script script = null;
        String currentFieldName = null;
        Map<String, String> bucketsPathsMap = null;
        GapPolicy gapPolicy = GapPolicy.SKIP;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (context.parseFieldMatcher().match(currentFieldName, BUCKETS_PATH)) {
                    bucketsPathsMap = new HashMap<>();
                    bucketsPathsMap.put("_value", parser.text());
                } else if (context.parseFieldMatcher().match(currentFieldName, GAP_POLICY)) {
                    gapPolicy = GapPolicy.parse(context, parser.text(), parser.getTokenLocation());
                } else if (context.parseFieldMatcher().match(currentFieldName, ScriptField.SCRIPT)) {
                    script = Script.parse(parser, context.parseFieldMatcher());
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (context.parseFieldMatcher().match(currentFieldName, BUCKETS_PATH)) {
                    List<String> paths = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String path = parser.text();
                        paths.add(path);
                    }
                    bucketsPathsMap = new HashMap<>();
                    for (int i = 0; i < paths.size(); i++) {
                        bucketsPathsMap.put("_value" + i, paths.get(i));
                    }
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (context.parseFieldMatcher().match(currentFieldName, ScriptField.SCRIPT)) {
                    script = Script.parse(parser, context.parseFieldMatcher());
                } else if (context.parseFieldMatcher().match(currentFieldName, BUCKETS_PATH)) {
                    Map<String, Object> map = parser.map();
                    bucketsPathsMap = new HashMap<>();
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        bucketsPathsMap.put(entry.getKey(), String.valueOf(entry.getValue()));
                    }
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + reducerName + "]: ["
                            + currentFieldName + "].", parser.getTokenLocation());
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + reducerName + "].",
                        parser.getTokenLocation());
            }
        }

        if (bucketsPathsMap == null) {
            throw new SearchParseException(context, "Missing required field [" + BUCKETS_PATH.getPreferredName()
                    + "] for bucket_selector aggregation [" + reducerName + "]", parser.getTokenLocation());
        }

        if (script == null) {
            throw new SearchParseException(context, "Missing required field [" + ScriptField.SCRIPT.getPreferredName()
                    + "] for bucket_selector aggregation [" + reducerName + "]", parser.getTokenLocation());
        }

        return new BucketSelectorPipelineAggregator.Factory(reducerName, bucketsPathsMap, script, gapPolicy);
    }

}

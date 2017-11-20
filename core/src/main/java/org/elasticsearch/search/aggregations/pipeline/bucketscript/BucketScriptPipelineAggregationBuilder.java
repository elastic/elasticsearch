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

package org.elasticsearch.search.aggregations.pipeline.bucketscript;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.BUCKETS_PATH;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.FORMAT;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.GAP_POLICY;

public class BucketScriptPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<BucketScriptPipelineAggregationBuilder> {
    public static final String NAME = "bucket_script";

    private final Script script;
    private final Map<String, String> bucketsPathsMap;
    private String format = null;
    private GapPolicy gapPolicy = GapPolicy.SKIP;

    public BucketScriptPipelineAggregationBuilder(String name, Map<String, String> bucketsPathsMap, Script script) {
        super(name, NAME, new TreeMap<>(bucketsPathsMap).values().toArray(new String[bucketsPathsMap.size()]));
        this.bucketsPathsMap = bucketsPathsMap;
        this.script = script;
    }

    public BucketScriptPipelineAggregationBuilder(String name, Script script, String... bucketsPaths) {
        this(name, convertToBucketsPathMap(bucketsPaths), script);
    }

    /**
     * Read from a stream.
     */
    public BucketScriptPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        int mapSize = in.readVInt();
        bucketsPathsMap = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            bucketsPathsMap.put(in.readString(), in.readString());
        }
        script = new Script(in);
        format = in.readOptionalString();
        gapPolicy = GapPolicy.readFrom(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(bucketsPathsMap.size());
        for (Entry<String, String> e : bucketsPathsMap.entrySet()) {
            out.writeString(e.getKey());
            out.writeString(e.getValue());
        }
        script.writeTo(out);
        out.writeOptionalString(format);
        gapPolicy.writeTo(out);
    }

    private static Map<String, String> convertToBucketsPathMap(String[] bucketsPaths) {
        Map<String, String> bucketsPathsMap = new HashMap<>();
        for (int i = 0; i < bucketsPaths.length; i++) {
            bucketsPathsMap.put("_value" + i, bucketsPaths[i]);
        }
        return bucketsPathsMap;
    }

    /**
     * Sets the format to use on the output of this aggregation.
     */
    public BucketScriptPipelineAggregationBuilder format(String format) {
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

    protected DocValueFormat formatter() {
        if (format != null) {
            return new DocValueFormat.Decimal(format);
        } else {
            return DocValueFormat.RAW;
        }
    }

    /**
     * Sets the gap policy to use for this aggregation.
     */
    public BucketScriptPipelineAggregationBuilder gapPolicy(GapPolicy gapPolicy) {
        if (gapPolicy == null) {
            throw new IllegalArgumentException("[gapPolicy] must not be null: [" + name + "]");
        }
        this.gapPolicy = gapPolicy;
        return this;
    }

    /**
     * Gets the gap policy to use for this aggregation.
     */
    public GapPolicy gapPolicy() {
        return gapPolicy;
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException {
        return new BucketScriptPipelineAggregator(name, bucketsPathsMap, script, formatter(), gapPolicy, metaData);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(BUCKETS_PATH.getPreferredName(), bucketsPathsMap);
        builder.field(Script.SCRIPT_PARSE_FIELD.getPreferredName(), script);
        if (format != null) {
            builder.field(FORMAT.getPreferredName(), format);
        }
        builder.field(GAP_POLICY.getPreferredName(), gapPolicy.getName());
        return builder;
    }

    public static BucketScriptPipelineAggregationBuilder parse(String reducerName, XContentParser parser) throws IOException {
        XContentParser.Token token;
        Script script = null;
        String currentFieldName = null;
        Map<String, String> bucketsPathsMap = null;
        String format = null;
        GapPolicy gapPolicy = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (FORMAT.match(currentFieldName)) {
                    format = parser.text();
                } else if (BUCKETS_PATH.match(currentFieldName)) {
                    bucketsPathsMap = new HashMap<>();
                    bucketsPathsMap.put("_value", parser.text());
                } else if (GAP_POLICY.match(currentFieldName)) {
                    gapPolicy = GapPolicy.parse(parser.text(), parser.getTokenLocation());
                } else if (Script.SCRIPT_PARSE_FIELD.match(currentFieldName)) {
                    script = Script.parse(parser);
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (BUCKETS_PATH.match(currentFieldName)) {
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
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (Script.SCRIPT_PARSE_FIELD.match(currentFieldName)) {
                    script = Script.parse(parser);
                } else if (BUCKETS_PATH.match(currentFieldName)) {
                    Map<String, Object> map = parser.map();
                    bucketsPathsMap = new HashMap<>();
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        bucketsPathsMap.put(entry.getKey(), String.valueOf(entry.getValue()));
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unexpected token " + token + " in [" + reducerName + "].");
            }
        }

        if (bucketsPathsMap == null) {
            throw new ParsingException(parser.getTokenLocation(), "Missing required field [" + BUCKETS_PATH.getPreferredName()
                    + "] for series_arithmetic aggregation [" + reducerName + "]");
        }

        if (script == null) {
            throw new ParsingException(parser.getTokenLocation(), "Missing required field [" + Script.SCRIPT_PARSE_FIELD.getPreferredName()
                    + "] for series_arithmetic aggregation [" + reducerName + "]");
        }

        BucketScriptPipelineAggregationBuilder factory =
                new BucketScriptPipelineAggregationBuilder(reducerName, bucketsPathsMap, script);
        if (format != null) {
            factory.format(format);
        }
        if (gapPolicy != null) {
            factory.gapPolicy(gapPolicy);
        }
        return factory;
    }


    @Override
    protected boolean overrideBucketsPath() {
        return true;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(bucketsPathsMap, script, format, gapPolicy);
    }

    @Override
    protected boolean doEquals(Object obj) {
        BucketScriptPipelineAggregationBuilder other = (BucketScriptPipelineAggregationBuilder) obj;
        return Objects.equals(bucketsPathsMap, other.bucketsPathsMap) && Objects.equals(script, other.script)
                && Objects.equals(format, other.format) && Objects.equals(gapPolicy, other.gapPolicy);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}

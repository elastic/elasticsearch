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

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;

import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.BUCKETS_PATH;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.FORMAT;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.GAP_POLICY;

public class BucketScriptPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<BucketScriptPipelineAggregationBuilder> {
    public static final String NAME = "bucket_script";

    private final Script script;
    private final Map<String, String> bucketsPathsMap;
    private String format = null;
    private GapPolicy gapPolicy = GapPolicy.SKIP;

    private static final Function<String, ConstructingObjectParser<BucketScriptPipelineAggregationBuilder, Void>> PARSER
        = name -> {

        @SuppressWarnings("unchecked")
        ConstructingObjectParser<BucketScriptPipelineAggregationBuilder, Void> parser = new ConstructingObjectParser<>(
            BucketScriptPipelineAggregationBuilder.NAME,
            false,
            o -> new BucketScriptPipelineAggregationBuilder(name, (Map<String, String>) o[0], (Script) o[1]));

        parser.declareField(ConstructingObjectParser.constructorArg()
            , BucketScriptPipelineAggregationBuilder::extractBucketPath
            , BUCKETS_PATH_FIELD
            , ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING);
        parser.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> Script.parse(p), Script.SCRIPT_PARSE_FIELD, ObjectParser.ValueType.OBJECT_OR_STRING);

        parser.declareString(BucketScriptPipelineAggregationBuilder::format, FORMAT);
        parser.declareField(BucketScriptPipelineAggregationBuilder::gapPolicy, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return GapPolicy.parse(p.text().toLowerCase(Locale.ROOT), p.getTokenLocation());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, GAP_POLICY, ObjectParser.ValueType.STRING);

        return parser;
    };


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

    private static Map<String, String> extractBucketPath(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
       if (token == XContentParser.Token.VALUE_STRING) {
           // input is a string, name of the path set to '_value'.
           // This is a bit odd as there is not constructor for it
           return Collections.singletonMap("_value", parser.text());
       } else if (token == XContentParser.Token.START_ARRAY) {
           // input is an array, name of the path set to '_value' + position
           Map<String, String> bucketsPathsMap = new HashMap<>();
           int i =0;
           while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
               String path = parser.text();
               bucketsPathsMap.put("_value" + i++, path);
           }
           return bucketsPathsMap;
       } else  {
           // input is an object, it should contain name / value pairs
           return parser.mapStrings();
       }
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
    protected PipelineAggregator createInternal(Map<String, Object> metaData) {
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

    public static BucketScriptPipelineAggregationBuilder parse(String aggName, XContentParser parser) {
        return PARSER.apply(aggName).apply(parser, null);
    }

    @Override
    protected boolean overrideBucketsPath() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bucketsPathsMap, script, format, gapPolicy);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        BucketScriptPipelineAggregationBuilder other = (BucketScriptPipelineAggregationBuilder) obj;
        return Objects.equals(bucketsPathsMap, other.bucketsPathsMap)
            && Objects.equals(script, other.script)
            && Objects.equals(format, other.format)
            && Objects.equals(gapPolicy, other.gapPolicy);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}

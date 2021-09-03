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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.BUCKETS_PATH;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.GAP_POLICY;

public class BucketSelectorPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<BucketSelectorPipelineAggregationBuilder> {
    public static final String NAME = "bucket_selector";

    private final Map<String, String> bucketsPathsMap;
    private final Script script;
    private GapPolicy gapPolicy = GapPolicy.SKIP;

    public BucketSelectorPipelineAggregationBuilder(String name, Map<String, String> bucketsPathsMap, Script script) {
        super(name, NAME, new TreeMap<>(bucketsPathsMap).values().toArray(new String[bucketsPathsMap.size()]));
        this.bucketsPathsMap = bucketsPathsMap;
        this.script = script;
    }

    public BucketSelectorPipelineAggregationBuilder(String name, Script script, String... bucketsPaths) {
        this(name, convertToBucketsPathMap(bucketsPaths), script);
    }

    /**
     * Read from a stream.
     */
    public BucketSelectorPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        bucketsPathsMap = in.readMap(StreamInput::readString, StreamInput::readString);
        script = new Script(in);
        gapPolicy = GapPolicy.readFrom(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeMap(bucketsPathsMap, StreamOutput::writeString, StreamOutput::writeString);
        script.writeTo(out);
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
     * Sets the gap policy to use for this aggregation.
     */
    public BucketSelectorPipelineAggregationBuilder gapPolicy(GapPolicy gapPolicy) {
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
    protected PipelineAggregator createInternal(Map<String, Object> metadata) {
        return new BucketSelectorPipelineAggregator(name, bucketsPathsMap, script, gapPolicy, metadata);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(BUCKETS_PATH.getPreferredName(), bucketsPathsMap);
        builder.field(Script.SCRIPT_PARSE_FIELD.getPreferredName(), script);
        builder.field(GAP_POLICY.getPreferredName(), gapPolicy.getName());
        return builder;
    }

    public static BucketSelectorPipelineAggregationBuilder parse(String reducerName, XContentParser parser) throws IOException {
        XContentParser.Token token;
        Script script = null;
        String currentFieldName = null;
        Map<String, String> bucketsPathsMap = null;
        GapPolicy gapPolicy = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (BUCKETS_PATH.match(currentFieldName, parser.getDeprecationHandler())) {
                    bucketsPathsMap = new HashMap<>();
                    bucketsPathsMap.put("_value", parser.text());
                } else if (GAP_POLICY.match(currentFieldName, parser.getDeprecationHandler())) {
                    gapPolicy = GapPolicy.parse(parser.text(), parser.getTokenLocation());
                } else if (Script.SCRIPT_PARSE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    script = Script.parse(parser);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "]."
                    );
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (BUCKETS_PATH.match(currentFieldName, parser.getDeprecationHandler())) {
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
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "]."
                    );
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (Script.SCRIPT_PARSE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    script = Script.parse(parser);
                } else if (BUCKETS_PATH.match(currentFieldName, parser.getDeprecationHandler())) {
                    Map<String, Object> map = parser.map();
                    bucketsPathsMap = new HashMap<>();
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        bucketsPathsMap.put(entry.getKey(), String.valueOf(entry.getValue()));
                    }
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "]."
                    );
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unexpected token " + token + " in [" + reducerName + "].");
            }
        }

        if (bucketsPathsMap == null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Missing required field [" + BUCKETS_PATH.getPreferredName() + "] for bucket_selector aggregation [" + reducerName + "]"
            );
        }

        if (script == null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Missing required field ["
                    + Script.SCRIPT_PARSE_FIELD.getPreferredName()
                    + "] for bucket_selector aggregation ["
                    + reducerName
                    + "]"
            );
        }

        BucketSelectorPipelineAggregationBuilder factory = new BucketSelectorPipelineAggregationBuilder(
            reducerName,
            bucketsPathsMap,
            script
        );
        if (gapPolicy != null) {
            factory.gapPolicy(gapPolicy);
        }
        return factory;
    }

    @Override
    protected void validate(ValidationContext context) {
        context.validateHasParent(NAME, name);
    }

    @Override
    protected boolean overrideBucketsPath() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bucketsPathsMap, script, gapPolicy);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        BucketSelectorPipelineAggregationBuilder other = (BucketSelectorPipelineAggregationBuilder) obj;
        return Objects.equals(bucketsPathsMap, other.bucketsPathsMap)
            && Objects.equals(script, other.script)
            && Objects.equals(gapPolicy, other.gapPolicy);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}

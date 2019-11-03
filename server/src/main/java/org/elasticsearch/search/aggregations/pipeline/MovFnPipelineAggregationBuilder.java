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

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.io.IOException;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.BUCKETS_PATH;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.FORMAT;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.GAP_POLICY;

public class MovFnPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<MovFnPipelineAggregationBuilder> {
    public static final String NAME = "moving_fn";
    private static final ParseField WINDOW = new ParseField("window");
    private static final ParseField SHIFT = new ParseField("shift");

    private final Script script;
    private final String bucketsPathString;
    private String format = null;
    private GapPolicy gapPolicy = GapPolicy.SKIP;
    private int window;
    private int shift;

    private static final Function<String, ConstructingObjectParser<MovFnPipelineAggregationBuilder, Void>> PARSER
        = name -> {

        ConstructingObjectParser<MovFnPipelineAggregationBuilder, Void> parser = new ConstructingObjectParser<>(
            MovFnPipelineAggregationBuilder.NAME,
            false,
            o -> new MovFnPipelineAggregationBuilder(name, (String) o[0], (Script) o[1], (int)o[2]));

        parser.declareString(ConstructingObjectParser.constructorArg(), BUCKETS_PATH_FIELD);
        parser.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> Script.parse(p), Script.SCRIPT_PARSE_FIELD, ObjectParser.ValueType.OBJECT_OR_STRING);
        parser.declareInt(ConstructingObjectParser.constructorArg(), WINDOW);

        parser.declareInt(MovFnPipelineAggregationBuilder::setShift, SHIFT);
        parser.declareString(MovFnPipelineAggregationBuilder::format, FORMAT);
        parser.declareField(MovFnPipelineAggregationBuilder::gapPolicy, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return GapPolicy.parse(p.text().toLowerCase(Locale.ROOT), p.getTokenLocation());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, GAP_POLICY, ObjectParser.ValueType.STRING);

        return parser;
    };


    public MovFnPipelineAggregationBuilder(String name, String bucketsPath, Script script, int window) {
        super(name, NAME, new String[]{bucketsPath});
        this.bucketsPathString = bucketsPath;
        this.script = script;
        if (window <= 0) {
            throw new IllegalArgumentException("[" + WINDOW.getPreferredName() + "] must be a positive, non-zero integer.");
        }
        this.window = window;
    }

    public MovFnPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        bucketsPathString = in.readString();
        script = new Script(in);
        format = in.readOptionalString();
        gapPolicy = GapPolicy.readFrom(in);
        window = in.readInt();
        if (in.getVersion().onOrAfter(Version.V_7_4_0)) {
            shift = in.readInt();
        } else {
            shift = 0;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(bucketsPathString);
        script.writeTo(out);
        out.writeOptionalString(format);
        gapPolicy.writeTo(out);
        out.writeInt(window);
        if (out.getVersion().onOrAfter(Version.V_7_4_0)) {
            out.writeInt(shift);
        }
    }

    /**
     * Sets the format to use on the output of this aggregation.
     */
    public MovFnPipelineAggregationBuilder format(String format) {
        if (Strings.isNullOrEmpty(format)) {
            throw new IllegalArgumentException("[" + FORMAT.getPreferredName() + "] must not be null or an empty string.");
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
        }
        return DocValueFormat.RAW;
    }

    /**
     * Sets the gap policy to use for this aggregation.
     */
    public MovFnPipelineAggregationBuilder gapPolicy(GapPolicy gapPolicy) {
        if (gapPolicy == null) {
            throw new IllegalArgumentException("[" + GAP_POLICY.getPreferredName() + "] must not be null.");
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

    /**
     * Returns the window size for this aggregation
     */
    public int getWindow() {
        return window;
    }

    /**
     * Sets the window size for this aggregation
     */
    public void setWindow(int window) {
        if (window <= 0) {
            throw new IllegalArgumentException("[" + WINDOW.getPreferredName() + "] must be a positive, non-zero integer.");
        }
        this.window = window;
    }

    public void setShift(int shift) {
        this.shift = shift;
    }

    @Override
    public void doValidate(AggregatorFactory parent, Collection<AggregationBuilder> aggFactories,
                           Collection<PipelineAggregationBuilder> pipelineAggregatorFactories) {
        if (window <= 0) {
            throw new IllegalArgumentException("[" + WINDOW.getPreferredName() + "] must be a positive, non-zero integer.");
        }
        
        validateSequentiallyOrderedParentAggs(parent, NAME, name);
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) {
        return new MovFnPipelineAggregator(name, bucketsPathString, script, window, shift, formatter(), gapPolicy, metaData);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(BUCKETS_PATH.getPreferredName(), bucketsPathString);
        builder.field(Script.SCRIPT_PARSE_FIELD.getPreferredName(), script);
        if (format != null) {
            builder.field(FORMAT.getPreferredName(), format);
        }
        builder.field(GAP_POLICY.getPreferredName(), gapPolicy.getName());
        builder.field(WINDOW.getPreferredName(), window);
        builder.field(SHIFT.getPreferredName(), shift);
        return builder;
    }

    public static MovFnPipelineAggregationBuilder parse(String aggName, XContentParser parser) {
        return PARSER.apply(aggName).apply(parser, null);
    }

    /**
     * Used for serialization testing, since pipeline aggs serialize themselves as a named object but are parsed
     * as a regular object with the name passed in.
     */
    static MovFnPipelineAggregationBuilder parse(XContentParser parser) throws IOException {
        parser.nextToken();
        if (parser.currentToken().equals(XContentParser.Token.START_OBJECT)) {
            parser.nextToken();
            if (parser.currentToken().equals(XContentParser.Token.FIELD_NAME)) {
                String aggName = parser.currentName();
                parser.nextToken(); // "moving_fn"
                parser.nextToken(); // start_object
                return PARSER.apply(aggName).apply(parser, null);
            }
        }

        throw new IllegalStateException("Expected aggregation name but none found");
    }

    @Override
    protected boolean overrideBucketsPath() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bucketsPathString, script, format, gapPolicy, window, shift);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        MovFnPipelineAggregationBuilder other = (MovFnPipelineAggregationBuilder) obj;
        return Objects.equals(bucketsPathString, other.bucketsPathString)
            && Objects.equals(script, other.script)
            && Objects.equals(format, other.format)
            && Objects.equals(gapPolicy, other.gapPolicy)
            && Objects.equals(window, other.window)
            && Objects.equals(shift, other.shift);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}

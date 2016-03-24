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

package org.elasticsearch.search.aggregations.pipeline.serialdiff;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class SerialDiffPipelineAggregatorBuilder extends PipelineAggregatorBuilder<SerialDiffPipelineAggregatorBuilder> {

    static final SerialDiffPipelineAggregatorBuilder PROTOTYPE = new SerialDiffPipelineAggregatorBuilder("", "");

    private String format;
    private GapPolicy gapPolicy = GapPolicy.SKIP;
    private int lag = 1;

    public SerialDiffPipelineAggregatorBuilder(String name, String bucketsPath) {
        this(name, new String[] { bucketsPath });
    }

    private SerialDiffPipelineAggregatorBuilder(String name, String[] bucketsPaths) {
        super(name, SerialDiffPipelineAggregator.TYPE.name(), bucketsPaths);
    }

    /**
     * Sets the lag to use when calculating the serial difference.
     */
    public SerialDiffPipelineAggregatorBuilder lag(int lag) {
        if (lag <= 0) {
            throw new IllegalArgumentException("[lag] must be a positive integer: [" + name + "]");
        }
        this.lag = lag;
        return this;
    }

    /**
     * Gets the lag to use when calculating the serial difference.
     */
    public int lag() {
        return lag;
    }

    /**
     * Sets the format to use on the output of this aggregation.
     */
    public SerialDiffPipelineAggregatorBuilder format(String format) {
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
    public SerialDiffPipelineAggregatorBuilder gapPolicy(GapPolicy gapPolicy) {
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

    protected ValueFormatter formatter() {
        if (format != null) {
            return ValueFormat.Patternable.Number.format(format).formatter();
        } else {
            return ValueFormatter.RAW;
        }
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException {
        return new SerialDiffPipelineAggregator(name, bucketsPaths, formatter(), gapPolicy, lag, metaData);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(SerialDiffParser.FORMAT.getPreferredName(), format);
        }
        builder.field(SerialDiffParser.GAP_POLICY.getPreferredName(), gapPolicy.getName());
        builder.field(SerialDiffParser.LAG.getPreferredName(), lag);
        return builder;
    }

    @Override
    protected SerialDiffPipelineAggregatorBuilder doReadFrom(String name, String[] bucketsPaths, StreamInput in) throws IOException {
        SerialDiffPipelineAggregatorBuilder factory = new SerialDiffPipelineAggregatorBuilder(name, bucketsPaths);
        factory.format = in.readOptionalString();
        factory.gapPolicy = GapPolicy.readFrom(in);
        factory.lag = in.readVInt();
        return factory;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(format);
        gapPolicy.writeTo(out);
        out.writeVInt(lag);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(format, gapPolicy, lag);
    }
    @Override
    protected boolean doEquals(Object obj) {
        SerialDiffPipelineAggregatorBuilder other = (SerialDiffPipelineAggregatorBuilder) obj;
        return Objects.equals(format, other.format)
                && Objects.equals(gapPolicy, other.gapPolicy)
                && Objects.equals(lag, other.lag);
    }

}
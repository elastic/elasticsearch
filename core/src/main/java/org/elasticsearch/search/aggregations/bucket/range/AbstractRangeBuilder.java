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

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamInputReader;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class AbstractRangeBuilder<AB extends AbstractRangeBuilder<AB, R>, R extends Range>
        extends ValuesSourceAggregationBuilder<ValuesSource.Numeric, AB> {

    protected final InternalRange.Factory<?, ?> rangeFactory;
    protected List<R> ranges = new ArrayList<>();
    protected boolean keyed = false;

    protected AbstractRangeBuilder(String name, InternalRange.Factory<?, ?> rangeFactory) {
        super(name, rangeFactory.type(), rangeFactory.getValueSourceType(), rangeFactory.getValueType());
        this.rangeFactory = rangeFactory;
    }

    /**
     * Read from a stream.
     */
    protected AbstractRangeBuilder(StreamInput in, InternalRange.Factory<?, ?> rangeFactory, StreamInputReader<R> rangeReader)
            throws IOException {
        super(in, rangeFactory.type(), rangeFactory.getValueSourceType(), rangeFactory.getValueType());
        this.rangeFactory = rangeFactory;
        ranges = in.readList(rangeReader);
        keyed = in.readBoolean();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(ranges.size());
        for (Range range : ranges) {
            range.writeTo(out);
        }
        out.writeBoolean(keyed);
    }

    public AB addRange(R range) {
        if (range == null) {
            throw new IllegalArgumentException("[range] must not be null: [" + name + "]");
        }
        ranges.add(range);
        return (AB) this;
    }

    public List<R> ranges() {
        return ranges;
    }

    public AB keyed(boolean keyed) {
        this.keyed = keyed;
        return (AB) this;
    }

    public boolean keyed() {
        return keyed;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(RangeAggregator.RANGES_FIELD.getPreferredName(), ranges);
        builder.field(RangeAggregator.KEYED_FIELD.getPreferredName(), keyed);
        return builder;
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(ranges, keyed);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        AbstractRangeBuilder<AB, R> other = (AbstractRangeBuilder<AB, R>) obj;
        return Objects.equals(ranges, other.ranges)
                && Objects.equals(keyed, other.keyed);
    }
}

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

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Class for holding Rollover related information within an index
 */
public class RolloverInfo extends AbstractDiffable<RolloverInfo> implements Writeable, ToXContentFragment {

    public static final ParseField CONDITION_FIELD = new ParseField("met_conditions");
    public static final ParseField TIME_FIELD = new ParseField("time");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<RolloverInfo, String> PARSER = new ConstructingObjectParser<>("rollover_info", false,
        (a, alias) -> new RolloverInfo(alias, (List<Condition<?>>) a[0], (Long) a[1]));
    static {
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(),
            (p, c, n) -> p.namedObject(Condition.class, n, c), CONDITION_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TIME_FIELD);
    }

    private final String alias;
    private final List<Condition<?>> metConditions;
    private final long time;

    public RolloverInfo(String alias, List<Condition<?>> metConditions, long time) {
        this.alias = alias;
        this.metConditions = metConditions;
        this.time = time;
    }

    @SuppressWarnings("unchecked")
    public RolloverInfo(StreamInput in) throws IOException {
        this.alias = in.readString();
        this.time = in.readVLong();
        this.metConditions = (List) in.readNamedWriteableList(Condition.class);
    }

    public static RolloverInfo parse(XContentParser parser, String alias) {
        return PARSER.apply(parser, alias);
    }

    public String getAlias() {
        return alias;
    }

    public List<Condition<?>> getMetConditions() {
        return metConditions;
    }

    public long getTime() {
        return time;
    }

    public static Diff<RolloverInfo> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(RolloverInfo::new, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(alias);
        out.writeVLong(time);
        out.writeNamedWriteableList(metConditions);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(alias);
        builder.startObject(CONDITION_FIELD.getPreferredName());
        for (Condition<?> condition : metConditions) {
            condition.toXContent(builder, params);
        }
        builder.endObject();
        builder.field(TIME_FIELD.getPreferredName(), time);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias, metConditions, time);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        RolloverInfo other = (RolloverInfo) obj;
        return Objects.equals(alias, other.alias) &&
            Objects.equals(metConditions, other.metConditions) &&
            Objects.equals(time, other.time);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}

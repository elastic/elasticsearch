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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.WriteableZoneId;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Object representing information about rollup-v2 indices and their respective original-indexes. These objects
 * also include information about their capabilities, like which date-intervals and date-timezones they are configured
 * with. Used by {@link RollupMetadata}.
 *
 * The information in this class will be used to decide which index within the <code>group</code> will be chosen
 * for a specific aggregation. For example, if there are two indices with different intervals (`1h`, `1d`) and
 * a date-histogram aggregation request is sent for daily intervals, then the index with the associated `1d` interval
 * will be chosen.
 */
public class RollupGroup extends AbstractDiffable<RollupGroup> implements ToXContentObject {
    private static final ParseField GROUP_FIELD = new ParseField("group");
    private static final ParseField DATE_INTERVAL_FIELD = new ParseField("interval");
    private static final ParseField DATE_TIMEZONE_FIELD = new ParseField("timezone");

    // the list of indices part of this rollup group
    private List<String> group;
    // a map from index-name to the date interval used in the associated index
    private Map<String, DateHistogramInterval> dateInterval;
    // a map from index-name to timezone used in the associated index
    private Map<String, WriteableZoneId> dateTimezone;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<RollupGroup, Void> PARSER =
        new ConstructingObjectParser<>("rollup_group", false,
            a -> new RollupGroup((List<String>) a[0], (Map<String, DateHistogramInterval>) a[1], (Map<String, WriteableZoneId>) a[2]));

    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), GROUP_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, DateHistogramInterval> intervalMap = new HashMap<>();

            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                p.nextToken();
                String expression = p.text();
                intervalMap.put(name, new DateHistogramInterval(expression));
            }
            return intervalMap;
        }, DATE_INTERVAL_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, WriteableZoneId> zoneMap = new HashMap<>();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                p.nextToken();
                String timezone = p.text();
                zoneMap.put(name, WriteableZoneId.of(timezone));
            }
            return zoneMap;
        }, DATE_TIMEZONE_FIELD);
    }

    public RollupGroup(List<String> group, Map<String, DateHistogramInterval> dateInterval, Map<String, WriteableZoneId> dateTimezone) {
        this.group = group;
        this.dateInterval = dateInterval;
        this.dateTimezone = dateTimezone;
    }

    public RollupGroup() {
        this.group = new ArrayList<>();
        this.dateInterval = new HashMap<>();
        this.dateTimezone = new HashMap<>();
    }

    public RollupGroup(StreamInput in) throws IOException {
        this.group = in.readStringList();
        this.dateInterval = in.readMap(StreamInput::readString, DateHistogramInterval::new);
        this.dateTimezone = in.readMap(StreamInput::readString, WriteableZoneId::new);
    }


    public static RollupGroup fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public void add(String name, DateHistogramInterval interval, WriteableZoneId timezone) {
        group.add(name);
        dateInterval.put(name, interval);
        dateTimezone.put(name, timezone);
    }

    public void remove(String name) {
        group.remove(name);
        dateInterval.remove(name);
        dateTimezone.remove(name);
    }

    public boolean contains(String name) {
        return group.contains(name);
    }

    public DateHistogramInterval getDateInterval(String name) {
        return dateInterval.get(name);
    }

    public WriteableZoneId getDateTimezone(String name) {
        return dateTimezone.get(name);
    }

    public List<String> getIndices() {
        return group;
    }

    static Diff<RollupGroup> readDiffFrom(StreamInput in) throws IOException {
        return AbstractDiffable.readDiffFrom(RollupGroup::new, in);
    }

    public static RollupGroup parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(group);
        out.writeMap(dateInterval, StreamOutput::writeString, (stream, val) -> val.writeTo(stream));
        out.writeMap(dateTimezone, StreamOutput::writeString, (stream, val) -> val.writeTo(stream));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder
            .startObject()
            .field(GROUP_FIELD.getPreferredName(), group)
            .field(DATE_INTERVAL_FIELD.getPreferredName(), dateInterval)
            .field(DATE_TIMEZONE_FIELD.getPreferredName(), dateTimezone)
            .endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RollupGroup that = (RollupGroup) o;
        return group.equals(that.group) &&
            dateInterval.equals(that.dateInterval) &&
            dateTimezone.equals(that.dateTimezone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, dateInterval, dateTimezone);
    }
}

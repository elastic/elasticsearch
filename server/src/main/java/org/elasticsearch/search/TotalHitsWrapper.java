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

package org.elasticsearch.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

class TotalHitsWrapper implements Writeable, ToXContentFragment {
    private static ConstructingObjectParser<TotalHitsWrapper, Void> PARSER = new ConstructingObjectParser<>("tracked_total",
        (a) -> new TotalHitsWrapper((long) a[0], (TotalHits.Relation) a[1]));

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), new ParseField("value"));
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> parseRelation(p.text()), new ParseField("relation"), ObjectParser.ValueType.STRING);
    }

    final TotalHits totalHits;

    TotalHitsWrapper(long value, TotalHits.Relation relation) {
        this.totalHits = new TotalHits(value, relation);
    }

    TotalHitsWrapper(StreamInput in) throws IOException {
        this.totalHits = new TotalHits(in.readVLong(), in.readEnum(TotalHits.Relation.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalHits.value);
        out.writeEnum(totalHits.relation);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("value", totalHits.value);
        builder.field("relation", getRelation());
        return builder;
    }

    public static TotalHitsWrapper fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TotalHitsWrapper that = (TotalHitsWrapper) o;
        return totalHits.value == that.totalHits.value &&
            totalHits.relation == that.totalHits.relation;
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalHits.value, totalHits.relation);
    }

    private String getRelation() {
        return totalHits.relation == TotalHits.Relation.EQUAL_TO ? "eq" : "gte";
    }

    private static TotalHits.Relation parseRelation(String rel) {
        switch (rel) {
            case "gte":
                return TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;

            case "eq":
                return TotalHits.Relation.EQUAL_TO;

            default:
                throw new IllegalArgumentException("invalid relation:[" + rel + "] for [tracked_total]");
        }
    }
}

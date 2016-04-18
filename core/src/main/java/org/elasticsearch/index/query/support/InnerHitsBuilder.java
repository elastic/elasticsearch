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
package org.elasticsearch.index.query.support;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class InnerHitsBuilder extends ToXContentToBytes implements Writeable<InnerHitsBuilder> {
    private final Map<String, InnerHitBuilder> innerHitsBuilders;

    public InnerHitsBuilder() {
        this.innerHitsBuilders = new HashMap<>();
    }

    public InnerHitsBuilder(Map<String, InnerHitBuilder> innerHitsBuilders) {
        this.innerHitsBuilders = Objects.requireNonNull(innerHitsBuilders);
    }

    /**
     * Read from a stream.
     */
    public InnerHitsBuilder(StreamInput in) throws IOException {
        int size = in.readVInt();
        innerHitsBuilders = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            innerHitsBuilders.put(in.readString(), new InnerHitBuilder(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(innerHitsBuilders.size());
        for (Map.Entry<String, InnerHitBuilder> entry : innerHitsBuilders.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    public InnerHitsBuilder addInnerHit(String name, InnerHitBuilder builder) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(builder);
        this.innerHitsBuilders.put(name, builder.setName(name));
        return this;
    }

    public Map<String, InnerHitBuilder> getInnerHitsBuilders() {
        return innerHitsBuilders;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Map.Entry<String, InnerHitBuilder> entry : innerHitsBuilders.entrySet()) {
            builder.field(entry.getKey(), entry.getValue(), params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InnerHitsBuilder that = (InnerHitsBuilder) o;
        return innerHitsBuilders.equals(that.innerHitsBuilders);

    }

    @Override
    public int hashCode() {
        return innerHitsBuilders.hashCode();
    }

    public static InnerHitsBuilder fromXContent(QueryParseContext context) throws IOException {
        Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
        String innerHitName = null;
        XContentParser parser = context.parser();
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            switch (token) {
                case START_OBJECT:
                    InnerHitBuilder innerHitBuilder = InnerHitBuilder.fromXContent(context);
                    innerHitBuilder.setName(innerHitName);
                    innerHitBuilders.put(innerHitName, innerHitBuilder);
                    break;
                case FIELD_NAME:
                    innerHitName = parser.currentName();
                    break;
                default:
                    throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.START_OBJECT + "] in ["
                            + parser.currentName() + "] but found [" + token + "]", parser.getTokenLocation());
            }
        }
        return new InnerHitsBuilder(innerHitBuilders);
    }


}

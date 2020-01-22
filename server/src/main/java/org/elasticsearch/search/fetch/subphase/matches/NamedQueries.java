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

package org.elasticsearch.search.fetch.subphase.matches;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class NamedQueries extends MatchesResult {

    private final List<String> names;

    public NamedQueries(List<String> names) {
        this.names = names;
    }

    public NamedQueries(StreamInput in) throws IOException {
        this.names = in.readStringList();
    }

    public List<String> getNamedQueries() {
        return names;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(names);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(NamedQueriesProcessor.NAME);
        for (String name : names) {
            builder.value(name);
        }
        return builder.endArray();
    }

    @Override
    public String getWriteableName() {
        return NamedQueriesProcessor.NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NamedQueries that = (NamedQueries) o;
        return Objects.equals(names, that.names);
    }

    @Override
    public int hashCode() {
        return Objects.hash(names);
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<NamedQueries, Void> PARSER = new ConstructingObjectParser<>(NamedQueriesProcessor.NAME,
        args -> {
            List<String> names = (List<String>) args[0];
            return new NamedQueries(names);
        });

    public static NamedQueries fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}

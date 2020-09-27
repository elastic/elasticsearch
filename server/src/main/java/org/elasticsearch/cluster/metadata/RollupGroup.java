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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

// TODO(talevy): optimize for fast lookup speed
public class RollupGroup extends AbstractDiffable<RollupGroup> implements ToXContentObject {
    private static final ParseField GROUP = new ParseField("group");

    private List<String> group;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<RollupGroup, Void> PARSER =
        new ConstructingObjectParser<>("rollup_group", false,
            a -> new RollupGroup((List<String>) a[0]));

    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), GROUP);
    }

    public RollupGroup(List<String> group) {
        this.group = group;
    }

    public RollupGroup(StreamInput in) throws IOException {
        this.group = in.readStringList();
    }

    public void add(String name) {
        group.add(name);
    }

    public void remove(String name) {
        group.remove(name);
    }

    public boolean contains(String name) {
        return group.contains(name);
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
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().array(GROUP.getPreferredName(), group).endObject();
    }
}

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

package org.elasticsearch.cluster.factory;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.SettingsLoader;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;

public class SettingsClusterStatePartFactory extends ClusterStatePartFactory<Settings> {
    public SettingsClusterStatePartFactory(String type) {
        super(type);
    }

    public SettingsClusterStatePartFactory(String type, EnumSet<XContentContext> context) {
        super(type, context);
    }

    @Override
    public Settings readFrom(StreamInput in, String partName, @Nullable DiscoveryNode localNode) throws IOException {
        return ImmutableSettings.readSettingsFromStream(in);
    }

    @Override
    public void writeTo(Settings part, StreamOutput out) throws IOException {
        ImmutableSettings.writeSettingsToStream(part, out);
    }

    @Override
    public Settings fromXContent(XContentParser parser, String partName, @Nullable DiscoveryNode localNode) throws IOException {
        return ImmutableSettings.settingsBuilder().put(SettingsLoader.Helper.loadNestedFromMap(parser.mapOrdered())).build();
    }

    @Override
    public void toXContent(Settings part, XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        part.toXContent(builder, params);
        builder.endObject();
    }
}
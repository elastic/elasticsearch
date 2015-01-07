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
package org.elasticsearch.cluster;

import org.elasticsearch.cluster.metadata.IndexClusterStatePart;
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
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.readSettingsFromStream;
import static org.elasticsearch.common.settings.ImmutableSettings.writeSettingsToStream;

/**
 * Wrapper for settings as a cluster state part.
 *
 * Used to represent persistent, transient and index settings in the cluster metadata.
 */
public class ClusterStateSettingsPart extends AbstractClusterStatePart implements IndexClusterStatePart<ClusterStateSettingsPart> {

    private final String type;
    private final Settings settings;

    private ClusterStateSettingsPart(String type, Settings settings) {
        this.type = type;
        this.settings = settings;
    }

    public Settings getSettings() {
        return settings;
    }

    @Override
    public String partType() {
        return type;
    }

    @Override
    public ClusterStateSettingsPart mergeWith(ClusterStateSettingsPart second) {
        return second;
    }

    public static class Factory extends AbstractClusterStatePart.AbstractFactory<ClusterStateSettingsPart> {
        private final String type;
        private final EnumSet<XContentContext> xContentContext;

        public Factory(String type) {
            this.type = type;
            xContentContext = API;
        }

        public Factory(String type, EnumSet<XContentContext> xContentContext) {
            this.type = type;
            this.xContentContext = xContentContext;
        }

        @Override
        public ClusterStateSettingsPart readFrom(StreamInput in, LocalContext context) throws IOException {
            return new ClusterStateSettingsPart(type, readSettingsFromStream(in));
        }

        @Override
        public void writeTo(ClusterStateSettingsPart clusterStateSettingsPart, StreamOutput out) throws IOException {
            writeSettingsToStream(clusterStateSettingsPart.settings, out);
        }
        @Override
        public ClusterStateSettingsPart fromXContent(XContentParser parser, LocalContext context) throws IOException {
            Settings settings = ImmutableSettings.settingsBuilder().put(SettingsLoader.Helper.loadNestedFromMap(parser.mapOrdered())).build();
            return new ClusterStateSettingsPart(type, settings);
        }

        @Override
        public void toXContent(ClusterStateSettingsPart clusterStateSettingsPart, XContentBuilder builder, Params params) throws IOException {
            for (Map.Entry<String, String> entry : clusterStateSettingsPart.settings.getAsMap().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        }

        public ClusterStateSettingsPart fromSettings(Settings settings) {
            return new ClusterStateSettingsPart(type, settings);
        }

        @Override
        public EnumSet<XContentContext> context() {
            return xContentContext;
        }

        @Override
        public String partType() {
            return type;
        }

    }
}

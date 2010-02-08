/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this 
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.util.MapBuilder;
import org.elasticsearch.util.Preconditions;
import org.elasticsearch.util.concurrent.Immutable;
import org.elasticsearch.util.settings.ImmutableSettings;
import org.elasticsearch.util.settings.Settings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.util.settings.ImmutableSettings.*;

/**
 * @author kimchy (Shay Banon)
 */
@Immutable
public class IndexMetaData {

    public static final String SETTING_NUMBER_OF_SHARDS = "index.numberOfShards";

    public static final String SETTING_NUMBER_OF_REPLICAS = "index.numberOfReplicas";

    private final String index;

    private final Settings settings;

    private final ImmutableMap<String, String> mappings;

    private transient final int totalNumberOfShards;

    private IndexMetaData(String index, Settings settings, ImmutableMap<String, String> mappings) {
        Preconditions.checkArgument(settings.getAsInt(SETTING_NUMBER_OF_SHARDS, -1) != -1, "must specify numberOfShards");
        Preconditions.checkArgument(settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, -1) != -1, "must specify numberOfReplicas");
        this.index = index;
        this.settings = settings;
        this.mappings = mappings;
        this.totalNumberOfShards = numberOfShards() * (numberOfReplicas() + 1);
    }

    public String index() {
        return index;
    }

    public int numberOfShards() {
        return settings.getAsInt(SETTING_NUMBER_OF_SHARDS, -1);
    }

    public int numberOfReplicas() {
        return settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, -1);
    }

    public int totalNumberOfShards() {
        return totalNumberOfShards;
    }

    public Settings settings() {
        return settings;
    }

    public ImmutableMap<String, String> mappings() {
        return mappings;
    }

    public static Builder newIndexMetaDataBuilder(String index) {
        return new Builder(index);
    }

    public static Builder newIndexMetaDataBuilder(IndexMetaData indexMetaData) {
        return new Builder(indexMetaData);
    }

    public static class Builder {

        private String index;

        private Settings settings = ImmutableSettings.Builder.EMPTY_SETTINGS;

        private MapBuilder<String, String> mappings = MapBuilder.newMapBuilder();

        public Builder(String index) {
            this.index = index;
        }

        public Builder(IndexMetaData indexMetaData) {
            this(indexMetaData.index());
            settings(indexMetaData.settings());
            mappings.putAll(indexMetaData.mappings);
        }

        public String index() {
            return index;
        }

        public Builder numberOfShards(int numberOfShards) {
            settings = ImmutableSettings.settingsBuilder().putAll(settings).putInt(SETTING_NUMBER_OF_SHARDS, numberOfShards).build();
            return this;
        }

        public int numberOfShards() {
            return settings.getAsInt(SETTING_NUMBER_OF_SHARDS, -1);
        }

        public Builder numberOfReplicas(int numberOfReplicas) {
            settings = ImmutableSettings.settingsBuilder().putAll(settings).putInt(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build();
            return this;
        }

        public int numberOfReplicas() {
            return settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, -1);
        }

        public Builder settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder removeMapping(String mappingType) {
            mappings.remove(mappingType);
            return this;
        }

        public Builder addMapping(String mappingType, String mappingSource) {
            mappings.put(mappingType, mappingSource);
            return this;
        }

        public IndexMetaData build() {
            return new IndexMetaData(index, settings, mappings.immutableMap());
        }

        public static IndexMetaData readFrom(DataInput in, Settings globalSettings) throws ClassNotFoundException, IOException {
            Builder builder = new Builder(in.readUTF());
            builder.settings(readSettingsFromStream(in, globalSettings));
            int mappingsSize = in.readInt();
            for (int i = 0; i < mappingsSize; i++) {
                builder.addMapping(in.readUTF(), in.readUTF());
            }
            return builder.build();
        }

        public static void writeTo(IndexMetaData indexMetaData, DataOutput out) throws IOException {
            out.writeUTF(indexMetaData.index());
            writeSettingsToStream(indexMetaData.settings(), out);
            out.writeInt(indexMetaData.mappings().size());
            for (Map.Entry<String, String> entry : indexMetaData.mappings().entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
        }
    }
}

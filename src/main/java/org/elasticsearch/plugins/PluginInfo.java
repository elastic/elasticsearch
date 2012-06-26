/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.plugins;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class PluginInfo implements Streamable, Serializable, ToXContent {

    // plugin info contains name and description of all plugins installed
    private ImmutableMap<String, String> pluginInfo;

    PluginInfo() {
    }
    
    public PluginInfo(ImmutableCollection<Plugin> plugins) {
        Map<String, String> pluginInfo = Maps.newHashMap();
        for (Plugin plugin: plugins) {
            pluginInfo.put(plugin.name(), plugin.description());
        }
        this.pluginInfo = ImmutableMap.copyOf(pluginInfo);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("plugins");
        for (Map.Entry<String,String> e : pluginInfo.entrySet()) {
            builder.field(e.getKey(), e.getValue());
        }
        builder.endObject();
        return builder;
    }
    
    public static PluginInfo readPluginInfo(StreamInput in) throws IOException {
        PluginInfo info = new PluginInfo();
        info.readFrom(in);
        return info;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        Map<String, String> plugins = Maps.newHashMap();
        int n = in.readVInt();
        for (int i = 0; i < n; i++) {
            String name = in.readUTF();
            String description = in.readUTF();
            plugins.put(name, description);
        }
        this.pluginInfo = ImmutableMap.copyOf(plugins);
    }
    
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(pluginInfo.size());
        for (Map.Entry<String,String> e : pluginInfo.entrySet()) {
            out.writeUTF(e.getKey());
            out.writeUTF(e.getValue());
        }
    }
}

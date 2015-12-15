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

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.PluginInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Information about plugins and modules
 */
public class PluginsAndModules implements Streamable, ToXContent {
    private List<PluginInfo> plugins;
    private List<PluginInfo> modules;

    public PluginsAndModules() {
        plugins = new ArrayList<>();
        modules = new ArrayList<>();
    }

    /**
     * Returns an ordered list based on plugins name
     */
    public List<PluginInfo> getPluginInfos() {
        List<PluginInfo> plugins = new ArrayList<>(this.plugins);
        Collections.sort(plugins, new Comparator<PluginInfo>() {
            @Override
            public int compare(PluginInfo p1, PluginInfo p2) {
                return p1.getName().compareTo(p2.getName());
            }
        });
        return plugins;
    }

    /**
     * Returns an ordered list based on modules name
     */
    public List<PluginInfo> getModuleInfos() {
        List<PluginInfo> modules = new ArrayList<>(this.modules);
        Collections.sort(modules, new Comparator<PluginInfo>() {
            @Override
            public int compare(PluginInfo p1, PluginInfo p2) {
                return p1.getName().compareTo(p2.getName());
            }
        });
        return modules;
    }

    public void addPlugin(PluginInfo info) {
        plugins.add(info);
    }

    public void addModule(PluginInfo info) {
        modules.add(info);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (plugins.isEmpty() == false || modules.isEmpty() == false) {
            throw new IllegalStateException("instance is already populated");
        }
        int plugins_size = in.readInt();
        for (int i = 0; i < plugins_size; i++) {
            plugins.add(PluginInfo.readFromStream(in));
        }
        if (in.getVersion().onOrAfter(Version.V_2_2_0)) {
            int modules_size = in.readInt();
            for (int i = 0; i < modules_size; i++) {
                modules.add(PluginInfo.readFromStream(in));
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_2_2_0)) {
            out.writeInt(plugins.size() + modules.size());
            for (PluginInfo plugin : getPluginInfos()) {
                plugin.writeTo(out);
            }
            for (PluginInfo module : getModuleInfos()) {
                module.writeTo(out);
            }
        }
        else {
            out.writeInt(plugins.size());
            for (PluginInfo plugin : getPluginInfos()) {
                plugin.writeTo(out);
            }
            out.writeInt(modules.size());
            for (PluginInfo module : getModuleInfos()) {
                module.writeTo(out);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("plugins");
        for (PluginInfo pluginInfo : getPluginInfos()) {
            pluginInfo.toXContent(builder, params);
        }
        builder.endArray();
        // TODO: not ideal, make a better api for this (e.g. with jar metadata, and so on)
        builder.startArray("modules");
        for (PluginInfo moduleInfo : getModuleInfos()) {
            moduleInfo.toXContent(builder, params);
        }
        builder.endArray();

        return builder;
    }
}

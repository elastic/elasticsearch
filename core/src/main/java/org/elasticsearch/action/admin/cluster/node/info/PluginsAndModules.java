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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.PluginInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Information about plugins and modules
 */
public class PluginsAndModules implements Writeable, ToXContentFragment {
    private final List<PluginInfo> plugins;
    private final List<PluginInfo> modules;

    public PluginsAndModules(List<PluginInfo> plugins, List<PluginInfo> modules) {
        this.plugins = Collections.unmodifiableList(plugins);
        this.modules = Collections.unmodifiableList(modules);
    }

    public PluginsAndModules(StreamInput in) throws IOException {
        this.plugins = Collections.unmodifiableList(in.readList(PluginInfo::new));
        this.modules = Collections.unmodifiableList(in.readList(PluginInfo::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(plugins);
        out.writeList(modules);
    }

    /**
     * Returns an ordered list based on plugins name
     */
    public List<PluginInfo> getPluginInfos() {
        List<PluginInfo> plugins = new ArrayList<>(this.plugins);
        Collections.sort(plugins, (p1, p2) -> p1.getName().compareTo(p2.getName()));
        return plugins;
    }
    
    /**
     * Returns an ordered list based on modules name
     */
    public List<PluginInfo> getModuleInfos() {
        List<PluginInfo> modules = new ArrayList<>(this.modules);
        Collections.sort(modules, (p1, p2) -> p1.getName().compareTo(p2.getName()));
        return modules;
    }

    public void addPlugin(PluginInfo info) {
        plugins.add(info);
    }
    
    public void addModule(PluginInfo info) {
        modules.add(info);
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

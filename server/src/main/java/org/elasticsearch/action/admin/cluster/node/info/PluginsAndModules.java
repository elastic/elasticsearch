/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.plugins.PluginRuntimeInfo;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Information about plugins and modules
 */
public class PluginsAndModules implements ReportingService.Info {
    private final List<PluginRuntimeInfo> plugins;
    private final List<PluginDescriptor> modules;

    public PluginsAndModules(List<PluginRuntimeInfo> plugins, List<PluginDescriptor> modules) {
        this.plugins = Collections.unmodifiableList(plugins);
        this.modules = Collections.unmodifiableList(modules);
    }

    public PluginsAndModules(StreamInput in) throws IOException {
        this.plugins = in.readImmutableList(PluginRuntimeInfo::new);
        this.modules = in.readImmutableList(PluginDescriptor::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_3_0)) {
            out.writeList(plugins);
        } else {
            out.writeList(plugins.stream().map(PluginRuntimeInfo::descriptor).toList());
        }
        out.writeList(modules);
    }

    /**
     * Returns an ordered list based on plugins name
     */
    public List<PluginRuntimeInfo> getPluginInfos() {
        return plugins.stream().sorted(Comparator.comparing(p -> p.descriptor().getName())).toList();
    }

    /**
     * Returns an ordered list based on modules name
     */
    public List<PluginDescriptor> getModuleInfos() {
        List<PluginDescriptor> modules = new ArrayList<>(this.modules);
        Collections.sort(modules, Comparator.comparing(PluginDescriptor::getName));
        return modules;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("plugins");
        for (PluginRuntimeInfo pluginInfo : plugins) {
            pluginInfo.toXContent(builder, params);
        }
        builder.endArray();
        // TODO: not ideal, make a better api for this (e.g. with jar metadata, and so on)
        builder.startArray("modules");
        for (PluginDescriptor moduleInfo : getModuleInfos()) {
            moduleInfo.toXContent(builder, params);
        }
        builder.endArray();

        return builder;
    }
}

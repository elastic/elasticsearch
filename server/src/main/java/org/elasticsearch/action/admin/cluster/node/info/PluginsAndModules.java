/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.plugins.PluginDescriptor;
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
    private final List<PluginDescriptor> plugins;
    private final List<PluginDescriptor> modules;

    public PluginsAndModules(List<PluginDescriptor> plugins, List<PluginDescriptor> modules) {
        this.plugins = Collections.unmodifiableList(plugins);
        this.modules = Collections.unmodifiableList(modules);
    }

    public PluginsAndModules(StreamInput in) throws IOException {
        this.plugins = Collections.unmodifiableList(in.readList(PluginDescriptor::new));
        this.modules = Collections.unmodifiableList(in.readList(PluginDescriptor::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(plugins);
        out.writeList(modules);
    }

    /**
     * Returns an ordered list based on plugins name
     */
    public List<PluginDescriptor> getPluginInfos() {
        List<PluginDescriptor> plugins = new ArrayList<>(this.plugins);
        Collections.sort(plugins, Comparator.comparing(PluginDescriptor::getName));
        return plugins;
    }

    /**
     * Returns an ordered list based on modules name
     */
    public List<PluginDescriptor> getModuleInfos() {
        List<PluginDescriptor> modules = new ArrayList<>(this.modules);
        Collections.sort(modules, Comparator.comparing(PluginDescriptor::getName));
        return modules;
    }

    public void addPlugin(PluginDescriptor info) {
        plugins.add(info);
    }

    public void addModule(PluginDescriptor info) {
        modules.add(info);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("plugins");
        for (PluginDescriptor pluginDescriptor : getPluginInfos()) {
            pluginDescriptor.toXContent(builder, params);
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

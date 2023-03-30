/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Runtime information about a plugin that was loaded.
 *
 * @param descriptor Static information about the plugin
 * @param isOfficial {@code true} if the plugin is an official plugin, or {@code false} otherwise.
 *                   For nodes before 8.3.0 this is {@code null}.
 * @param pluginApiInfo Runtime information about a custom plugin
 */
public record PluginRuntimeInfo(PluginDescriptor descriptor, @Nullable Boolean isOfficial, @Nullable PluginApiInfo pluginApiInfo)
    implements
        Writeable,
        ToXContentObject {

    public PluginRuntimeInfo(PluginDescriptor descriptor) {
        this(descriptor, false, null);
    }

    public PluginRuntimeInfo(StreamInput in) throws IOException {
        this(new PluginDescriptor(in), readIsOfficial(in), readApiInfo(in));
    }

    private static Boolean readIsOfficial(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_3_0)) {
            return in.readBoolean();
        } else {
            return null;
        }
    }

    private static PluginApiInfo readApiInfo(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_3_0)) {
            return in.readOptionalWriteable(PluginApiInfo::new);
        } else {
            return null;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        descriptor.toXContentFragment(builder, params);
        if (isOfficial != null) {
            builder.field("is_official", isOfficial);
        }
        if (pluginApiInfo != null) {
            pluginApiInfo.toXContent(builder, params);
        }
        builder.endObject();
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        descriptor.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_3_0)) {
            out.writeBoolean(isOfficial);
            out.writeOptionalWriteable(pluginApiInfo);
        }
    }
}

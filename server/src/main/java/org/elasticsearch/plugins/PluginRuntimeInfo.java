/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.Version;
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
 * @param pluginApiInfo Runtime information about a non-official lugin
 */
public record PluginRuntimeInfo(PluginDescriptor descriptor, @Nullable PluginApiInfo pluginApiInfo) implements Writeable, ToXContentObject {

    public PluginRuntimeInfo(PluginDescriptor descriptor) {
        this(descriptor, null);
    }

    public PluginRuntimeInfo(StreamInput in) throws IOException {
        this(new PluginDescriptor(in), readApiInfo(in));
    }

    private static PluginApiInfo readApiInfo(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_8_3_0)) {
            return in.readOptionalWriteable(PluginApiInfo::new);
        } else {
            return null;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        descriptor.toXContentFragment(builder, params);
        if (pluginApiInfo != null) {
            pluginApiInfo.toXContent(builder, params);
        }
        builder.endObject();
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        descriptor.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_8_3_0)) {
            out.writeOptionalWriteable(pluginApiInfo);
        }
    }
}

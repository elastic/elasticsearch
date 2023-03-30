/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Information about APIs extended by a custom plugin.
 *
 * @param legacyInterfaces Plugin API interfaces that the plugin implemented, introspected at runtime.
 * @param legacyMethods Method names overriden from the {@link Plugin} class and Plugin API interfaces
 */
public record PluginApiInfo(List<String> legacyInterfaces, List<String> legacyMethods) implements Writeable, ToXContentFragment {

    public PluginApiInfo(StreamInput in) throws IOException {
        this(in.readImmutableList(StreamInput::readString), in.readImmutableList(StreamInput::readString));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("legacy_interfaces", legacyInterfaces);
        builder.field("legacy_methods", legacyMethods);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(legacyInterfaces);
        out.writeStringCollection(legacyMethods);
    }
}

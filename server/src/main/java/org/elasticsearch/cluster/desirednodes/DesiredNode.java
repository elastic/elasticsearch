/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.desirednodes;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public record DesiredNode(Settings settings, int processors, ByteSizeValue memory, ByteSizeValue storage, Version version)
    implements
        Writeable,
        ToXContentObject {

    private static final ParseField SETTINGS_FIELD = new ParseField("settings");
    private static final ParseField PROCESSORS_FIELD = new ParseField("processors");
    private static final ParseField MEMORY_FIELD = new ParseField("memory");
    private static final ParseField STORAGE_FIELD = new ParseField("storage");
    private static final ParseField VERSION_FIELD = new ParseField("version");

    public DesiredNode(StreamInput in) throws IOException {
        this(Settings.readSettingsFromStream(in), in.readInt(), new ByteSizeValue(in), new ByteSizeValue(in), Version.readVersion(in));
    }

    public static final ConstructingObjectParser<DesiredNode, String> PARSER = new ConstructingObjectParser<>(
        "desired_node",
        false,
        (args, name) -> new DesiredNode(
            (Settings) args[0],
            (int) args[1],
            (ByteSizeValue) args[2],
            (ByteSizeValue) args[3],
            (Version) args[4]
        )
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Settings.fromXContent(p), SETTINGS_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), PROCESSORS_FIELD);
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MEMORY_FIELD.getPreferredName()),
            MEMORY_FIELD,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), STORAGE_FIELD.getPreferredName()),
            STORAGE_FIELD,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> Version.fromString(p.text()),
            VERSION_FIELD,
            ObjectParser.ValueType.STRING
        );
    }

    public static DesiredNode fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SETTINGS_FIELD.getPreferredName(), settings);
        builder.field(PROCESSORS_FIELD.getPreferredName(), processors);
        builder.field(MEMORY_FIELD.getPreferredName(), memory);
        builder.field(STORAGE_FIELD.getPreferredName(), storage);
        builder.field(VERSION_FIELD.getPreferredName(), version);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Settings.writeSettingsToStream(settings, out);
        out.writeInt(processors);
        memory.writeTo(out);
        storage.writeTo(out);
        Version.writeVersion(version, out);
    }
}

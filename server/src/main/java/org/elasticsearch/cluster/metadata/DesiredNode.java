/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
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
import java.util.Collections;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static java.lang.String.format;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;

public final class DesiredNode implements Writeable, ToXContentObject, Comparable<DesiredNode> {

    private static final ParseField SETTINGS_FIELD = new ParseField("settings");
    private static final ParseField PROCESSORS_FIELD = new ParseField("processors");
    private static final ParseField MEMORY_FIELD = new ParseField("memory");
    private static final ParseField STORAGE_FIELD = new ParseField("storage");
    private static final ParseField VERSION_FIELD = new ParseField("node_version");

    public static final ConstructingObjectParser<DesiredNode, String> PARSER = new ConstructingObjectParser<>(
        "desired_node",
        false,
        (args, name) -> new DesiredNode(
            (Settings) args[0],
            (Processors) args[1],
            (ByteSizeValue) args[2],
            (ByteSizeValue) args[3],
            (Version) args[4]
        )
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Settings.fromXContent(p), SETTINGS_FIELD);
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> Processors.fromXContent(p),
            PROCESSORS_FIELD,
            ObjectParser.ValueType.OBJECT_OR_NUMBER
        );
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
            (p, c) -> parseVersion(p.text()),
            VERSION_FIELD,
            ObjectParser.ValueType.STRING
        );
    }

    private static Version parseVersion(String version) {
        if (version == null || version.isBlank()) {
            throw new IllegalArgumentException(VERSION_FIELD.getPreferredName() + " must not be empty");
        }
        return Version.fromString(version);
    }

    private final Settings settings;
    private final Processors processors;
    private final ByteSizeValue memory;
    private final ByteSizeValue storage;
    private final Version version;
    private final String externalId;
    private final Set<DiscoveryNodeRole> roles;

    public DesiredNode(Settings settings, int processors, ByteSizeValue memory, ByteSizeValue storage, Version version) {
        this(settings, new Processors(processors), memory, storage, version);
    }

    public DesiredNode(Settings settings, Processors processors, ByteSizeValue memory, ByteSizeValue storage, Version version) {
        assert settings != null;
        assert processors != null;
        assert memory != null;
        assert storage != null;
        assert version != null;

        if (NODE_EXTERNAL_ID_SETTING.get(settings).isBlank()) {
            throw new IllegalArgumentException(
                format(Locale.ROOT, "[%s] or [%s] is missing or empty", NODE_NAME_SETTING.getKey(), NODE_EXTERNAL_ID_SETTING.getKey())
            );
        }

        this.settings = settings;
        this.processors = processors;
        this.memory = memory;
        this.storage = storage;
        this.version = version;
        this.externalId = NODE_EXTERNAL_ID_SETTING.get(settings);
        this.roles = Collections.unmodifiableSortedSet(new TreeSet<>(DiscoveryNode.getRolesFromSettings(settings)));
    }

    public DesiredNode(StreamInput in) throws IOException {
        this(
            Settings.readSettingsFromStream(in),
            Processors.readFrom(in),
            new ByteSizeValue(in),
            new ByteSizeValue(in),
            Version.readVersion(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Settings.writeSettingsToStream(settings, out);
        processors.writeTo(out);
        memory.writeTo(out);
        storage.writeTo(out);
        Version.writeVersion(version, out);
    }

    public static DesiredNode fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(SETTINGS_FIELD.getPreferredName());
        settings.toXContent(builder, params);
        builder.endObject();
        builder.field(PROCESSORS_FIELD.getPreferredName(), processors);
        builder.field(MEMORY_FIELD.getPreferredName(), memory);
        builder.field(STORAGE_FIELD.getPreferredName(), storage);
        builder.field(VERSION_FIELD.getPreferredName(), version);
        builder.endObject();
        return builder;
    }

    public boolean hasMasterRole() {
        return NODE_ROLES_SETTING.get(settings).contains(DiscoveryNodeRole.MASTER_ROLE);
    }

    public Settings settings() {
        return settings;
    }

    public float minProcessors() {
        return processors.min();
    }

    public int roundedMinProcessors() {
        return Math.max(1, processors.roundedMin());
    }

    public float maxProcessors() {
        return processors.max();
    }

    public int roundedMaxProcessors() {
        return Math.max(1, processors.roundedMax());
    }

    public ByteSizeValue memory() {
        return memory;
    }

    public ByteSizeValue storage() {
        return storage;
    }

    public Version version() {
        return version;
    }

    public String externalId() {
        return externalId;
    }

    public Set<DiscoveryNodeRole> getRoles() {
        return roles;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (DesiredNode) obj;
        // Note that we might consider a DesiredNode different if the order
        // in some settings is different, i.e. we convert node roles to a set in this class,
        // so it can be confusing if we compare two DesiredNode instances that only differ
        // in the node.roles setting order, but that's the semantics provided by the Settings class.
        return Objects.equals(this.settings, that.settings)
            && Objects.equals(this.processors, that.processors)
            && Objects.equals(this.memory, that.memory)
            && Objects.equals(this.storage, that.storage)
            && Objects.equals(this.version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(settings, processors, memory, storage, version);
    }

    @Override
    public int compareTo(DesiredNode o) {
        return externalId.compareTo(o.externalId);
    }

    @Override
    public String toString() {
        return "DesiredNode["
            + "settings="
            + settings
            + ", "
            + "processors="
            + processors
            + ", "
            + "memory="
            + memory
            + ", "
            + "storage="
            + storage
            + ", "
            + "version="
            + version
            + ']';
    }

    public record Processors(float min, float max) implements Writeable, ToXContentObject {

        private static final Version MIN_MAX_PROCESSORS_VERSION = Version.V_8_3_0;

        private static final ParseField MIN_FIELD = new ParseField("min");
        private static final ParseField MAX_FIELD = new ParseField("max");

        public static final ConstructingObjectParser<Processors, String> PROCESSORS_PARSER = new ConstructingObjectParser<>(
            "processors",
            false,
            (args, name) -> new Processors((float) args[0], (float) args[1])
        );

        static {
            PROCESSORS_PARSER.declareFloat(ConstructingObjectParser.constructorArg(), MIN_FIELD);
            PROCESSORS_PARSER.declareFloat(ConstructingObjectParser.constructorArg(), MAX_FIELD);
        }

        public Processors(int processors) {
            this((float) processors, (float) processors);
        }

        static Processors fromXContent(XContentParser parser) throws IOException {
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                return PROCESSORS_PARSER.parse(parser, null);
            } else {
                // For BWC with nodes pre 8.3
                float processors = parser.floatValue();
                return new Processors(processors, processors);
            }
        }

        public Processors {
            if (min > max) {
                throw new IllegalArgumentException(
                    "min processors must be less than or equal to max processors and it was: min: " + min + " max: " + max
                );
            }

            if (min <= 0) {
                throw new IllegalArgumentException("min processors must be greater than 0, but got " + min);
            }

            if (max <= 0) {
                throw new IllegalArgumentException("min processors must be greater than 0, but got " + max);
            }
        }

        private static Processors readFrom(StreamInput in) throws IOException {
            if (in.getVersion().onOrAfter(MIN_MAX_PROCESSORS_VERSION)) {
                return new Processors(in.readFloat(), in.readFloat());
            } else {
                int processors = in.readInt();
                return new Processors((float) processors, (float) processors);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().onOrAfter(MIN_MAX_PROCESSORS_VERSION)) {
                out.writeFloat(min);
                out.writeFloat(max);
            } else {
                out.writeInt(roundedMin());
            }
        }

        public int roundedMin() {
            return Math.round(min);
        }

        public int roundedMax() {
            return Math.round(max);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MIN_FIELD.getPreferredName(), min);
            builder.field(MAX_FIELD.getPreferredName(), max);
            builder.endObject();
            return builder;
        }
    }
}

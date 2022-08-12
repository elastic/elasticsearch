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
import org.elasticsearch.core.Nullable;
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
    public static final Version RANGE_FLOAT_PROCESSORS_SUPPORT_VERSION = Version.V_8_3_0;

    private static final ParseField SETTINGS_FIELD = new ParseField("settings");
    private static final ParseField PROCESSORS_FIELD = new ParseField("processors");
    private static final ParseField PROCESSORS_RANGE_FIELD = new ParseField("processors_range");
    private static final ParseField MEMORY_FIELD = new ParseField("memory");
    private static final ParseField STORAGE_FIELD = new ParseField("storage");
    private static final ParseField VERSION_FIELD = new ParseField("node_version");

    public static final ConstructingObjectParser<DesiredNode, Void> PARSER = new ConstructingObjectParser<>(
        "desired_node",
        false,
        (args, name) -> new DesiredNode(
            (Settings) args[0],
            (Float) args[1],
            (ProcessorsRange) args[2],
            (ByteSizeValue) args[3],
            (ByteSizeValue) args[4],
            (Version) args[5]
        )
    );

    static {
        configureParser(PARSER);
    }

    static <T> void configureParser(ConstructingObjectParser<T, Void> parser) {
        parser.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Settings.fromXContent(p), SETTINGS_FIELD);
        parser.declareFloat(ConstructingObjectParser.optionalConstructorArg(), PROCESSORS_FIELD);
        parser.declareObjectOrNull(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ProcessorsRange.fromXContent(p),
            null,
            PROCESSORS_RANGE_FIELD
        );
        parser.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MEMORY_FIELD.getPreferredName()),
            MEMORY_FIELD,
            ObjectParser.ValueType.STRING
        );
        parser.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), STORAGE_FIELD.getPreferredName()),
            STORAGE_FIELD,
            ObjectParser.ValueType.STRING
        );
        parser.declareField(
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
    private final Float processors;
    private final ProcessorsRange processorsRange;
    private final ByteSizeValue memory;
    private final ByteSizeValue storage;
    private final Version version;
    private final String externalId;
    private final Set<DiscoveryNodeRole> roles;

    public DesiredNode(Settings settings, ProcessorsRange processorsRange, ByteSizeValue memory, ByteSizeValue storage, Version version) {
        this(settings, null, processorsRange, memory, storage, version);
    }

    public DesiredNode(Settings settings, float processors, ByteSizeValue memory, ByteSizeValue storage, Version version) {
        this(settings, processors, null, memory, storage, version);
    }

    DesiredNode(
        Settings settings,
        Float processors,
        ProcessorsRange processorsRange,
        ByteSizeValue memory,
        ByteSizeValue storage,
        Version version
    ) {
        assert settings != null;
        assert memory != null;
        assert storage != null;
        assert version != null;

        if (processors == null && processorsRange == null) {
            throw new IllegalArgumentException(
                PROCESSORS_FIELD.getPreferredName()
                    + " or "
                    + PROCESSORS_RANGE_FIELD.getPreferredName()
                    + " should be specified and none was specified"
            );
        }

        if (processors != null && processorsRange != null) {
            throw new IllegalArgumentException(
                PROCESSORS_FIELD.getPreferredName()
                    + " and "
                    + PROCESSORS_RANGE_FIELD.getPreferredName()
                    + " were specified, but only one should be specified"
            );
        }

        if (processors != null && invalidNumberOfProcessors(processors)) {
            throw new IllegalArgumentException(
                format(Locale.ROOT, "Only a positive number of [processors] are allowed and [%f] was provided", processors)
            );
        }

        if (NODE_EXTERNAL_ID_SETTING.get(settings).isBlank()) {
            throw new IllegalArgumentException(
                format(Locale.ROOT, "[%s] or [%s] is missing or empty", NODE_NAME_SETTING.getKey(), NODE_EXTERNAL_ID_SETTING.getKey())
            );
        }

        this.settings = settings;
        this.processors = processors;
        this.processorsRange = processorsRange;
        this.memory = memory;
        this.storage = storage;
        this.version = version;
        this.externalId = NODE_EXTERNAL_ID_SETTING.get(settings);
        this.roles = Collections.unmodifiableSortedSet(new TreeSet<>(DiscoveryNode.getRolesFromSettings(settings)));
    }

    public static DesiredNode readFrom(StreamInput in) throws IOException {
        final var settings = Settings.readSettingsFromStream(in);
        final Float processors;
        final ProcessorsRange processorsRange;
        if (in.getVersion().onOrAfter(RANGE_FLOAT_PROCESSORS_SUPPORT_VERSION)) {
            processors = in.readOptionalFloat();
            processorsRange = in.readOptionalWriteable(ProcessorsRange::readFrom);
        } else {
            processors = (float) in.readInt();
            processorsRange = null;
        }
        final var memory = new ByteSizeValue(in);
        final var storage = new ByteSizeValue(in);
        final var version = Version.readVersion(in);
        return new DesiredNode(settings, processors, processorsRange, memory, storage, version);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        settings.writeTo(out);
        if (out.getVersion().onOrAfter(RANGE_FLOAT_PROCESSORS_SUPPORT_VERSION)) {
            out.writeOptionalFloat(processors);
            out.writeOptionalWriteable(processorsRange);
        } else {
            assert processorsRange == null;
            assert processors != null;
            assert processorHasDecimals() == false;
            out.writeInt((int) (float) processors);
        }
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
        toInnerXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public void toInnerXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(SETTINGS_FIELD.getPreferredName());
        settings.toXContent(builder, params);
        builder.endObject();
        if (processors != null) {
            builder.field(PROCESSORS_FIELD.getPreferredName(), processors);
        }
        if (processorsRange != null) {
            builder.field(PROCESSORS_RANGE_FIELD.getPreferredName(), processorsRange);
        }
        builder.field(MEMORY_FIELD.getPreferredName(), memory);
        builder.field(STORAGE_FIELD.getPreferredName(), storage);
        builder.field(VERSION_FIELD.getPreferredName(), version);
    }

    public boolean hasMasterRole() {
        return NODE_ROLES_SETTING.get(settings).contains(DiscoveryNodeRole.MASTER_ROLE);
    }

    public Settings settings() {
        return settings;
    }

    public float minProcessors() {
        if (processors != null) {
            return processors;
        }
        return processorsRange.min();
    }

    public int roundedDownMinProcessors() {
        return roundDown(minProcessors());
    }

    public Float maxProcessors() {
        if (processors != null) {
            return processors;
        }

        return processorsRange.max();
    }

    public Integer roundedUpMaxProcessors() {
        if (maxProcessors() == null) {
            return null;
        }

        return roundUp(maxProcessors());
    }

    private boolean processorHasDecimals() {
        return processors != null && ((int) (float) processors) != Math.ceil(processors);
    }

    @Nullable
    Float processors() {
        return processors;
    }

    @Nullable
    ProcessorsRange processorsRange() {
        return processorsRange;
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

    public boolean isCompatibleWithVersion(Version version) {
        if (version.onOrAfter(RANGE_FLOAT_PROCESSORS_SUPPORT_VERSION)) {
            return true;
        }
        return processorsRange == null && processorHasDecimals() == false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DesiredNode that = (DesiredNode) o;
        return Objects.equals(settings, that.settings)
            && Objects.equals(processors, that.processors)
            && Objects.equals(processorsRange, that.processorsRange)
            && Objects.equals(memory, that.memory)
            && Objects.equals(storage, that.storage)
            && Objects.equals(version, that.version)
            && Objects.equals(externalId, that.externalId)
            && Objects.equals(roles, that.roles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(settings, processors, processorsRange, memory, storage, version, externalId, roles);
    }

    @Override
    public int compareTo(DesiredNode o) {
        return externalId.compareTo(o.externalId);
    }

    @Override
    public String toString() {
        return "DesiredNode{"
            + "settings="
            + settings
            + ", processors="
            + processors
            + ", processorsRange="
            + processorsRange
            + ", memory="
            + memory
            + ", storage="
            + storage
            + ", version="
            + version
            + ", externalId='"
            + externalId
            + '\''
            + ", roles="
            + roles
            + '}';
    }

    private static boolean invalidNumberOfProcessors(float processors) {
        return processors <= 0 || Float.isInfinite(processors) || Float.isNaN(processors);
    }

    private static int roundUp(float value) {
        return (int) Math.ceil(value);
    }

    private static int roundDown(float value) {
        return Math.max(1, (int) Math.floor(value));
    }

    public record ProcessorsRange(float min, Float max) implements Writeable, ToXContentObject {

        private static final ParseField MIN_FIELD = new ParseField("min");
        private static final ParseField MAX_FIELD = new ParseField("max");

        public static final ConstructingObjectParser<ProcessorsRange, String> PROCESSORS_PARSER = new ConstructingObjectParser<>(
            "processors",
            false,
            (args, name) -> new ProcessorsRange((float) args[0], (Float) args[1])
        );

        static {
            PROCESSORS_PARSER.declareFloat(ConstructingObjectParser.constructorArg(), MIN_FIELD);
            PROCESSORS_PARSER.declareFloat(ConstructingObjectParser.optionalConstructorArg(), MAX_FIELD);
        }

        static ProcessorsRange fromXContent(XContentParser parser) throws IOException {
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                return PROCESSORS_PARSER.parse(parser, null);
            } else {
                // For BWC with nodes pre 8.3
                float processors = parser.floatValue();
                return new ProcessorsRange(processors, processors);
            }
        }

        public ProcessorsRange {
            if (invalidNumberOfProcessors(min)) {
                throw new IllegalArgumentException(
                    format(
                        Locale.ROOT,
                        "Only a positive number of [%s] processors are allowed and [%f] was provided",
                        MIN_FIELD.getPreferredName(),
                        min
                    )
                );
            }

            if (max != null && invalidNumberOfProcessors(max)) {
                throw new IllegalArgumentException(
                    format(
                        Locale.ROOT,
                        "Only a positive number of [%s] processors are allowed and [%f] was provided",
                        MAX_FIELD.getPreferredName(),
                        max
                    )
                );
            }

            if (max != null && min > max) {
                throw new IllegalArgumentException(
                    "min processors must be less than or equal to max processors and it was: min: " + min + " max: " + max
                );
            }
        }

        @Nullable
        private static ProcessorsRange readFrom(StreamInput in) throws IOException {
            return new ProcessorsRange(in.readFloat(), in.readOptionalFloat());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeFloat(min);
            out.writeOptionalFloat(max);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MIN_FIELD.getPreferredName(), min);
            if (max != null) {
                builder.field(MAX_FIELD.getPreferredName(), max);
            }
            builder.endObject();
            return builder;
        }
    }
}

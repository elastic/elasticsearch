/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Processors;
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

    public static final TransportVersion RANGE_FLOAT_PROCESSORS_SUPPORT_TRANSPORT_VERSION = TransportVersions.V_8_3_0;

    private static final ParseField SETTINGS_FIELD = new ParseField("settings");
    private static final ParseField PROCESSORS_FIELD = new ParseField("processors");
    private static final ParseField PROCESSORS_RANGE_FIELD = new ParseField("processors_range");
    private static final ParseField MEMORY_FIELD = new ParseField("memory");
    private static final ParseField STORAGE_FIELD = new ParseField("storage");

    public static final ConstructingObjectParser<DesiredNode, Void> PARSER = new ConstructingObjectParser<>(
        "desired_node",
        false,
        (args, name) -> new DesiredNode(
            (Settings) args[0],
            (Processors) args[1],
            (ProcessorsRange) args[2],
            (ByteSizeValue) args[3],
            (ByteSizeValue) args[4]
        )
    );

    static {
        configureParser(PARSER);
    }

    static <T> void configureParser(ConstructingObjectParser<T, Void> parser) {
        parser.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Settings.fromXContent(p), SETTINGS_FIELD);
        parser.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> Processors.fromXContent(p),
            PROCESSORS_FIELD,
            ObjectParser.ValueType.DOUBLE
        );
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
    }

    private final Settings settings;
    private final Processors processors;
    private final ProcessorsRange processorsRange;
    private final ByteSizeValue memory;
    private final ByteSizeValue storage;

    private final String externalId;
    private final Set<DiscoveryNodeRole> roles;

    public DesiredNode(Settings settings, ProcessorsRange processorsRange, ByteSizeValue memory, ByteSizeValue storage) {
        this(settings, null, processorsRange, memory, storage);
    }

    public DesiredNode(Settings settings, double processors, ByteSizeValue memory, ByteSizeValue storage) {
        this(settings, Processors.of(processors), null, memory, storage);
    }

    DesiredNode(Settings settings, Processors processors, ProcessorsRange processorsRange, ByteSizeValue memory, ByteSizeValue storage) {
        assert settings != null;
        assert memory != null;
        assert storage != null;

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
        this.externalId = NODE_EXTERNAL_ID_SETTING.get(settings);
        this.roles = Collections.unmodifiableSortedSet(new TreeSet<>(DiscoveryNode.getRolesFromSettings(settings)));
    }

    public static DesiredNode readFrom(StreamInput in) throws IOException {
        final var settings = Settings.readSettingsFromStream(in);
        final Processors processors;
        final ProcessorsRange processorsRange;
        if (in.getTransportVersion().onOrAfter(RANGE_FLOAT_PROCESSORS_SUPPORT_TRANSPORT_VERSION)) {
            processors = in.readOptionalWriteable(Processors::readFrom);
            processorsRange = in.readOptionalWriteable(ProcessorsRange::readFrom);
        } else {
            processors = Processors.readFrom(in);
            processorsRange = null;
        }
        final var memory = ByteSizeValue.readFrom(in);
        final var storage = ByteSizeValue.readFrom(in);
        if (in.getTransportVersion().before(TransportVersions.REMOVE_DESIRED_NODE_VERSION)
            && in.getTransportVersion().isPatchFrom(TransportVersions.V_9_0_0) == false) {
            in.readOptionalString();
        }
        return new DesiredNode(settings, processors, processorsRange, memory, storage);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        settings.writeTo(out);
        if (out.getTransportVersion().onOrAfter(RANGE_FLOAT_PROCESSORS_SUPPORT_TRANSPORT_VERSION)) {
            out.writeOptionalWriteable(processors);
            out.writeOptionalWriteable(processorsRange);
        } else {
            assert processorsRange == null;
            assert processors != null;
            processors.writeTo(out);
        }
        memory.writeTo(out);
        storage.writeTo(out);
        if (out.getTransportVersion().before(TransportVersions.REMOVE_DESIRED_NODE_VERSION)
            && out.getTransportVersion().isPatchFrom(TransportVersions.V_9_0_0) == false) {
            out.writeOptionalString(null);
        }
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
    }

    public boolean hasMasterRole() {
        return NODE_ROLES_SETTING.get(settings).contains(DiscoveryNodeRole.MASTER_ROLE);
    }

    public Settings settings() {
        return settings;
    }

    public Processors minProcessors() {
        if (processors != null) {
            return processors;
        }
        return processorsRange.min();
    }

    public int roundedDownMinProcessors() {
        return minProcessors().roundDown();
    }

    @Nullable
    public Processors maxProcessors() {
        if (processors != null) {
            return processors;
        }

        return processorsRange.max();
    }

    public Integer roundedUpMaxProcessors() {
        final Processors maxProcessors = maxProcessors();
        if (maxProcessors == null) {
            return null;
        }

        return maxProcessors.roundUp();
    }

    @Nullable
    Processors processors() {
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

    public String externalId() {
        return externalId;
    }

    public Set<DiscoveryNodeRole> getRoles() {
        return roles;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DesiredNode that = (DesiredNode) o;
        return equalsWithoutProcessorsSpecification(that)
            && Objects.equals(processorsRange, that.processorsRange)
            && Objects.equals(processors, that.processors);
    }

    private boolean equalsWithoutProcessorsSpecification(DesiredNode that) {
        return Objects.equals(settings, that.settings)
            && Objects.equals(memory, that.memory)
            && Objects.equals(storage, that.storage)
            && Objects.equals(externalId, that.externalId)
            && Objects.equals(roles, that.roles);
    }

    public boolean equalsWithProcessorsCloseTo(DesiredNode that) {
        return equalsWithoutProcessorsSpecification(that)
            && Processors.equalsOrCloseTo(processors, that.processors)
            && ProcessorsRange.equalsOrCloseTo(processorsRange, that.processorsRange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(settings, processors, processorsRange, memory, storage, externalId, roles);
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
            + ", externalId='"
            + externalId
            + '\''
            + ", roles="
            + roles
            + '}';
    }

    public record ProcessorsRange(Processors min, @Nullable Processors max) implements Writeable, ToXContentObject {

        private static final ParseField MIN_FIELD = new ParseField("min");
        private static final ParseField MAX_FIELD = new ParseField("max");

        public static final ConstructingObjectParser<ProcessorsRange, String> PROCESSORS_RANGE_PARSER = new ConstructingObjectParser<>(
            "processors_range",
            false,
            (args, name) -> new ProcessorsRange((Processors) args[0], (Processors) args[1])
        );

        static {
            PROCESSORS_RANGE_PARSER.declareField(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> Processors.fromXContent(p),
                MIN_FIELD,
                ObjectParser.ValueType.DOUBLE
            );
            PROCESSORS_RANGE_PARSER.declareField(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> Processors.fromXContent(p),
                MAX_FIELD,
                ObjectParser.ValueType.DOUBLE
            );
        }

        static ProcessorsRange fromXContent(XContentParser parser) throws IOException {
            return PROCESSORS_RANGE_PARSER.parse(parser, null);
        }

        public ProcessorsRange(double min, Double max) {
            this(Processors.of(min), Processors.of(max));
        }

        public ProcessorsRange {
            if (max != null && min.compareTo(max) > 0) {
                throw new IllegalArgumentException(
                    "min processors must be less than or equal to max processors and it was: min: " + min + " max: " + max
                );
            }
        }

        private static ProcessorsRange readFrom(StreamInput in) throws IOException {
            return new ProcessorsRange(Processors.readFrom(in), in.readOptionalWriteable(Processors::readFrom));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            min.writeTo(out);
            out.writeOptionalWriteable(max);
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

        static boolean equalsOrCloseTo(ProcessorsRange a, ProcessorsRange b) {
            return (a == b) || (a != null && a.equalsOrCloseTo(b));
        }

        boolean equalsOrCloseTo(ProcessorsRange that) {
            return that != null
                && (equals(that) || (Processors.equalsOrCloseTo(min, that.min) && Processors.equalsOrCloseTo(max, that.max)));
        }
    }
}

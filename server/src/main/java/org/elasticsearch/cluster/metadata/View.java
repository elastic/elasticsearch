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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

/**
 * Represents a single view definition: a name, a query string, and the rights mode under which the view's body is executed.
 * <p>
 * A view runs with either <b>invoker</b> rights (the body is resolved and executed as the user who queries the view — today's
 * behaviour and the default for every existing view) or <b>definer</b> rights (the body would run as the identity that created the
 * view). As of the definer seam, definer-mode views are admitted, stored, and serialized — the definer identity is captured at
 * creation — but they are <b>not yet executed</b>: querying a definer view is rejected at resolution time so the view never silently
 * runs as the invoker (a privilege-confusion footgun). Invoker views are entirely unchanged.
 */
public final class View implements Writeable, ToXContentObject, IndexAbstraction {
    private static final ParseField NAME = new ParseField("name");
    private static final ParseField QUERY = new ParseField("query");
    private static final ParseField RIGHTS_MODE = new ParseField("rights_mode");
    private static final ParseField DEFINER = new ParseField("definer");

    private static final TransportVersion ESQL_VIEW_DEFINER_RIGHTS = TransportVersion.fromName("esql_view_definer_rights");

    /**
     * The rights axis a view's body executes under. {@link #INVOKER} is the default and the behaviour of every view created before
     * the definer seam: the body is resolved and executed as the querying user. {@link #DEFINER} captures the creator's identity at
     * create time and (in a later phase) would execute the body as that identity.
     */
    public enum RightsMode {
        INVOKER,
        DEFINER
    }

    /**
     * The captured identity of the view's definer, stored in an x-pack-free, server-module-safe shape.
     * <p>
     * The role descriptors are kept as an opaque {@link BytesReference} (serialized by the security layer at capture time, in the
     * x-pack module that owns {@code RoleDescriptor}) rather than as a typed list, because {@code RoleDescriptor} lives in
     * {@code x-pack/plugin/core} and is not reachable from the {@code server} module where {@link View} lives. This mirrors how the
     * security layer already ships role descriptors as bytes in API-key authentication headers: the server module transports and
     * persists them opaquely and never interprets them.
     *
     * @param username             the definer's principal username
     * @param realm                the realm the definer authenticated against
     * @param roleDescriptorsBytes the definer's role descriptors, serialized opaquely by the security layer (may be empty)
     */
    public record DefinerInfo(String username, String realm, BytesReference roleDescriptorsBytes) implements Writeable {
        public DefinerInfo {
            Objects.requireNonNull(username, "definer username must not be null");
            Objects.requireNonNull(realm, "definer realm must not be null");
            Objects.requireNonNull(roleDescriptorsBytes, "definer role descriptors must not be null");
        }

        public DefinerInfo(StreamInput in) throws IOException {
            this(in.readString(), in.readString(), in.readBytesReference());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(username);
            out.writeString(realm);
            out.writeBytesReference(roleDescriptorsBytes);
        }
    }

    private static final ParseField DEFINER_USERNAME = new ParseField("username");
    private static final ParseField DEFINER_REALM = new ParseField("realm");
    private static final ParseField DEFINER_ROLE_DESCRIPTORS = new ParseField("role_descriptors");

    // Declared before the view PARSERs because they reference it via declareRightsFields; class-init order matters.
    private static final ConstructingObjectParser<DefinerInfo, Void> DEFINER_PARSER = new ConstructingObjectParser<>(
        "view_definer",
        false,
        args -> new DefinerInfo((String) args[0], (String) args[1], new BytesArray(Base64.getDecoder().decode((String) args[2])))
    );

    static {
        DEFINER_PARSER.declareString(ConstructingObjectParser.constructorArg(), DEFINER_USERNAME);
        DEFINER_PARSER.declareString(ConstructingObjectParser.constructorArg(), DEFINER_REALM);
        DEFINER_PARSER.declareString(ConstructingObjectParser.constructorArg(), DEFINER_ROLE_DESCRIPTORS);
    }

    // Parser that includes the name field (eg. serializing/deserializing the full object)
    static final ConstructingObjectParser<View, Void> PARSER = new ConstructingObjectParser<>(
        "view",
        false,
        (args, ctx) -> buildFromXContent((String) args[0], (String) args[1], (String) args[2], (DefinerInfo) args[3])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), QUERY);
        declareRightsFields(PARSER);
    }

    // Parser that excludes the name field (eg. when the name is provided externally, in the URL path)
    public static ConstructingObjectParser<View, Void> parser(String name) {
        ConstructingObjectParser<View, Void> parser = new ConstructingObjectParser<>(
            "view",
            false,
            (args, ctx) -> buildFromXContent(name, (String) args[0], (String) args[1], (DefinerInfo) args[2])
        );
        parser.declareString(ConstructingObjectParser.constructorArg(), QUERY);
        declareRightsFields(parser);
        return parser;
    }

    private static void declareRightsFields(ConstructingObjectParser<View, Void> parser) {
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), RIGHTS_MODE);
        parser.declareObject(ConstructingObjectParser.optionalConstructorArg(), DEFINER_PARSER, DEFINER);
    }

    private static View buildFromXContent(String name, String query, @Nullable String rightsMode, @Nullable DefinerInfo definer) {
        RightsMode mode = rightsMode == null ? RightsMode.INVOKER : RightsMode.valueOf(rightsMode);
        return new View(name, query, mode, definer);
    }

    private final String name;
    private final String query;
    private final RightsMode rightsMode;
    @Nullable
    private final DefinerInfo definer;

    public View(String name, String query) {
        this(name, query, RightsMode.INVOKER, null);
    }

    public View(String name, String query, RightsMode rightsMode, @Nullable DefinerInfo definer) {
        this.name = Objects.requireNonNull(name, "view name must not be null");
        this.query = Objects.requireNonNull(query, "view query must not be null");
        this.rightsMode = Objects.requireNonNull(rightsMode, "view rights mode must not be null");
        if (rightsMode == RightsMode.INVOKER && definer != null) {
            throw new IllegalArgumentException("invoker-rights view must not carry a definer identity");
        }
        this.definer = definer;
    }

    public View(StreamInput in) throws IOException {
        this.name = in.readString();
        this.query = in.readString();
        if (in.getTransportVersion().supports(ESQL_VIEW_DEFINER_RIGHTS)) {
            this.rightsMode = in.readEnum(RightsMode.class);
            this.definer = in.readOptionalWriteable(DefinerInfo::new);
        } else {
            // Old nodes never sent a rights mode; such a view is, and always was, invoker-rights.
            this.rightsMode = RightsMode.INVOKER;
            this.definer = null;
        }
    }

    public static View fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(query);
        if (out.getTransportVersion().supports(ESQL_VIEW_DEFINER_RIGHTS)) {
            out.writeEnum(rightsMode);
            out.writeOptionalWriteable(definer);
        }
        // Pre-seam nodes only understand invoker views. A definer-mode view is unqueryable until a later phase, so dropping the
        // definer fields here cannot change any execution result on the old node; it would simply read the view as invoker, which
        // is the safe default. The rights axis is re-asserted from cluster state on every node that does support it.
    }

    public String name() {
        return name;
    }

    public String query() {
        return query;
    }

    public RightsMode rightsMode() {
        return rightsMode;
    }

    @Nullable
    public DefinerInfo definer() {
        return definer;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME.getPreferredName(), name);
        builder.field(QUERY.getPreferredName(), query);
        builder.field(RIGHTS_MODE.getPreferredName(), rightsMode.name());
        if (definer != null) {
            builder.startObject(DEFINER.getPreferredName());
            builder.field(DEFINER_USERNAME.getPreferredName(), definer.username());
            builder.field(DEFINER_REALM.getPreferredName(), definer.realm());
            builder.field(
                DEFINER_ROLE_DESCRIPTORS.getPreferredName(),
                Base64.getEncoder().encodeToString(BytesReference.toBytes(definer.roleDescriptorsBytes()))
            );
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        View other = (View) o;
        return Objects.equals(name, other.name)
            && Objects.equals(query, other.query)
            && rightsMode == other.rightsMode
            && Objects.equals(definer, other.definer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, query, rightsMode, definer);
    }

    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public Type getType() {
        return Type.VIEW;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<Index> getIndices() {
        return List.of();
    }

    @Override
    public Index getWriteIndex() {
        return null;
    }

    @Override
    public DataStream getParentDataStream() {
        return null;
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public boolean isSystem() {
        return false;
    }
}

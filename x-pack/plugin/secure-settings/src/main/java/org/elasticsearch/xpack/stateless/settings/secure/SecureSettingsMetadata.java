/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Secure setting values set through {@code PUT _cluster/settings}, each encrypted with the project encryption key.
 * <p>A {@code null} value means the name is still defined but its value was wiped by a destructive encryption key reset.
 * <p>Only persisted to disk: the custom is neither returned by APIs such as {@code GET _cluster/state} nor written to snapshots.
 */
public final class SecureSettingsMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {

    public static final String TYPE = "secure_settings";

    static final TransportVersion SECURE_SETTINGS_METADATA = TransportVersion.fromName("secure_settings_metadata");

    public static final SecureSettingsMetadata EMPTY = new SecureSettingsMetadata(Map.of());

    public static final List<NamedWriteableRegistry.Entry> NAMED_WRITEABLES = List.of(
        new NamedWriteableRegistry.Entry(Metadata.ProjectCustom.class, TYPE, SecureSettingsMetadata::new),
        new NamedWriteableRegistry.Entry(NamedDiff.class, TYPE, SecureSettingsMetadata::readDiffFrom)
    );

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SecureSettingsMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        true,
        args -> new SecureSettingsMetadata((Map<String, EncryptedData>) args[0])
    );

    static {
        PARSER.declareObject(
            constructorArg(),
            (p, c) -> p.map(TreeMap::new, v -> v.currentToken() == XContentParser.Token.VALUE_NULL ? null : EncryptedData.fromXContent(v)),
            new ParseField("settings")
        );
    }

    private final Map<String, EncryptedData> settings;

    public SecureSettingsMetadata(Map<String, EncryptedData> settings) {
        this.settings = Collections.unmodifiableMap(new TreeMap<>(settings));
    }

    public SecureSettingsMetadata(StreamInput in) throws IOException {
        this(in.readMap(StreamInput::readString, i -> i.readOptionalWriteable(EncryptedData::new)));
    }

    public static SecureSettingsMetadata get(ProjectMetadata project) {
        SecureSettingsMetadata metadata = project.custom(TYPE);
        return metadata == null ? EMPTY : metadata;
    }

    /**
     * The encrypted values, keyed by setting name. A {@code null} value was wiped by a destructive reset.
     */
    public Map<String, EncryptedData> settings() {
        return settings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(settings, StreamOutput::writeString, StreamOutput::writeOptionalWriteable);
    }

    public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return SECURE_SETTINGS_METADATA;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return EnumSet.of(Metadata.XContentContext.GATEWAY);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContentHelper.chunk((builder, p) -> {
            builder.startObject("settings");
            for (Map.Entry<String, EncryptedData> entry : settings.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            return builder.endObject();
        });
    }

    public static SecureSettingsMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof SecureSettingsMetadata other && settings.equals(other.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(settings);
    }

    @Override
    public String toString() {
        return "SecureSettingsMetadata{names=" + settings.keySet() + "}";
    }
}

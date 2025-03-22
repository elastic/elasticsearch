/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.secrets;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Secrets that are stored in project state as a {@link Metadata.ProjectCustom}
 *
 * <p>Project state secrets are initially loaded on the master node, from a file on disk.
 * Once the cluster is running, the master node watches the file for changes. This class
 * propagates changes in the file-based secure settings for each project from the master
 * node out to other nodes using the transport protocol.
 *
 * <p>Since the master node should always have settings on disk, we don't need to
 * persist this class to saved cluster state, either on disk or in the cloud. Therefore,
 * we have defined this {@link Metadata.ProjectCustom} as a "private custom" object by not
 * serializing its content in {@link #toXContentChunked(ToXContent.Params)}.
 */
public class ProjectSecrets extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {

    public static final String TYPE = "project_state_secrets";

    private final SecureSettings settings;

    public static final ParseField STRING_SECRETS_FIELD = new ParseField("string_secrets");
    public static final ParseField FILE_SECRETS_FIELD = new ParseField("file_secrets");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Map<String, byte[]>, Void> PARSER = new ConstructingObjectParser<>(
        "project_secrets_parser",
        a -> {
            final var decoder = Base64.getDecoder();

            Map<String, byte[]> stringSecretsMap = a[0] == null
                ? Map.of()
                : ((Map<String, String>) a[0]).entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getBytes(StandardCharsets.UTF_8)));

            Map<String, byte[]> fileSecretsByteMap = a[1] == null
                ? Map.of()
                : ((Map<String, String>) a[1]).entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> decoder.decode(e.getValue())));

            Set<String> duplicateKeys = fileSecretsByteMap.keySet()
                .stream()
                .filter(stringSecretsMap::containsKey)
                .collect(Collectors.toSet());

            if (duplicateKeys.isEmpty() == false) {
                throw new IllegalStateException("Some settings were defined as both string and file settings: " + duplicateKeys);
            }

            return Stream.concat(stringSecretsMap.entrySet().stream(), fileSecretsByteMap.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), STRING_SECRETS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), FILE_SECRETS_FIELD);
    }

    public ProjectSecrets(SecureSettings settings) {
        this.settings = settings;
    }

    public ProjectSecrets(StreamInput in) throws IOException {
        this.settings = new SecureClusterStateSettings(in);
    }

    public SecureSettings getSettings() {
        return settings;
    }

    public static ProjectSecrets fromXContent(XContentParser parser) {
        return new ProjectSecrets(new SecureClusterStateSettings(PARSER.apply(parser, null)));
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        // No need to persist in index or return to user, so do not serialize the secrets
        return Collections.emptyIterator();
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.MULTI_PROJECT;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        settings.writeTo(out);
    }

    public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
    }

    @Override
    public String toString() {
        return "ProjectSecrets{[all secret]}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProjectSecrets that = (ProjectSecrets) o;
        return Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(settings);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return EnumSet.noneOf(Metadata.XContentContext.class);
    }
}

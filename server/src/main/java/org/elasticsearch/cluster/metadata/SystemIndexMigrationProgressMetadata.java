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
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Project-level orchestration checkpoints for upgrading system indices
 * (<a href="https://github.com/elastic/elasticsearch/issues/146662">#146662</a>). Actual per-index checkpoints continue to evolve in
 * follow-up pulls; PR1 introduces the metadata shell only; the persistent-task path remains authoritative until rewired.
 */
public final class SystemIndexMigrationProgressMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom>
    implements
        Metadata.ProjectCustom {

    public static final String TYPE = "system_index_migration_progress";

    private static final TransportVersion INTRODUCTION = TransportVersion.fromName("migration_progress_project_metadata");

    private static final ParseField PHASE_FIELD = new ParseField("phase");
    private static final ParseField GENERATION_FIELD = new ParseField("generation");
    private static final ParseField CURRENT_FEATURE_FIELD = new ParseField("current_feature");

    /** No system index upgrade coordinated via this metadata. */
    public static final SystemIndexMigrationProgressMetadata EMPTY = new SystemIndexMigrationProgressMetadata(Phase.IDLE, 0L, null);

    public enum Phase {
        IDLE,
        RUNNING;

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }

        static Phase parse(String text) {
            return switch (text.toLowerCase(Locale.ROOT)) {
                case "idle" -> IDLE;
                case "running" -> RUNNING;
                default -> throw new IllegalArgumentException("Unexpected system index migration phase [" + text + "]");
            };
        }
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SystemIndexMigrationProgressMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        true,
        args -> {
            Phase phase = args[0] == null ? Phase.IDLE : Phase.parse((String) args[0]);
            long generation = args[1] == null ? 0L : (long) args[1];
            String currentFeature = args[2] == null ? null : (String) args[2];
            return new SystemIndexMigrationProgressMetadata(phase, generation, currentFeature);
        }
    );

    static {
        PARSER.declareString(optionalConstructorArg(), PHASE_FIELD);
        PARSER.declareLong(optionalConstructorArg(), GENERATION_FIELD);
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : p.text(),
            CURRENT_FEATURE_FIELD,
            ValueType.STRING_OR_NULL
        );
    }

    private final Phase phase;
    private final long generation;

    @Nullable
    private final String currentFeature;

    public SystemIndexMigrationProgressMetadata(StreamInput in) throws IOException {
        phase = in.readEnum(Phase.class);
        generation = in.readLong();
        currentFeature = in.readOptionalString();
    }

    public SystemIndexMigrationProgressMetadata(Phase phase, long generation, @Nullable String currentFeature) {
        this.phase = Objects.requireNonNull(phase);
        if (generation < 0) {
            throw new IllegalArgumentException("generation cannot be negative");
        }
        if (phase == Phase.IDLE && generation != 0L) {
            throw new IllegalArgumentException("IDLE phase requires generation zero");
        }
        if (phase == Phase.IDLE && currentFeature != null) {
            throw new IllegalArgumentException("IDLE phase cannot track a feature");
        }
        this.generation = generation;
        this.currentFeature = currentFeature;
    }

    public Phase phase() {
        return phase;
    }

    public long generation() {
        return generation;
    }

    public @Nullable String currentFeature() {
        return currentFeature;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return INTRODUCTION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SystemIndexMigrationProgressMetadata that = (SystemIndexMigrationProgressMetadata) o;
        return generation == that.generation && phase == that.phase && Objects.equals(currentFeature, that.currentFeature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(phase, generation, currentFeature);
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.supports(INTRODUCTION);
    }

    public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(phase);
        out.writeLong(generation);
        out.writeOptionalString(currentFeature);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        Iterator<? extends ToXContent> basics = Iterators.concat(
            ChunkedToXContentHelper.chunk((builder, bParams) -> builder.field(PHASE_FIELD.getPreferredName(), phase.value())),
            ChunkedToXContentHelper.chunk((builder, bParams) -> builder.field(GENERATION_FIELD.getPreferredName(), generation))
        );
        if (currentFeature != null) {
            return Iterators.concat(
                basics,
                ChunkedToXContentHelper.chunk((builder, bParams) -> builder.field(CURRENT_FEATURE_FIELD.getPreferredName(), currentFeature))
            );
        }
        return basics;
    }

    public static SystemIndexMigrationProgressMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}

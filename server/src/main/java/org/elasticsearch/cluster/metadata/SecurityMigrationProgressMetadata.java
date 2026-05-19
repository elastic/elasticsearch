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
import org.elasticsearch.xcontent.ConstructingObjectParser;
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
 * Project-level cluster state describing orchestration progress for `.security`-index migrations
 * (<a href="https://github.com/elastic/elasticsearch/issues/146662">#146662</a>). Persisted alongside
 * other {@link Metadata.ProjectCustom} values. Actual migration checkpoints remain on the security index itself;
 * this metadata only coordinates concurrent execution across later refactors. PR1 introduces the metadata shell only; for now this type is intentionally unused because persistent tasks remain authoritative until rewired.
 */
public final class SecurityMigrationProgressMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom>
    implements
        Metadata.ProjectCustom {

    public static final String TYPE = "security_migration_progress";

    private static final TransportVersion INTRODUCTION = TransportVersion.fromName("migration_progress_project_metadata");

    private static final ParseField PHASE_FIELD = new ParseField("phase");
    private static final ParseField GENERATION_FIELD = new ParseField("generation");

    /** No migration coordinated via this metadata. */
    public static final SecurityMigrationProgressMetadata EMPTY = new SecurityMigrationProgressMetadata(Phase.IDLE, 0L);

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
                default -> throw new IllegalArgumentException("Unexpected security migration phase [" + text + "]");
            };
        }
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SecurityMigrationProgressMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        true,
        args -> {
            Phase phase = args[0] == null ? Phase.IDLE : Phase.parse((String) args[0]);
            long generation = args[1] == null ? 0L : (long) args[1];
            return new SecurityMigrationProgressMetadata(phase, generation);
        }
    );

    static {
        PARSER.declareString(optionalConstructorArg(), PHASE_FIELD);
        PARSER.declareLong(optionalConstructorArg(), GENERATION_FIELD);
    }

    private final Phase phase;
    private final long generation;

    public SecurityMigrationProgressMetadata(StreamInput in) throws IOException {
        phase = in.readEnum(Phase.class);
        generation = in.readLong();
    }

    public SecurityMigrationProgressMetadata(Phase phase, long generation) {
        this.phase = Objects.requireNonNull(phase);
        if (generation < 0) {
            throw new IllegalArgumentException("generation cannot be negative");
        }
        if (phase == Phase.IDLE && generation != 0L) {
            throw new IllegalArgumentException("IDLE phase requires generation zero");
        }
        this.generation = generation;
    }

    public Phase phase() {
        return phase;
    }

    public long generation() {
        return generation;
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
        SecurityMigrationProgressMetadata that = (SecurityMigrationProgressMetadata) o;
        return generation == that.generation && phase == that.phase;
    }

    @Override
    public int hashCode() {
        return Objects.hash(phase, generation);
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
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.chunk((builder, bParams) -> builder.field(PHASE_FIELD.getPreferredName(), phase.value())),
            ChunkedToXContentHelper.chunk((builder, bParams) -> builder.field(GENERATION_FIELD.getPreferredName(), generation))
        );
    }

    public static SecurityMigrationProgressMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}

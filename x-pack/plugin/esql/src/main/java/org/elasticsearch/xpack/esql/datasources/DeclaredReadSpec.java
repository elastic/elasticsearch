/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * The read-instructions a declared dataset mapping derives for the data node. Carried as a first-class field along the
 * resolution &rarr; plan &rarr; operator seam ({@code ExternalSourceResolution.ResolvedSource} &rarr;
 * {@link org.elasticsearch.xpack.esql.plan.logical.ExternalRelation}
 * &rarr; {@link org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec} &rarr;
 * {@link org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext} &rarr; the operator factory), so a new
 * declared read-instruction is one typed field rather than another string key fenced and sniffed out of the config map.
 * <ul>
 *   <li>{@code renames} — the declared logical&rarr;physical column renames a {@code path} move produces. Consumed at
 *       the reader-facing boundary via {@link PhysicalNames} (projection + read schema physicalization) and by the
 *       pushdown planner rules. Empty when the mapping renames nothing.</li>
 *   <li>{@code idPath} — the declared {@code mappings._id.path} (a single logical column name), or {@code null}. When
 *       present the data node stamps {@code _id} from that column instead of the synthetic (file+row-position) identity
 *       ({@link VirtualColumnIterator}).</li>
 *   <li>{@code dateFormats} — per-column date parse-patterns, keyed by <b>logical</b> column name. The text readers
 *       parse that column's timestamps with the given pattern (via the ES {@code DateFormatter}) instead of the ISO
 *       default / file-level {@code datetime_format}. Physicalized to file-column names at the reader boundary
 *       ({@code FileSourceFactory}). Empty when no column declares a {@code format}.</li>
 *   <li>{@code declaredTypeColumns} — the <b>logical</b> names of the columns whose target type came from an explicit
 *       declaration (as opposed to inference). A declared type licenses a lossy read-time coercion toward it — e.g. a
 *       declared {@code integer} over an {@code int64} file column narrows per value (null on overflow). An inferred
 *       target must never narrow: a cross-file clash widens-or-nulls. The by-name columnar readers (Parquet/ORC) key
 *       their whole-column incompatibility null-fill on this set — a declared column keeps the coercion escape, an
 *       inferred one null-fills whenever the file type is not widening-compatible. Physicalized to file-column names at
 *       the reader boundary ({@code FileSourceFactory}); the text readers ignore it (they parse straight into the
 *       target). Empty when the mapping declares no column types.</li>
 *   <li>{@code provenance} — whether the {@code readSchema} this spec accompanies is a
 *       {@link SchemaProvenance#DECLARED} claim (bind by name, report absent columns) or was
 *       {@link SchemaProvenance#INFERRED} from the file (bind by position). The axis {@code dynamic} actually
 *       selects; the data node reads provenance, never the mode. Defaults to {@code INFERRED} — the identity a
 *       pre-gate peer and every non-declared read carry.</li>
 * </ul>
 * A plain {@link Writeable}: its wire gate lives on the enclosing plan nodes, which only read/write it when the
 * {@code dataset_declared_schema} transport version is supported (mirrors how {@code DatasetMapping.Mappings} is gated
 * by its container rather than self-gating).
 */
public record DeclaredReadSpec(
    Map<String, String> renames,
    @Nullable String idPath,
    Map<String, String> dateFormats,
    Set<String> declaredTypeColumns,
    SchemaProvenance provenance
) implements Writeable {

    /**
     * Wire gate for {@link #provenance}; a pre-gate peer reads/writes the four original fields and defaults INFERRED.
     * During a rolling upgrade a peer that predates this version therefore binds a strict read positionally — the
     * pre-fix behaviour. That is TOLERATED by design, not failed loud: this is a read path (nothing is persisted or
     * corrupted — the worst case is a transient wrong query result), the degraded behaviour equals what that peer
     * already ships, and it self-heals once every node supports the version. Failing loud would break every running
     * strict query for the upgrade window to prevent a transient, non-durable result — a worse trade.
     */
    private static final TransportVersion DECLARED_READ_SPEC_PROVENANCE = TransportVersion.fromName("declared_read_spec_provenance");

    /** The empty spec — nothing declared. The default carried on every non-declared read. */
    public static final DeclaredReadSpec NONE = new DeclaredReadSpec(Map.of(), null, Map.of(), Set.of(), SchemaProvenance.INFERRED);

    public DeclaredReadSpec {
        renames = renames != null ? Map.copyOf(renames) : Map.of();
        dateFormats = dateFormats != null ? Map.copyOf(dateFormats) : Map.of();
        declaredTypeColumns = declaredTypeColumns != null ? Set.copyOf(declaredTypeColumns) : Set.of();
        provenance = provenance != null ? provenance : SchemaProvenance.INFERRED;
    }

    /**
     * Canonical factory: collapses an all-empty spec to the {@link #NONE} singleton so an absent declaration always
     * serializes identically. The emptiness test is delegated to {@link #isEmpty()} (the single source of truth), so a
     * future field added to this record only has to update {@code isEmpty()} for the collapse to stay correct.
     */
    public static DeclaredReadSpec of(
        @Nullable Map<String, String> renames,
        @Nullable String idPath,
        @Nullable Map<String, String> dateFormats,
        @Nullable Set<String> declaredTypeColumns,
        @Nullable SchemaProvenance provenance
    ) {
        DeclaredReadSpec spec = new DeclaredReadSpec(renames, idPath, dateFormats, declaredTypeColumns, provenance);
        return spec.isEmpty() ? NONE : spec;
    }

    /** Convenience for a spec over an inferred schema ({@link SchemaProvenance#INFERRED}). */
    public static DeclaredReadSpec of(
        @Nullable Map<String, String> renames,
        @Nullable String idPath,
        @Nullable Map<String, String> dateFormats,
        @Nullable Set<String> declaredTypeColumns
    ) {
        return of(renames, idPath, dateFormats, declaredTypeColumns, SchemaProvenance.INFERRED);
    }

    /** Convenience for a spec with no declared date formats and no declared column types. */
    public static DeclaredReadSpec of(@Nullable Map<String, String> renames, @Nullable String idPath) {
        return of(renames, idPath, Map.of(), Set.of(), SchemaProvenance.INFERRED);
    }

    /**
     * True when the mapping declared nothing for the data node to apply — no rename, {@code _id.path}, format, or type,
     * and {@link SchemaProvenance#INFERRED} provenance. A DECLARED provenance is itself an instruction, so it
     * keeps the spec from collapsing to {@link #NONE} (whose provenance is INFERRED) and being lost on the wire.
     */
    public boolean isEmpty() {
        return renames.isEmpty()
            && idPath == null
            && dateFormats.isEmpty()
            && declaredTypeColumns.isEmpty()
            && provenance == SchemaProvenance.INFERRED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(renames, StreamOutput::writeString, StreamOutput::writeString);
        out.writeOptionalString(idPath);
        out.writeMap(dateFormats, StreamOutput::writeString, StreamOutput::writeString);
        out.writeCollection(declaredTypeColumns, StreamOutput::writeString);
        if (out.getTransportVersion().supports(DECLARED_READ_SPEC_PROVENANCE)) {
            out.writeEnum(provenance);
        }
    }

    public static DeclaredReadSpec readFrom(StreamInput in) throws IOException {
        Map<String, String> renames = in.readMap(StreamInput::readString);
        String idPath = in.readOptionalString();
        Map<String, String> dateFormats = in.readMap(StreamInput::readString);
        Set<String> declaredTypeColumns = in.readCollectionAsSet(StreamInput::readString);
        SchemaProvenance provenance = in.getTransportVersion().supports(DECLARED_READ_SPEC_PROVENANCE)
            ? in.readEnum(SchemaProvenance.class)
            : SchemaProvenance.INFERRED;
        return of(renames, idPath, dateFormats, declaredTypeColumns, provenance);
    }
}

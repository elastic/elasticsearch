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
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;

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
 *   <li>{@code bindsByName} — the {@code readSchema} this spec accompanies names columns rather than positions, so
 *       the reader binds each one against the file's own physical-name space (header line, columnar footer, JSON keys,
 *       or the self-encoded {@code col<N>} names of a headerless text file) and null-fills a named column the file does
 *       not supply, with one deduplicated warning. False binds by position: the <em>i</em>-th column of the schema is
 *       the <em>i</em>-th physical field. Defaults to false — what every non-declared read carries.</li>
 *   <li>{@code blankStringCellIsEmptyString} — a present but empty cell on a string column holds the empty string
 *       rather than {@code null}, unless {@code null_value} names the blank. False reads such a cell as {@code null} on
 *       every column type. Defaults to false.</li>
 * </ul>
 * <p>
 * The two flags are read instructions, not a statement about where the schema came from. Discovery decides them (a
 * {@code dynamic:false} mapping sets both); nothing downstream asks how they were chosen, which is what keeps a cached
 * statistic keyed on what a read DID to the cells rather than on the mapping that happened to produce it.
 * <p>
 * A plain {@link Writeable}: its wire gate lives on the enclosing plan nodes, which only read/write it when the
 * {@code dataset_declared_schema} transport version is supported (mirrors how {@code DatasetMapping.Mappings} is gated
 * by its container rather than self-gating).
 */
public record DeclaredReadSpec(
    Map<String, String> renames,
    Map<String, String> dateFormats,
    Set<String> declaredTypeColumns,
    boolean bindsByName,
    boolean blankStringCellIsEmptyString
) implements Writeable {

    /**
     * Wire gate for the read-instruction slot; a pre-gate peer reads/writes the four original fields and defaults both
     * flags false. During a rolling upgrade a peer that predates this version therefore binds a strict read
     * positionally — the pre-fix behaviour. That is TOLERATED by design, not failed loud: this is a read path (nothing is persisted or
     * corrupted — the worst case is a transient wrong query result), the degraded behaviour equals what that peer
     * already ships, and it self-heals once every node supports the version. Failing loud would break every running
     * strict query for the upgrade window to prevent a transient, non-durable result — a worse trade.
     */
    private static final TransportVersion DECLARED_READ_SPEC_PROVENANCE = TransportVersion.fromName("declared_read_spec_provenance");

    /** The empty spec — nothing declared. The default carried on every non-declared read. */
    public static final DeclaredReadSpec NONE = new DeclaredReadSpec(Map.of(), Map.of(), Set.of(), false, false);

    public DeclaredReadSpec {
        renames = renames != null ? Map.copyOf(renames) : Map.of();
        dateFormats = dateFormats != null ? Map.copyOf(dateFormats) : Map.of();
        declaredTypeColumns = declaredTypeColumns != null ? Set.copyOf(declaredTypeColumns) : Set.of();
    }

    /**
     * Canonical factory: collapses an all-empty spec to the {@link #NONE} singleton so an absent declaration always
     * serializes identically. The emptiness test is delegated to {@link #isEmpty()} (the single source of truth), so a
     * future field added to this record only has to update {@code isEmpty()} for the collapse to stay correct.
     */
    public static DeclaredReadSpec of(
        @Nullable Map<String, String> renames,
        @Nullable Map<String, String> dateFormats,
        @Nullable Set<String> declaredTypeColumns,
        boolean bindsByName,
        boolean blankStringCellIsEmptyString
    ) {
        DeclaredReadSpec spec = new DeclaredReadSpec(renames, dateFormats, declaredTypeColumns, bindsByName, blankStringCellIsEmptyString);
        return spec.isEmpty() ? NONE : spec;
    }

    /** Convenience for a spec over a positionally bound schema whose blank string cells read as {@code null}. */
    public static DeclaredReadSpec of(
        @Nullable Map<String, String> renames,
        @Nullable Map<String, String> dateFormats,
        @Nullable Set<String> declaredTypeColumns
    ) {
        return of(renames, dateFormats, declaredTypeColumns, false, false);
    }

    /** Convenience for a spec with no declared date formats and no declared column types. */
    public static DeclaredReadSpec of(@Nullable Map<String, String> renames) {
        return of(renames, Map.of(), Set.of(), false, false);
    }

    /**
     * True when the mapping declared nothing for the data node to apply — no rename, format, or type, and neither read
     * instruction set. Either flag being true is itself an instruction, so it keeps the spec from collapsing to
     * {@link #NONE} (which carries both false) and being lost on the wire.
     */
    public boolean isEmpty() {
        return renames.isEmpty()
            && dateFormats.isEmpty()
            && declaredTypeColumns.isEmpty()
            && bindsByName == false
            && blankStringCellIsEmptyString == false;
    }

    /**
     * True when reading this spec under {@code policy} can drop a whole row: the read asked for
     * {@link ErrorPolicy.Mode#SKIP_ROW} <em>and</em> declares column types that a value can fail to coerce into.
     * With no declared types there is nothing to coerce, so no row is ever dropped and {@code skip_row} is
     * indistinguishable from {@code fail_fast} on the columnar readers.
     * <p>
     * This is the single predicate for that combination. It gates three independent decisions that must agree for
     * one read — whether {@code PushFiltersToSource} pushes the filter, whether {@code InsertExternalFieldExtraction}
     * inserts an {@code ExternalFieldExtractExec}, and whether the operator factory enables deferred extraction —
     * because each of those moves the page's shape away from the point where the reader drops rows. Two of them are
     * plan-time and one is execution-time, so any drift between them shows up as a plan the factory cannot honour.
     * <p>
     * Footer statistics need no such gate: {@code FileSplitProvider} already poisons declared-retyped and
     * date-format columns out of the published stats (their pre-coercion extrema are untrustworthy), and the
     * surviving {@code row_count} is what a {@code COUNT(*)} scan returns anyway — that scan projects no column, so
     * nothing is decoded and no row can be dropped.
     * <p>
     * Keyed on the <b>logical</b> {@code declaredTypeColumns}; {@code FileSourceFactory} physicalizes the same set
     * through the {@code path} renames for the by-name readers, which is a 1:1 map and so cannot change emptiness.
     */
    public boolean dropsRowsOnCoercionFailure(ErrorPolicy policy) {
        return policy.mode() == ErrorPolicy.Mode.SKIP_ROW && declaredTypeColumns.isEmpty() == false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(renames, StreamOutput::writeString, StreamOutput::writeString);
        // An optional string this version has no field for. A dataset answers METADATA _id as SQL NULL, so no
        // declared column feeds it, and the slot goes out empty. Both sides write it unconditionally from 9.5
        // onward, so dropping it needs a new transport version gating read and write; 9.5 leaving the
        // wire-compatibility window is not the trigger.
        out.writeOptionalString(null);
        out.writeMap(dateFormats, StreamOutput::writeString, StreamOutput::writeString);
        out.writeCollection(declaredTypeColumns, StreamOutput::writeString);
        if (out.getTransportVersion().supports(DECLARED_READ_SPEC_PROVENANCE)) {
            // One bit carries both instructions at this version: discovery sets them together, so the slot that used to
            // hold the schema-provenance enum holds bindsByName and the reader derives the blank rule from it. The
            // values match that enum's ordinals, so a peer predating this change reads exactly what it reads today. The
            // change that first lets the two differ must add its own slot under a new transport version.
            assert bindsByName == blankStringCellIsEmptyString
                : "the wire carries one bit for both read instructions; add a slot before letting them differ";
            out.writeVInt(bindsByName ? 1 : 0);
        }
    }

    public static DeclaredReadSpec readFrom(StreamInput in) throws IOException {
        Map<String, String> renames = in.readMap(StreamInput::readString);
        in.readOptionalString(); // the _id.path slot a 9.5 peer writes; see writeTo
        Map<String, String> dateFormats = in.readMap(StreamInput::readString);
        Set<String> declaredTypeColumns = in.readCollectionAsSet(StreamInput::readString);
        boolean bindsByName = in.getTransportVersion().supports(DECLARED_READ_SPEC_PROVENANCE) && in.readVInt() == 1;
        return of(renames, dateFormats, declaredTypeColumns, bindsByName, bindsByName);
    }
}

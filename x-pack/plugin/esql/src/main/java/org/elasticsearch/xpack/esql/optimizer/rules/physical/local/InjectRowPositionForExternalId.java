/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.datasources.ExternalMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.FileMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.SyntheticColumns;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;

import java.util.ArrayList;
import java.util.List;

/**
 * Inject the synthetic {@link ColumnExtractor#ROW_POSITION_COLUMN} into an
 * {@link ExternalSourceExec} whose bound output references the standard {@code _id} metadata column
 * or the {@code _file.record_ref} virtual column. The reader-emitted {@code _rowPosition} channel
 * carries each record's file-global, split-invariant position; the producer pipeline composes
 * {@code _id} as the opaque {@code (location, mtime, rowPosition)} hash per row and exposes the masked physical position
 * directly as {@code _file.record_ref} (see {@code ExternalRowIdentity} / {@code VirtualColumnIterator}).
 * <p>
 * Every file reader materializes {@code _rowPosition}: Parquet (Java) and ORC emit a file-global
 * row index from footer/stripe metadata (Parquet-Java encodes an extractor id into the high bits
 * for the deferred-extraction path, which the composition path masks off); Parquet-RS splices an
 * all-null block so {@code _id} renders null per the disclosed carve-out; NDJSON and CSV both emit
 * a file-global byte anchor (CSV: the record's start byte; NDJSON: the parser position just past
 * the record's opening token — exact anchor is opaque, intrinsic position is the contract), so the
 * same physical record carries the same {@code _rowPosition} regardless of split layout.
 * <p>
 * Sibling of {@link InsertExternalFieldExtraction}, which also injects {@code _rowPosition} but for
 * the deferred-extraction late-materialization path (it additionally requires a TopN above the
 * source) and only for {@link org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorAware}
 * (Parquet-class) readers, which encode an extractor id into the high bits; the composition path
 * masks that off, so a raw (unencoded) position from a non-deferred reader composes identically.
 * <p>
 * No-ops when the source's output already contains a {@code _rowPosition} attribute (deferred
 * extraction got there first, or another query rewrote the plan twice). Idempotent.
 */
public class InjectRowPositionForExternalId extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    ExternalSourceExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected ExternalSourceExec rule(ExternalSourceExec source, LocalPhysicalOptimizerContext ctx) {
        boolean positionRequested = false;
        boolean rowPositionPresent = false;
        for (Attribute a : source.output()) {
            if (a instanceof ExternalMetadataAttribute && ExternalMetadataColumns.ID.equals(a.name())) {
                positionRequested = true;
            }
            if (a instanceof ExternalMetadataAttribute && FileMetadataColumns.RECORD_REF.equals(a.name())) {
                positionRequested = true;
            }
            if (a instanceof MetadataAttribute m && m.synthetic() && ColumnExtractor.ROW_POSITION_COLUMN.equals(a.name())) {
                rowPositionPresent = true;
            }
        }
        if (positionRequested == false || rowPositionPresent) {
            return source;
        }

        MetadataAttribute rowPositionAttribute = SyntheticColumns.newRowPositionMetadataAttribute(source.source());

        List<Attribute> extended = new ArrayList<>(source.output().size() + 1);
        extended.addAll(source.output());
        extended.add(rowPositionAttribute);
        return source.withAttributes(extended);
    }
}

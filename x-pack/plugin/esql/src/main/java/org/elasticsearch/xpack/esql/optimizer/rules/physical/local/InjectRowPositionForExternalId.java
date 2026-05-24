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
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ExternalMetadataColumns;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;

import java.util.ArrayList;
import java.util.List;

/**
 * Inject the synthetic {@link ColumnExtractor#ROW_POSITION_COLUMN} into every
 * {@link ExternalSourceExec} whose bound output references the standard {@code _id} metadata
 * column. The producer pipeline composes {@code _id} as {@code <location>:<rowPosition>} per
 * row (see {@code ExternalRowIdentity}); the {@code _rowPosition} channel is the per-row
 * physical-position input that composition consumes.
 * <p>
 * Sibling of {@link InsertExternalFieldExtraction}, which also injects {@code _rowPosition}
 * but for the deferred-extraction late-materialization path. The two rules are kept separate:
 * deferred extraction depends on a TopN above the source and on
 * {@link org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorAware} reader support,
 * neither of which {@code _id} composition needs.
 * <p>
 * No-ops when the source's output already contains a {@code _rowPosition} attribute (deferred
 * extraction got there first, or another query rewrote the plan twice). Idempotent.
 */
public class InjectRowPositionForExternalId extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    ExternalSourceExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected ExternalSourceExec rule(ExternalSourceExec source, LocalPhysicalOptimizerContext ctx) {
        boolean idRequested = false;
        boolean rowPositionPresent = false;
        for (Attribute a : source.output()) {
            if (a instanceof ExternalMetadataAttribute && ExternalMetadataColumns.ID.equals(a.name())) {
                idRequested = true;
            }
            if (ColumnExtractor.ROW_POSITION_COLUMN.equals(a.name())) {
                rowPositionPresent = true;
            }
        }
        if (idRequested == false || rowPositionPresent) {
            return source;
        }

        MetadataAttribute rowPositionAttribute = new MetadataAttribute(
            source.source(),
            ColumnExtractor.ROW_POSITION_COLUMN,
            DataType.LONG,
            Nullability.FALSE,
            null,
            true,
            false
        );

        List<Attribute> extended = new ArrayList<>(source.output().size() + 1);
        extended.addAll(source.output());
        extended.add(rowPositionAttribute);
        return source.withAttributes(extended);
    }
}

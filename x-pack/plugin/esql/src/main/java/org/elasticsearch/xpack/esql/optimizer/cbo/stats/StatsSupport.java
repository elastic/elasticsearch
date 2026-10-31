/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.stats;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

final class StatsSupport {
    private StatsSupport() {}

    static Map<NameId, ColumnStats> unknownColumns(List<Attribute> attributes) {
        Map<NameId, ColumnStats> columns = new LinkedHashMap<>();
        for (Attribute attribute : attributes) {
            columns.put(attribute.id(), withDefaultWidth(attribute));
        }
        return columns;
    }

    static ColumnStats withDefaultWidth(Attribute attribute) {
        int estimatedSize = attribute.dataType().estimatedSize();
        Estimate averageBytes = estimatedSize > 0 ? Estimate.defaultValue(estimatedSize) : Estimate.UNKNOWN;
        return new ColumnStats(Estimate.UNKNOWN, Estimate.UNKNOWN, Optional.empty(), Optional.empty(), averageBytes, Estimate.UNKNOWN);
    }

    static Estimate averageRowBytes(List<Attribute> output, Map<NameId, ColumnStats> columns) {
        Estimate total = Estimate.exact(0);
        for (Attribute attribute : output) {
            ColumnStats column = columns.get(attribute.id());
            Estimate averageBytes = column == null ? Estimate.UNKNOWN : column.averageBytes();
            if (averageBytes.isUnknown()) {
                averageBytes = withDefaultWidth(attribute).averageBytes();
            }
            if (averageBytes.isUnknown()) {
                return Estimate.UNKNOWN;
            }
            total = total.plus(averageBytes);
        }
        return total;
    }

    static Estimate bytes(Estimate rows, Estimate averageRowBytes) {
        return rows.multiply(averageRowBytes);
    }
}

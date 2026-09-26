/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;

import java.util.List;
import java.util.Set;

/**
 * A label column of a translated PromQL relation. The label schema is open: which labels a series carries is not known
 * at plan time, so besides the labels the query names ({@link Static}) a relation may carry every other label of the
 * series packed into one physical column ({@link DynamicColumnList} - SQL's {@code * EXCEPT (..)}).
 */
public sealed interface TranslationColumn permits TranslationColumn.Static, TranslationColumn.DynamicColumnList {

    /** A label known by name. */
    record Static(String name) implements TranslationColumn {}

    /** Every label of the series except {@code except}, packed into one column named {@code _timeseries$except...}. */
    record DynamicColumnList(Set<String> except) implements TranslationColumn {

        public DynamicColumnList {
            except = Set.copyOf(except);
        }

        /** The physical column name: {@code _timeseries} for the full series, {@code _timeseries$a$b} (sorted) otherwise. */
        public String name() {
            return TimeSeriesMetadataAttribute.nameFor(except);
        }

        /** The attribute among {@code attributes} that produces this column, or null. */
        public Attribute find(List<Attribute> attributes) {
            for (var attribute : attributes) {
                if (attribute instanceof TimeSeriesMetadataAttribute metadata && metadata.excludedFields().equals(except)) {
                    return metadata;
                }
            }
            return null;
        }
    }
}

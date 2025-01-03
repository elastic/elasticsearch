/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DocCountFieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.IndexModeFieldMapper;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.xpack.cluster.routing.allocation.mapper.DataTierFieldMapper;

import java.util.Set;

public class MetadataFieldsAllowlist {
    private MetadataFieldsAllowlist() {}

    // public for testing
    public static final Set<String> FIELDS = Set.of(
        // built-in
        IgnoredFieldMapper.NAME,
        IdFieldMapper.NAME,
        RoutingFieldMapper.NAME,
        TimeSeriesIdFieldMapper.NAME,
        TimeSeriesRoutingHashFieldMapper.NAME,
        IndexFieldMapper.NAME,
        IndexModeFieldMapper.NAME,
        SourceFieldMapper.NAME,
        IgnoredSourceFieldMapper.NAME,
        NestedPathFieldMapper.NAME,
        NestedPathFieldMapper.NAME_PRE_V8,
        VersionFieldMapper.NAME,
        SeqNoFieldMapper.NAME,
        SeqNoFieldMapper.PRIMARY_TERM_NAME,
        DocCountFieldMapper.NAME,
        DataStreamTimestampFieldMapper.NAME,
        FieldNamesFieldMapper.NAME,
        // plugins
        // MapperSizePlugin
        "_size",
        // MapperExtrasPlugin
        "_feature",
        // XPackPlugin
        DataTierFieldMapper.NAME
    );

    public static final FieldPredicate PREDICATE = new FieldPredicate() {
        @Override
        public boolean test(String field) {
            return FIELDS.contains(field) || (field.startsWith("_") && nestedMatch(field));
        }

        private boolean nestedMatch(String field) {
            return FIELDS.stream().anyMatch(f -> field.startsWith(f + "."));
        }

        @Override
        public String modifyHash(String hash) {
            return hash + ":metadata_fields_allowlist";
        }

        @Override
        public long ramBytesUsed() {
            return 0; // shared
        }

        @Override
        public String toString() {
            return "metadata fields allowlist";
        }
    };
}

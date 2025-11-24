/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute.FieldName;

public class DisabledSearchStats implements SearchStats {

    @Override
    public boolean exists(FieldName field) {
        return true;
    }

    @Override
    public boolean isIndexed(FieldName field) {
        // Vector fields are not indexed, so vector similarity functions are be evaluated via evaluator instead of pushed down in CSV tests
        return field.string().contains("vector") == false;
    }

    @Override
    public boolean hasDocValues(FieldName field) {
        // Some geo tests assume doc values and the loader emulates it. Nothing else does.
        return field.string().endsWith("location") || field.string().endsWith("centroid") || field.string().equals("subset");
    }

    @Override
    public boolean hasExactSubfield(FieldName field) {
        return true;
    }

    @Override
    public boolean supportsLoaderConfig(
        FieldName name,
        BlockLoaderFunctionConfig config,
        MappedFieldType.FieldExtractPreference preference
    ) {
        return false;
    }

    @Override
    public long count() {
        return -1;
    }

    @Override
    public long count(FieldName field) {
        return -1;
    }

    @Override
    public long count(FieldName field, BytesRef value) {
        return -1;
    }

    @Override
    public Object min(FieldName field) {
        return null;
    }

    @Override
    public Object max(FieldName field) {
        return null;
    }

    @Override
    public boolean isSingleValue(FieldName field) {
        return false;
    }

    @Override
    public boolean canUseEqualityOnSyntheticSourceDelegate(FieldName name, String value) {
        return false;
    }
}

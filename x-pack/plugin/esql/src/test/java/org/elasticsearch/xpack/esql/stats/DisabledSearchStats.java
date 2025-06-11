/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute.FieldName;
import org.elasticsearch.xpack.esql.core.type.DataType;

public class DisabledSearchStats implements SearchStats {

    @Override
    public boolean exists(FieldName field) {
        return true;
    }

    @Override
    public boolean isIndexed(FieldName field) {
        return true;
    }

    @Override
    public boolean hasDocValues(FieldName field) {
        return true;
    }

    @Override
    public boolean hasExactSubfield(FieldName field) {
        return true;
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
    public byte[] min(FieldName field, DataType dataType) {
        return null;
    }

    @Override
    public byte[] max(FieldName field, DataType dataType) {
        return null;
    }

    @Override
    public boolean isSingleValue(FieldName field) {
        return false;
    }
}

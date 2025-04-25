/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.type.DataType;

public class DisabledSearchStats implements SearchStats {

    @Override
    public boolean exists(String field) {
        return true;
    }

    @Override
    public boolean isIndexed(String field) {
        return true;
    }

    @Override
    public boolean hasDocValues(String field) {
        return true;
    }

    @Override
    public boolean hasExactSubfield(String field) {
        return true;
    }

    @Override
    public long count() {
        return -1;
    }

    @Override
    public long count(String field) {
        return -1;
    }

    @Override
    public long count(String field, BytesRef value) {
        return -1;
    }

    @Override
    public byte[] min(String field, DataType dataType) {
        return null;
    }

    @Override
    public byte[] max(String field, DataType dataType) {
        return null;
    }

    @Override
    public boolean isSingleValue(String field) {
        return false;
    }

    @Override
    public boolean canUseEqualityOnSyntheticSourceDelegate(String name, String value) {
        return false;
    }
}

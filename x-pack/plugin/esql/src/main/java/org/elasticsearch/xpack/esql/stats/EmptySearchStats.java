/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.type.DataType;

public record EmptySearchStats() implements SearchStats {
    @Override
    public long count() {
        return 0;
    }

    @Override
    public long count(String field) {
        return 0;
    }

    @Override
    public long count(String field, BytesRef value) {
        return 0;
    }

    @Override
    public boolean exists(String field) {
        return false;
    }

    @Override
    public boolean isIndexed(String field) {
        return false;
    }

    @Override
    public boolean hasDocValues(String field) {
        return false;
    }

    @Override
    public boolean hasIdenticalDelegate(String field) {
        return false;
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
        return true;
    }

}

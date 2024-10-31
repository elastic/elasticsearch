/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * Interface for determining information about fields in the index.
 * This is used by the optimizer to make decisions about how to optimize queries.
 */
public interface SearchStats {
    boolean exists(String field);

    boolean isIndexed(String field);

    boolean hasDocValues(String field);

    boolean hasIdenticalDelegate(String field);

    long count();

    long count(String field);

    long count(String field, BytesRef value);

    byte[] min(String field, DataType dataType);

    byte[] max(String field, DataType dataType);

    boolean isSingleValue(String field);
}

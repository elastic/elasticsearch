/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.core.Nullable;

import java.util.List;

/**
 * Lightweight per-document container holding the output of a single-pass parse.
 * Contains all flattened field data (for batch encoding) and routing information
 * (hash and optional TSID) produced by {@link SinglePassParser}.
 */
final class FlattenedDoc {

    private final IndexRequest request;
    private final List<FieldValue> fields;
    private final int routingHash;
    @Nullable
    private final BytesRef tsid;

    FlattenedDoc(IndexRequest request, List<FieldValue> fields, int routingHash, @Nullable BytesRef tsid) {
        this.request = request;
        this.fields = fields;
        this.routingHash = routingHash;
        this.tsid = tsid;
    }

    IndexRequest request() {
        return request;
    }

    List<FieldValue> fields() {
        return fields;
    }

    int routingHash() {
        return routingHash;
    }

    @Nullable
    BytesRef tsid() {
        return tsid;
    }
}

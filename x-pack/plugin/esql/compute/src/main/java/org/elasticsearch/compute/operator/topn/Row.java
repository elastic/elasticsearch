/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;

interface Row extends Accountable, Releasable {
    BreakingBytesRefBuilder keys();

    TopNOperator.BytesOrder bytesOrder();

    void setShardRefCounted(RefCounted refCounted);

    BreakingBytesRefBuilder values();

    void clear();
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Releasable;

import java.util.List;

/** Independently owned selected payloads; borrowed views must be consumed before their owner is closed. */
interface SequenceMatch extends Releasable {
    BytesRef key();

    SequenceOrdinal start();

    SequenceOrdinal end();

    List<BytesRef> handles();
}

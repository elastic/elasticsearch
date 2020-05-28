/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.xpack.eql.execution.sequence.SequenceKey;

class KeyWithTime {
    final SequenceKey key;
    final long timestamp;

    KeyWithTime(SequenceKey key, long timestamp) {
        this.key = key;
        this.timestamp = timestamp;
    }
}

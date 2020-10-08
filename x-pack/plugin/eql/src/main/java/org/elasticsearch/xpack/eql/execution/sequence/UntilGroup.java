/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

public class UntilGroup extends OrdinalGroup<KeyAndOrdinal> {

    UntilGroup(SequenceKey key) {
        super(key, KeyAndOrdinal::ordinal);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.OrdinalMap;

/**
 * {@link TsdbDocValuesProducer} used on the optimized merge path that additionally carries the {@link MergeState}
 * and the field's {@link OrdinalMap}. A run-granularity merge writer needs both to read each source segment's
 * runs and remap ordinals to the merged terms dictionary; a plain {@code TsdbDocValuesProducer} carries only the
 * merge stats. The consumer detects this type to decide whether run-granularity merge is available for a field.
 */
public class MergingTsdbDocValuesProducer extends TsdbDocValuesProducer {

    public final MergeState mergeState;
    public final OrdinalMap ordinalMap;

    public MergingTsdbDocValuesProducer(DocValuesConsumerUtil.MergeStats mergeStats, MergeState mergeState, OrdinalMap ordinalMap) {
        super(mergeStats);
        this.mergeState = mergeState;
        this.ordinalMap = ordinalMap;
    }
}

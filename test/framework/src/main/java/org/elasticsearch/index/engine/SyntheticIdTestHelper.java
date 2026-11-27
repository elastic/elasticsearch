/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;

import java.io.IOException;

public class SyntheticIdTestHelper {

    /**
     * Wraps {@code policy} so that merges never materialize the synthetic {@code _id}, without pruning anything.
     * Index writers that write synthetic id segments outside of the engine need this, as the engine's merge policy
     * is what otherwise keeps the synthetic id reader away from merges.
     */
    public static MergePolicy skipSyntheticIdOnMerge(MergePolicy policy) {
        return new OneMergeWrappingMergePolicy(policy, toWrap -> new MergePolicy.OneMerge(toWrap.segments) {
            @Override
            public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                return PruningMergePolicy.unwrapSyntheticIdStoredFieldsReader(toWrap.wrapForMerge(reader));
            }
        });
    }
}

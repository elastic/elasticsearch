/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.engine;

import org.elasticsearch.index.store.Store;
import org.elasticsearch.xpack.parquet.write.flush.ShardCommitData;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** The commit data of a shard in the one Lucene commit of its store. */
final class StoreCommitData implements ShardCommitData {

    private final Store store;

    StoreCommitData(Store store) {
        this.store = store;
    }

    @Override
    public Map<String, String> userData() throws IOException {
        return new HashMap<>(store.readLastCommittedSegmentsInfo().getUserData());
    }

    @Override
    public void commit(Map<String, String> userData) throws IOException {
        store.associateIndexWithNewUserData(userData);
    }
}

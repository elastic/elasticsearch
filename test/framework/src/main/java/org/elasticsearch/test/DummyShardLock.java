/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.shard.ShardId;

/*
 * A ShardLock that does nothing... for tests only
 */
public class DummyShardLock extends ShardLock {

    public DummyShardLock(ShardId id) {
        super(id);
    }

    @Override
    protected void closeInternal() {}
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.util.PlainIterator;

import java.util.List;

/**
 * A simple {@link ShardsIterator} that iterates a list or sub-list of
 * {@link ShardRouting shard indexRoutings}.
 */
public class PlainShardsIterator extends PlainIterator<ShardRouting> implements ShardsIterator {
    public PlainShardsIterator(List<ShardRouting> shards) {
        super(shards);
    }

    @Override
    public int sizeActive() {
        return Math.toIntExact(getShardRoutings().stream().filter(ShardRouting::active).count());
    }

    @Override
    public List<ShardRouting> getShardRoutings() {
        return asList();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SearchShardTests extends ESTestCase {

   public void testEqualsAndHashcode() {
       String index = randomAlphaOfLengthBetween(5, 10);
       SearchShard searchShard = new SearchShard(randomBoolean() ? null : randomAlphaOfLengthBetween(3, 10),
           new ShardId(index, index + "-uuid", randomIntBetween(0, 1024)));
       EqualsHashCodeTestUtils.checkEqualsAndHashCode(searchShard,
           s -> new SearchShard(s.getClusterAlias(), s.getShardId()),
           s -> {
            if (randomBoolean()) {
                return new SearchShard(s.getClusterAlias() == null ? randomAlphaOfLengthBetween(3, 10) : null, s.getShardId());
            } else {
                String indexName = s.getShardId().getIndexName();
                int shardId = s.getShardId().getId();
                if (randomBoolean()) {
                    indexName += randomAlphaOfLength(5);
                } else {
                    shardId += randomIntBetween(1, 1024);
                }
                return new SearchShard(s.getClusterAlias(), new ShardId(indexName, indexName + "-uuid", shardId));
            }
           });
   }

   public void testCompareTo() {
       List<SearchShard> searchShards = new ArrayList<>();
       Index index0 = new Index("index0", "index0-uuid");
       Index index1 = new Index("index1", "index1-uuid");
       searchShards.add(new SearchShard(null, new ShardId(index0, 0)));
       searchShards.add(new SearchShard(null, new ShardId(index1, 0)));
       searchShards.add(new SearchShard(null, new ShardId(index0, 1)));
       searchShards.add(new SearchShard(null, new ShardId(index1, 1)));
       searchShards.add(new SearchShard(null, new ShardId(index0, 2)));
       searchShards.add(new SearchShard(null, new ShardId(index1, 2)));
       searchShards.add(new SearchShard("", new ShardId(index0, 0)));
       searchShards.add(new SearchShard("", new ShardId(index1, 0)));
       searchShards.add(new SearchShard("", new ShardId(index0, 1)));
       searchShards.add(new SearchShard("", new ShardId(index1, 1)));

       searchShards.add(new SearchShard("remote0", new ShardId(index0, 0)));
       searchShards.add(new SearchShard("remote0", new ShardId(index1, 0)));
       searchShards.add(new SearchShard("remote0", new ShardId(index0, 1)));
       searchShards.add(new SearchShard("remote0", new ShardId(index0, 2)));
       searchShards.add(new SearchShard("remote1", new ShardId(index0, 0)));
       searchShards.add(new SearchShard("remote1", new ShardId(index1, 0)));
       searchShards.add(new SearchShard("remote1", new ShardId(index0, 1)));
       searchShards.add(new SearchShard("remote1", new ShardId(index1, 1)));

       List<SearchShard> sorted = new ArrayList<>(searchShards);
       Collections.sort(sorted);
       assertEquals(searchShards, sorted);
   }
}

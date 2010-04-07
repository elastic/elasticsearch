/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.service;

import com.google.inject.Injector;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.index.IndexComponent;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.routing.OperationRouting;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.util.component.CloseableIndexComponent;

import java.util.Set;

/**
 * @author kimchy (Shay Banon)
 */
public interface IndexService extends IndexComponent, Iterable<IndexShard>, CloseableIndexComponent {

    Injector injector();

    IndexCache cache();

    OperationRouting operationRouting();

    MapperService mapperService();

    IndexQueryParserService queryParserService();

    SimilarityService similarityService();

    IndexShard createShard(int sShardId) throws ElasticSearchException;

    void deleteShard(int shardId) throws ElasticSearchException;

    int numberOfShards();

    Set<Integer> shardIds();

    boolean hasShard(int shardId);

    IndexShard shard(int shardId);

    IndexShard shardSafe(int shardId) throws IndexShardMissingException;

    Injector shardInjector(int shardId);

    Injector shardInjectorSafe(int shardId) throws IndexShardMissingException;
}

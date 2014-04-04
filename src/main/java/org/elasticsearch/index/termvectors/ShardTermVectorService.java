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

package org.elasticsearch.index.termvectors;

import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.action.termvector.TermVectorResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;

/**
 */

public class ShardTermVectorService extends AbstractIndexShardComponent {

    private IndexShard indexShard;
    private MapperService mapperService;

    @Inject
    public ShardTermVectorService(ShardId shardId, @IndexSettings Settings indexSettings, MapperService mapperService) {
        super(shardId, indexSettings);
    }

    // sadly, to overcome cyclic dep, we need to do this and inject it ourselves...
    public ShardTermVectorService setIndexShard(IndexShard indexShard) {
        this.indexShard = indexShard;
        return this;
    }

    public TermVectorResponse getTermVector(TermVectorRequest request) {
        final Engine.Searcher searcher = indexShard.acquireSearcher("term_vector");
        IndexReader topLevelReader = searcher.reader();
        final TermVectorResponse termVectorResponse = new TermVectorResponse(request.index(), request.type(), request.id());
        final Term uidTerm = new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(request.type(), request.id()));
        try {
            Fields topLevelFields = MultiFields.getFields(topLevelReader);
            Versions.DocIdAndVersion docIdAndVersion = Versions.loadDocIdAndVersion(topLevelReader, uidTerm);
            if (docIdAndVersion != null) {

                Fields termVectorsByField = docIdAndVersion.context.reader().getTermVectors(docIdAndVersion.docId);
                termVectorResponse.setFields(termVectorsByField, request.selectedFields(), request.getFlags(), topLevelFields);
                termVectorResponse.setExists(true);
                termVectorResponse.setDocVersion(docIdAndVersion.version);
            } else {
                termVectorResponse.setExists(false);
            }
        } catch (Throwable ex) {
            throw new ElasticsearchException("failed to execute term vector request", ex);
        } finally {
            searcher.close();
        }
        return termVectorResponse;
    }
}

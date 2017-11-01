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
package org.elasticsearch.index.shard;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;
import java.util.function.IntConsumer;
import java.util.function.Predicate;

/**
 * A query that selects all docs that do NOT belong in the current shards this query is executed on.
 * It can be used to split a shard into N shards marking every document that doesn't belong into the shard
 * as deleted. See {@link org.apache.lucene.index.IndexWriter#deleteDocuments(Query...)}
 */
final class ShardSplittingQuery extends Query {
    private final IndexMetaData indexMetaData;
    private final int shardId;

    ShardSplittingQuery(IndexMetaData indexMetaData, int shardId) {
        if (indexMetaData.getCreationVersion().before(Version.V_6_0_0_rc2)) {
            throw new IllegalArgumentException("Splitting query can only be executed on an index created with version "
                + Version.V_6_0_0_rc2 + " or higher");
        }
        this.indexMetaData = indexMetaData;
        this.shardId = shardId;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public String toString() {
                return "weight(delete docs query)";
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                LeafReader leafReader = context.reader();
                FixedBitSet bitSet = new FixedBitSet(leafReader.maxDoc());
                Terms terms = leafReader.terms(RoutingFieldMapper.NAME);
                Predicate<BytesRef> includeInShard = ref -> {
                    int targetShardId = OperationRouting.generateShardId(indexMetaData,
                        Uid.decodeId(ref.bytes, ref.offset, ref.length), null);
                    return shardId == targetShardId;
                };
                if (terms == null) { // this is the common case - no partitioning and no _routing values
                    assert indexMetaData.isRoutingPartitionedIndex() == false;
                    findSplitDocs(IdFieldMapper.NAME, includeInShard, leafReader, bitSet::set);
                } else {
                    if (indexMetaData.isRoutingPartitionedIndex()) {
                        // this is the heaviest invariant. Here we have to visit all docs stored fields do extract _id and _routing
                        // this this index is routing partitioned.
                        Visitor visitor = new Visitor();
                        return new ConstantScoreScorer(this, score(),
                            new RoutingPartitionedDocIdSetIterator(leafReader, visitor));
                    } else {
                        // in the _routing case we first go and find all docs that have a routing value and mark the ones we have to delete
                        findSplitDocs(RoutingFieldMapper.NAME, ref -> {
                            int targetShardId = OperationRouting.generateShardId(indexMetaData, null, ref.utf8ToString());
                            return shardId == targetShardId;
                        }, leafReader, bitSet::set);
                        // now if we have a mixed index where some docs have a _routing value and some don't we have to exclude the ones
                        // with a routing value from the next iteration an delete / select based on the ID.
                        if (terms.getDocCount() != leafReader.maxDoc()) {
                            // this is a special case where some of the docs have no routing values this sucks but it's possible today
                            FixedBitSet hasRoutingValue = new FixedBitSet(leafReader.maxDoc());
                            findSplitDocs(RoutingFieldMapper.NAME, ref -> false, leafReader,
                                    hasRoutingValue::set);
                            findSplitDocs(IdFieldMapper.NAME, includeInShard, leafReader, docId -> {
                                if (hasRoutingValue.get(docId) == false) {
                                    bitSet.set(docId);
                                }
                            });
                        }
                    }
                }
                return new ConstantScoreScorer(this, score(), new BitSetIterator(bitSet, bitSet.length()));
            }


        };
    }

    @Override
    public String toString(String field) {
        return "shard_splitting_query";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShardSplittingQuery that = (ShardSplittingQuery) o;

        if (shardId != that.shardId) return false;
        return indexMetaData.equals(that.indexMetaData);
    }

    @Override
    public int hashCode() {
        int result = indexMetaData.hashCode();
        result = 31 * result + shardId;
        return classHash() ^ result;
    }

    private static void findSplitDocs(String idField, Predicate<BytesRef> includeInShard,
                                      LeafReader leafReader, IntConsumer consumer) throws IOException {
        Terms terms = leafReader.terms(idField);
        TermsEnum iterator = terms.iterator();
        BytesRef idTerm;
        PostingsEnum postingsEnum = null;
        while ((idTerm = iterator.next()) != null) {
            if (includeInShard.test(idTerm) == false) {
                postingsEnum = iterator.postings(postingsEnum);
                int doc;
                while ((doc = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    consumer.accept(doc);
                }
            }
        }
    }

    private static final class Visitor extends StoredFieldVisitor {
        int leftToVisit = 2;
        final BytesRef spare = new BytesRef();
        String routing;
        String id;

        void reset() {
            routing = id = null;
            leftToVisit = 2;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
            switch (fieldInfo.name) {
                case IdFieldMapper.NAME:
                    id = Uid.decodeId(value);
                    break;
                default:
                    throw new IllegalStateException("Unexpected field: " + fieldInfo.name);
            }
        }

        @Override
        public void stringField(FieldInfo fieldInfo, byte[] value) throws IOException {
            spare.bytes = value;
            spare.offset = 0;
            spare.length = value.length;
            switch (fieldInfo.name) {
                case RoutingFieldMapper.NAME:
                    routing = spare.utf8ToString();
                    break;
                default:
                    throw new IllegalStateException("Unexpected field: " + fieldInfo.name);
            }
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) throws IOException {
            // we don't support 5.x so no need for the uid field
            switch (fieldInfo.name) {
                case IdFieldMapper.NAME:
                case RoutingFieldMapper.NAME:
                    leftToVisit--;
                    return Status.YES;
                default:
                    return leftToVisit == 0 ? Status.STOP : Status.NO;
            }
        }
    }

    /**
     * This two phase iterator visits every live doc and selects all docs that don't belong into this
     * shard based on their id and routing value. This is only used in a routing partitioned index.
     */
    private final class RoutingPartitionedDocIdSetIterator extends TwoPhaseIterator {
        private final LeafReader leafReader;
        private final Visitor visitor;

        RoutingPartitionedDocIdSetIterator(LeafReader leafReader, Visitor visitor) {
            super(DocIdSetIterator.all(leafReader.maxDoc())); // we iterate all live-docs
            this.leafReader = leafReader;
            this.visitor = visitor;
        }

        @Override
        public boolean matches() throws IOException {
            int doc = approximation.docID();
            visitor.reset();
            leafReader.document(doc, visitor);
            int targetShardId = OperationRouting.generateShardId(indexMetaData, visitor.id, visitor.routing);
            return targetShardId != shardId;
        }

        @Override
        public float matchCost() {
            return 42; // that's obvious, right?
        }
    }
}



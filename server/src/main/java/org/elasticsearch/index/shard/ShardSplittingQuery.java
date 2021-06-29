/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.Predicate;

/**
 * A query that selects all docs that do NOT belong in the current shards this query is executed on.
 * It can be used to split a shard into N shards marking every document that doesn't belong into the shard
 * as deleted. See {@link org.apache.lucene.index.IndexWriter#deleteDocuments(Query...)}
 */
final class ShardSplittingQuery extends Query {
    private final IndexMetadata indexMetadata;
    private final int shardId;
    private final BitSetProducer nestedParentBitSetProducer;

    ShardSplittingQuery(IndexMetadata indexMetadata, int shardId, boolean hasNested) {
        this.indexMetadata = indexMetadata;
        this.shardId = shardId;
        this.nestedParentBitSetProducer =  hasNested ? newParentDocBitSetProducer() : null;
    }
    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
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
                    int targetShardId = OperationRouting.generateShardId(indexMetadata,
                        Uid.decodeId(ref.bytes, ref.offset, ref.length), null);
                    return shardId == targetShardId;
                };
                if (terms == null) {
                    // this is the common case - no partitioning and no _routing values
                    // in this case we also don't do anything special with regards to nested docs since we basically delete
                    // by ID and parent and nested all have the same id.
                    assert indexMetadata.isRoutingPartitionedIndex() == false;
                    findSplitDocs(IdFieldMapper.NAME, includeInShard, leafReader, bitSet::set);
                } else {
                    final BitSet parentBitSet;
                    if (nestedParentBitSetProducer == null) {
                        parentBitSet = null;
                    } else {
                        parentBitSet = nestedParentBitSetProducer.getBitSet(context);
                        if (parentBitSet == null) {
                            return null; // no matches
                        }
                    }
                    if (indexMetadata.isRoutingPartitionedIndex()) {
                        // this is the heaviest invariant. Here we have to visit all docs stored fields do extract _id and _routing
                        // this this index is routing partitioned.
                        Visitor visitor = new Visitor(leafReader);
                        TwoPhaseIterator twoPhaseIterator =
                            parentBitSet == null ? new RoutingPartitionedDocIdSetIterator(visitor) :
                                new NestedRoutingPartitionedDocIdSetIterator(visitor, parentBitSet);
                        return new ConstantScoreScorer(this, score(), scoreMode, twoPhaseIterator);
                    } else {
                        // here we potentially guard the docID consumers with our parent bitset if we have one.
                        // this ensures that we are only marking root documents in the nested case and if necessary
                        // we do a second pass to mark the corresponding children in markChildDocs
                        Function<IntConsumer, IntConsumer> maybeWrapConsumer = consumer -> {
                            if (parentBitSet != null) {
                                return docId -> {
                                    if (parentBitSet.get(docId)) {
                                        consumer.accept(docId);
                                    }
                                };
                            }
                            return consumer;
                        };
                        // in the _routing case we first go and find all docs that have a routing value and mark the ones we have to delete
                        findSplitDocs(RoutingFieldMapper.NAME, ref -> {
                            int targetShardId = OperationRouting.generateShardId(indexMetadata, null, ref.utf8ToString());
                            return shardId == targetShardId;
                        }, leafReader, maybeWrapConsumer.apply(bitSet::set));

                        // now if we have a mixed index where some docs have a _routing value and some don't we have to exclude the ones
                        // with a routing value from the next iteration an delete / select based on the ID.
                        if (terms.getDocCount() != leafReader.maxDoc()) {
                            // this is a special case where some of the docs have no routing values this sucks but it's possible today
                            FixedBitSet hasRoutingValue = new FixedBitSet(leafReader.maxDoc());
                            findSplitDocs(RoutingFieldMapper.NAME, ref -> false, leafReader, maybeWrapConsumer.apply(hasRoutingValue::set));
                            IntConsumer bitSetConsumer = maybeWrapConsumer.apply(bitSet::set);
                            findSplitDocs(IdFieldMapper.NAME, includeInShard, leafReader, docId -> {
                                if (hasRoutingValue.get(docId) == false) {
                                    bitSetConsumer.accept(docId);
                                }
                            });
                        }
                    }
                    if (parentBitSet != null) {
                        // if nested docs are involved we also need to mark all child docs that belong to a matching parent doc.
                        markChildDocs(parentBitSet, bitSet);
                    }
                }

                return new ConstantScoreScorer(this, score(), scoreMode, new BitSetIterator(bitSet, bitSet.length()));
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // This is not a regular query, let's not cache it. It wouldn't help
                // anyway.
                return false;
            }
        };
    }

    private void markChildDocs(BitSet parentDocs, BitSet matchingDocs) {
        int currentDeleted = 0;
        while (currentDeleted < matchingDocs.length() &&
            (currentDeleted = matchingDocs.nextSetBit(currentDeleted)) != DocIdSetIterator.NO_MORE_DOCS) {
            int previousParent = parentDocs.prevSetBit(Math.max(0, currentDeleted-1));
            for (int i = previousParent + 1; i < currentDeleted; i++) {
                matchingDocs.set(i);
            }
            currentDeleted++;
        }
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
        return indexMetadata.equals(that.indexMetadata);
    }

    @Override
    public int hashCode() {
        int result = indexMetadata.hashCode();
        result = 31 * result + shardId;
        return classHash() ^ result;
    }

    private static void findSplitDocs(String idField, Predicate<BytesRef> includeInShard, LeafReader leafReader,
                                      IntConsumer consumer) throws IOException {
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

    /* this class is a stored fields visitor that reads _id and/or _routing from the stored fields which is necessary in the case
       of a routing partitioned index sine otherwise we would need to un-invert the _id and _routing field which is memory heavy */
    private final class Visitor extends StoredFieldVisitor {
        final LeafReader leafReader;
        private int leftToVisit = 2;
        private final BytesRef spare = new BytesRef();
        private String routing;
        private String id;

        Visitor(LeafReader leafReader) {
            this.leafReader = leafReader;
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

        boolean matches(int doc) throws IOException {
            routing = id = null;
            leftToVisit = 2;
            leafReader.document(doc, this);
            assert id != null : "docID must not be null - we might have hit a nested document";
            int targetShardId = OperationRouting.generateShardId(indexMetadata, id, routing);
            return targetShardId != shardId;
        }
    }

    /**
     * This two phase iterator visits every live doc and selects all docs that don't belong into this
     * shard based on their id and routing value. This is only used in a routing partitioned index.
     */
    private static final class RoutingPartitionedDocIdSetIterator extends TwoPhaseIterator {
        private final Visitor visitor;

        RoutingPartitionedDocIdSetIterator(Visitor visitor) {
            super(DocIdSetIterator.all(visitor.leafReader.maxDoc())); // we iterate all live-docs
            this.visitor = visitor;
        }

        @Override
        public boolean matches() throws IOException {
            return visitor.matches(approximation.docID());
        }

        @Override
        public float matchCost() {
            return 42; // that's obvious, right?
        }
    }

    /**
     * This TwoPhaseIterator marks all nested docs of matching parents as matches as well.
     */
    private static final class NestedRoutingPartitionedDocIdSetIterator extends TwoPhaseIterator {
        private final Visitor visitor;
        private final BitSet parentDocs;
        private int nextParent = -1;
        private boolean nextParentMatches;

        NestedRoutingPartitionedDocIdSetIterator(Visitor visitor, BitSet parentDocs) {
            super(DocIdSetIterator.all(visitor.leafReader.maxDoc())); // we iterate all live-docs
            this.parentDocs = parentDocs;
            this.visitor = visitor;
        }

        @Override
        public boolean matches() throws IOException {
            // the educated reader might ask why this works, it does because all live doc ids (root docs and nested docs) are evaluated in
            // order and that way we don't need to seek backwards as we do in other nested docs cases.
            int doc = approximation.docID();
            if (doc > nextParent) {
                // we only check once per nested/parent set
                nextParent = parentDocs.nextSetBit(doc);
                // never check a child document against the visitor, they neihter have _id nor _routing as stored fields
                nextParentMatches = visitor.matches(nextParent);
            }
            return nextParentMatches;
        }

        @Override
        public float matchCost() {
            return 42; // that's obvious, right?
        }
    }

    /*
     * this is used internally to obtain a bitset for parent documents. We don't cache this since we never access the same reader more
     * than once. There is no point in using BitsetFilterCache#BitSetProducerWarmer since we use this only as a delete by query which is
     * executed on a recovery-private index writer. There is no point in caching it and it won't have a cache hit either.
     */
    private static BitSetProducer newParentDocBitSetProducer() {
        return context -> BitsetFilterCache.bitsetFromQuery(Queries.newNonNestedFilter(), context);
    }
}



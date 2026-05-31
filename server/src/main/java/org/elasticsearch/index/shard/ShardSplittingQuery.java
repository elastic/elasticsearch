/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.shard;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.Predicate;

/**
 * A query that selects all docs that do NOT belong in the current shards this query is executed on.
 * It can be used to split a shard into N shards marking every document that doesn't belong into the shard
 * as deleted. See {@link org.apache.lucene.index.IndexWriter#deleteDocuments(Query...)}
 */
public final class ShardSplittingQuery extends Query {
    private final IndexMetadata indexMetadata;
    private final BiPredicate<String, String> shardMatcher;
    private final int shardId;
    private final BitSetProducer nestedParentBitSetProducer;

    public ShardSplittingQuery(IndexMetadata indexMetadata, int shardId, boolean hasNested) {
        this.indexMetadata = indexMetadata;
        this.shardMatcher = IndexRouting.fromIndexMetadata(indexMetadata).shardMatcherForSplit(shardId);
        this.shardId = shardId;
        this.nestedParentBitSetProducer = hasNested ? newParentDocBitSetProducer(indexMetadata.getCreationVersion()) : null;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public String toString() {
                return "weight(delete docs query)";
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                LeafReader leafReader = context.reader();
                FixedBitSet bitSet = new FixedBitSet(leafReader.maxDoc());
                // Detect whether routing is stored as sorted doc values (no inverted index).
                FieldInfo routingFieldInfo = leafReader.getFieldInfos().fieldInfo(RoutingFieldMapper.NAME);
                boolean routingStoredAsDocValues = routingFieldInfo != null && routingFieldInfo.getDocValuesType() == DocValuesType.SORTED;

                Terms terms;
                DocValuesSkipper routingSkipper;
                if (routingStoredAsDocValues) {
                    routingSkipper = leafReader.getDocValuesSkipper(RoutingFieldMapper.NAME);
                    terms = null;
                } else {
                    routingSkipper = null;
                    terms = leafReader.terms(RoutingFieldMapper.NAME);
                }
                Predicate<BytesRef> idOnlyPredicate = ref -> {
                    // TODO IndexRouting should build the query somehow
                    String id = Uid.decodeId(ref.bytes, ref.offset, ref.length);
                    return shardMatcher.test(id, null);
                };

                return new ScorerSupplier() {
                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        if (terms == null && routingSkipper == null) {
                            // this is the common case - no partitioning and no _routing values
                            // in this case we also don't do anything special with regards to nested docs since we basically delete
                            // by ID and parent and nested all have the same id.
                            assert indexMetadata.isRoutingPartitionedIndex() == false;
                            findSplitDocsBasedOnId((idOnlyPredicate), leafReader, bitSet::set);
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
                                // this index is routing partitioned.
                                Visitor visitor = new Visitor(leafReader);
                                TwoPhaseIterator twoPhaseIterator = parentBitSet == null
                                    ? new RoutingPartitionedDocIdSetIterator(visitor)
                                    : new NestedRoutingPartitionedDocIdSetIterator(visitor, parentBitSet);
                                return new ConstantScoreScorer(score(), scoreMode, twoPhaseIterator);
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
                                // in the _routing case we first go and find all docs that have a routing value and mark the ones we have to
                                // delete
                                Predicate<BytesRef> routingOnlyPredicate = bytes -> shardMatcher.test(null, bytes.utf8ToString());
                                findSplitDocsBasedOnRouting(
                                    routingStoredAsDocValues,
                                    routingOnlyPredicate,
                                    leafReader,
                                    maybeWrapConsumer.apply(bitSet::set)
                                );

                                // TODO have the IndexRouting build the query and pass routingRequired in
                                boolean routingRequired = indexMetadata.mapping() == null
                                    ? false
                                    : indexMetadata.mapping().routingRequired();
                                // now if we have a mixed index where some docs have a _routing value and some don't we have to exclude the
                                // ones
                                // with a routing value from the next iteration and delete / select based on the ID.
                                int docCount = terms != null ? terms.getDocCount() : routingSkipper.docCount();
                                if (routingRequired == false && docCount != leafReader.maxDoc()) {
                                    /*
                                     * This is a special case where some docs don't have routing values.
                                     * It's annoying, but it's allowed to build an index where some documents
                                     * hve routing and others don't.
                                     *
                                     * Luckily, if the routing field is required in the mapping then we can
                                     * safely assume that all documents which are don't have a routing are
                                     * nested documents. And we pick those up later based on the assignment
                                     * of the document that contains them.
                                     */
                                    FixedBitSet hasRoutingValue = new FixedBitSet(leafReader.maxDoc());
                                    findSplitDocsBasedOnRouting(
                                        routingStoredAsDocValues,
                                        Predicates.never(),
                                        leafReader,
                                        maybeWrapConsumer.apply(hasRoutingValue::set)
                                    );
                                    IntConsumer bitSetConsumer = maybeWrapConsumer.apply(bitSet::set);
                                    findSplitDocsBasedOnId(idOnlyPredicate, leafReader, docId -> {
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

                        return new ConstantScoreScorer(score(), scoreMode, new BitSetIterator(bitSet, bitSet.length()));
                    }

                    @Override
                    public long cost() {
                        return leafReader.maxDoc();
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // This is not a regular query, let's not cache it. It wouldn't help
                // anyway.
                return false;
            }
        };
    }

    private static void markChildDocs(BitSet parentDocs, BitSet matchingDocs) {
        int currentDeleted = 0;
        while (currentDeleted < matchingDocs.length()
            && (currentDeleted = matchingDocs.nextSetBit(currentDeleted)) != DocIdSetIterator.NO_MORE_DOCS) {
            int previousParent = parentDocs.prevSetBit(Math.max(0, currentDeleted - 1));
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
        if (sameClassAs(o) == false) return false;

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

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    static void findSplitDocsBasedOnId(Predicate<BytesRef> includeInShard, LeafReader leafReader, IntConsumer consumer) throws IOException {
        Terms terms = leafReader.terms(IdFieldMapper.NAME);
        consumeLocalDocIds(includeInShard, consumer, terms);
    }

    static void findSplitDocsBasedOnRouting(
        boolean useDocValues,
        Predicate<BytesRef> includeInShard,
        LeafReader leafReader,
        IntConsumer consumer
    ) throws IOException {
        if (useDocValues) {
            SortedDocValues routingDocValues = leafReader.getSortedDocValues(RoutingFieldMapper.NAME);
            assert routingDocValues != null;
            for (int docID = routingDocValues.nextDoc(); docID != DocIdSetIterator.NO_MORE_DOCS; docID = routingDocValues.nextDoc()) {
                int ordinal = routingDocValues.ordValue();
                BytesRef value = routingDocValues.lookupOrd(ordinal);
                if (includeInShard.test(value) == false) {
                    consumer.accept(docID);
                }
            }
        } else {
            Terms terms = leafReader.terms(RoutingFieldMapper.NAME);
            consumeLocalDocIds(includeInShard, consumer, terms);
        }
    }

    private static void consumeLocalDocIds(Predicate<BytesRef> includeInShard, IntConsumer consumer, Terms terms) throws IOException {
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
       of a routing partitioned index sine otherwise we would need to un-invert the _id and _routing field which is memory heavy.
       When routing is stored as sorted doc values (doc_values: true), it is read from doc values instead of stored fields. */
    private final class Visitor {
        final LeafReader leafReader;
        final StoredFields storedFields;
        final BaseFieldVisitor storedFieldVisitor;
        final RoutingDocValuesReader routingDocValuesReader;
        private String routing;
        private String id;

        Visitor(LeafReader leafReader) throws IOException {
            this.leafReader = leafReader;
            this.storedFields = leafReader.storedFields();
            FieldInfo routingFieldInfo = leafReader.getFieldInfos().fieldInfo(RoutingFieldMapper.NAME);
            boolean routingHasDocValues = routingFieldInfo != null && routingFieldInfo.getDocValuesType() == DocValuesType.SORTED;
            if (routingHasDocValues) {
                this.storedFieldVisitor = new IdStoredFieldVisitor();
                this.routingDocValuesReader = new RoutingDocValuesReader(leafReader);
            } else {
                this.storedFieldVisitor = new IdAndRoutingStoredFieldVisitor();
                this.routingDocValuesReader = null;
            }
        }

        boolean matches(int doc) throws IOException {
            routing = id = null;
            storedFieldVisitor.reset();
            storedFields.document(doc, storedFieldVisitor);
            assert id != null : "docID must not be null - we might have hit a nested document";
            if (routingDocValuesReader != null) {
                assert routing == null;
                routing = routingDocValuesReader.read(doc);
            }
            return shardMatcher.test(id, routing) == false;
        }

        abstract class BaseFieldVisitor extends StoredFieldVisitor {

            final int fieldsToVisit;
            int leftToVisit;

            BaseFieldVisitor(int leftToVisit) {
                this.fieldsToVisit = leftToVisit;
                this.leftToVisit = leftToVisit;
            }

            void reset() {
                this.leftToVisit = fieldsToVisit;
            }

            @Override
            public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
                switch (fieldInfo.name) {
                    case IdFieldMapper.NAME -> id = Uid.decodeId(value);
                    default -> throw new IllegalStateException("Unexpected field: " + fieldInfo.name);
                }
            }
        }

        final class IdStoredFieldVisitor extends BaseFieldVisitor {

            IdStoredFieldVisitor() {
                super(1);
            }

            @Override
            public Status needsField(FieldInfo fieldInfo) throws IOException {
                // we don't support 5.x so no need for the uid field
                switch (fieldInfo.name) {
                    case IdFieldMapper.NAME:
                        leftToVisit--;
                        return Status.YES;
                    default:
                        return leftToVisit == 0 ? Status.STOP : Status.NO;
                }
            }

        }

        final class IdAndRoutingStoredFieldVisitor extends BaseFieldVisitor {

            IdAndRoutingStoredFieldVisitor() {
                super(2);
            }

            @Override
            public void stringField(FieldInfo fieldInfo, String value) throws IOException {
                switch (fieldInfo.name) {
                    case RoutingFieldMapper.NAME -> routing = value;
                    default -> throw new IllegalStateException("Unexpected field: " + fieldInfo.name);
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

        static final class RoutingDocValuesReader {

            final SortedDocValues docValues;

            RoutingDocValuesReader(LeafReader leafReader) throws IOException {
                this.docValues = leafReader.getSortedDocValues(RoutingFieldMapper.NAME);
            }

            public String read(int docId) throws IOException {
                if (docValues.advanceExact(docId)) {
                    // TODO: maybe cache looking up ordinal and converting to utf8 string?
                    int ordinal = docValues.ordValue();
                    return docValues.lookupOrd(ordinal).utf8ToString();
                }
                return null;
            }

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
    private static BitSetProducer newParentDocBitSetProducer(IndexVersion indexCreationVersion) {
        return context -> BitsetFilterCache.bitsetFromQuery(Queries.newNonNestedFilter(indexCreationVersion), context);
    }
}

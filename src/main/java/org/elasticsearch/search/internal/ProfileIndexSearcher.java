package org.elasticsearch.search.internal;

import com.google.common.base.Stopwatch;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.index.engine.Engine;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class ProfileIndexSearcher extends ContextIndexSearcher implements Releasable {

    private Stopwatch stopwatch;


    public ProfileIndexSearcher(SearchContext searchContext, Engine.Searcher searcher) {
        super(searchContext, searcher);
        stopwatch = Stopwatch.createUnstarted();
    }

    @Override
    public void close() {
    }

    @Override
    public Query rewrite(Query original) throws IOException {
        startTime();
        Query rewritten = super.rewrite(original);
        stopAndRecordTime(Timing.REWRITE, original);

        return rewritten;
    }

    @Override
    public Weight createNormalizedWeight(Query query, boolean needsScores) throws IOException {
        startTime();
        Weight weight = super.createNormalizedWeight(query, needsScores);
        stopAndRecordTime(Timing.EXECUTION, query);

        return new ProfileWeight(query, weight, this);
    }


    

    public enum Timing {
        REWRITE, EXECUTION, ALL
    }

    public void startTime() {
        stopwatch.start();
    }

    public void stopAndRecordTime(Timing timing, Query query) {
        stopwatch.stop();
        long time = stopwatch.elapsed(TimeUnit.MICROSECONDS);
        stopwatch.reset();

        //timings.put(query, time);

        switch (timing) {
            case REWRITE:
                //rewriteTime += time;
                break;
            case EXECUTION:
                //executionTime += time;
                break;
            case ALL:
                throw new ElasticsearchIllegalArgumentException("Must setTime for either REWRITE or EXECUTION timing.");
            default:
                throw new ElasticsearchIllegalArgumentException("Must setTime for either REWRITE or EXECUTION timing.");
        }
    }
    
    
    
    class ProfiledComponent {
        private long rewriteTime = 0;
        private long executionTime = 0;


    }



    class ProfileWeight extends Weight {

        final Weight subQueryWeight;
        private final ProfileIndexSearcher pSearcher;

        public ProfileWeight(Query query, Weight subQueryWeight, ProfileIndexSearcher pSearcher) throws IOException {
            super(query);
            this.subQueryWeight = subQueryWeight;
            this.pSearcher = pSearcher;
        }

        @Override
        public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
            Scorer subQueryScorer = subQueryWeight.scorer(context, acceptDocs);
            if (subQueryScorer == null) {
                return null;
            }

            return new ProfileScorer(this, subQueryScorer, pSearcher, getQuery());
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
            pSearcher.startTime();
            BulkScorer bScorer = subQueryWeight.bulkScorer(context, acceptDocs);
            pSearcher.stopAndRecordTime(Timing.EXECUTION, getQuery());
            return new ProfileBulkScorer(bScorer, pSearcher, getQuery());
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return subQueryWeight.explain(context, doc);
        }

        @Override
        public float getValueForNormalization() throws IOException {
            pSearcher.startTime();
            float sum = subQueryWeight.getValueForNormalization();
            pSearcher.stopAndRecordTime(Timing.EXECUTION, getQuery());

            return sum;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
            pSearcher.startTime();
            subQueryWeight.normalize(norm, topLevelBoost);
            pSearcher.stopAndRecordTime(Timing.EXECUTION, getQuery());
        }
    }




    static class ProfileScorer extends Scorer {

        private final Scorer scorer;
        private ProfileWeight profileWeight;
        private final ProfileIndexSearcher pSearcher;
        private final Query query;

        private ProfileScorer(ProfileWeight w, Scorer scorer, ProfileIndexSearcher pSearcher, Query query) throws IOException {
            super(w);
            this.scorer = scorer;
            this.profileWeight = w;
            this.pSearcher = pSearcher;
            this.query = query;
        }

        @Override
        public int docID() {
            return scorer.docID();
        }

        @Override
        public int advance(int target) throws IOException {
            pSearcher.startTime();
            int id = scorer.advance(target);
            pSearcher.stopAndRecordTime(Timing.EXECUTION, query);
            return id;
        }

        @Override
        public int nextDoc() throws IOException {
            pSearcher.startTime();
            int docId = scorer.nextDoc();
            pSearcher.stopAndRecordTime(Timing.EXECUTION, query);

            return docId;
        }

        @Override
        public float score() throws IOException {
            pSearcher.startTime();
            float score = scorer.score();
            pSearcher.stopAndRecordTime(Timing.EXECUTION, query);

            return score;
        }

        @Override
        public int freq() throws IOException {
            return scorer.freq();
        }

        @Override
        public long cost() {
            return scorer.cost();
        }
    }

    static class ProfileBulkScorer extends BulkScorer {

        private final ProfileIndexSearcher pSearcher;
        private final Query query;
        private final BulkScorer bulkScorer;

        public ProfileBulkScorer(BulkScorer bulkScorer, ProfileIndexSearcher pSearcher, Query query) {
            super();
            this.bulkScorer = bulkScorer;
            this.pSearcher = pSearcher;
            this.query = query;
        }

        @Override
        public void score(LeafCollector collector) throws IOException {
            pSearcher.startTime();
            bulkScorer.score(collector);
            pSearcher.stopAndRecordTime(Timing.EXECUTION, query);
        }

        @Override
        public int score(LeafCollector collector, int min, int max) throws IOException {
            pSearcher.startTime();
            int score = bulkScorer.score(collector, min, max);
            pSearcher.stopAndRecordTime(Timing.EXECUTION, query);
            return score;
        }

        @Override
        public long cost() {
            pSearcher.startTime();
            long cost = bulkScorer.cost();
            pSearcher.stopAndRecordTime(Timing.EXECUTION, query);
            return cost;
        }
    }


}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.rescorer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toUnmodifiableSet;

public class InferenceRescorer implements Rescorer {

    public static final InferenceRescorer INSTANCE = new InferenceRescorer();
    private static final Logger logger = LogManager.getLogger(InferenceRescorer.class);

    private static final Comparator<ScoreDoc> SCORE_DOC_COMPARATOR = (o1, o2) -> {
        int cmp = Float.compare(o2.score, o1.score);
        return cmp == 0 ? Integer.compare(o1.doc, o2.doc) : cmp;
    };

    private InferenceRescorer() {

    }

    @Override
    public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException {
        if (topDocs.scoreDocs.length == 0) {
            return topDocs;
        }
        InferenceRescorerContext ltrRescoreContext = (InferenceRescorerContext) rescoreContext;
        if (ltrRescoreContext.inferenceDefinition == null) {
            throw new IllegalStateException("local model reference is null, missing rewriteAndFetch before rescore phase?");
        }
        LocalModel definition = ltrRescoreContext.inferenceDefinition;

        // First take top slice of incoming docs, to be rescored:
        TopDocs topNFirstPass = topN(topDocs, rescoreContext.getWindowSize());
        // Save doc IDs for which rescoring was applied to be used in score explanation
        Set<Integer> topNDocIDs = Arrays.stream(topNFirstPass.scoreDocs).map(scoreDoc -> scoreDoc.doc).collect(toUnmodifiableSet());
        rescoreContext.setRescoredDocs(topNDocIDs);
        ScoreDoc[] hitsToRescore = topNFirstPass.scoreDocs;
        Arrays.sort(hitsToRescore, Comparator.comparingInt(a -> a.doc));
        int hitUpto = 0;
        int readerUpto = -1;
        int endDoc = 0;
        int docBase = 0;
        List<LeafReaderContext> leaves = ltrRescoreContext.executionContext.searcher().getIndexReader().leaves();
        LeafReaderContext currentSegment = null;
        boolean changedSegment = true;
        List<FeatureExtractor> featureExtractors = ltrRescoreContext.buildFeatureExtractors(searcher);
        List<Map<String, Object>> docFeatures = new ArrayList<>(topNDocIDs.size());
        int featureSize = featureExtractors.stream().mapToInt(fe -> fe.featureNames().size()).sum();
        while (hitUpto < hitsToRescore.length) {
            final ScoreDoc hit = hitsToRescore[hitUpto];
            final int docID = hit.doc;
            while (docID >= endDoc) {
                readerUpto++;
                currentSegment = leaves.get(readerUpto);
                endDoc = currentSegment.docBase + currentSegment.reader().maxDoc();
                changedSegment = true;
            }
            assert currentSegment != null : "Unexpected null segment";
            if (changedSegment) {
                // We advanced to another segment and update our document value fetchers
                docBase = currentSegment.docBase;
                for (FeatureExtractor featureExtractor : featureExtractors) {
                    featureExtractor.setNextReader(currentSegment);
                }
                changedSegment = false;
            }
            int targetDoc = docID - docBase;
            Map<String, Object> features = Maps.newMapWithExpectedSize(featureSize);
            for (FeatureExtractor featureExtractor : featureExtractors) {
                featureExtractor.addFeatures(features, targetDoc);
            }
            logger.debug(() -> Strings.format("doc [%d] has features [%s]", targetDoc, features));
            docFeatures.add(features);
            hitUpto++;
        }
        for (int i = 0; i < hitsToRescore.length; i++) {
            Map<String, Object> features = docFeatures.get(i);
            try {
                InferenceResults results = definition.inferLtr(features, ltrRescoreContext.inferenceConfig);
                if (results instanceof WarningInferenceResults warningInferenceResults) {
                    logger.warn("Failure rescoring doc, warning returned [" + warningInferenceResults.getWarning() + "]");
                } else if (results.predictedValue() instanceof Number prediction) {
                    hitsToRescore[i].score = prediction.floatValue();
                } else {
                    logger.warn("Failure rescoring doc, unexpected inference result of kind [" + results.getWriteableName() + "]");
                }
            } catch (Exception ex) {
                logger.warn("Failure rescoring doc...", ex);
            }
        }
        assert rescoreContext.getWindowSize() >= hitsToRescore.length
            : "unexpected, windows size [" + rescoreContext.getWindowSize() + "] should be gte [" + hitsToRescore.length + "]";

        Arrays.sort(topDocs.scoreDocs, SCORE_DOC_COMPARATOR);
        return topDocs;
    }

    @Override
    public Explanation explain(int topLevelDocId, IndexSearcher searcher, RescoreContext rescoreContext, Explanation sourceExplanation)
        throws IOException {
        // TODO: Call infer again but with individual feature importance values and explaining the model (which features are used, etc.)
        return null;
    }

    /** Returns a new {@link TopDocs} with the topN from the incoming one, or the same TopDocs if the number of hits is already &lt;=
     *  topN. */
    private static TopDocs topN(TopDocs in, int topN) {
        if (in.scoreDocs.length < topN) {
            return in;
        }

        ScoreDoc[] subset = new ScoreDoc[topN];
        System.arraycopy(in.scoreDocs, 0, subset, 0, topN);

        return new TopDocs(in.totalHits, subset);
    }
}

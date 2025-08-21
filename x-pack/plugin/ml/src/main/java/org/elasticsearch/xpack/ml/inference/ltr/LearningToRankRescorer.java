/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ltr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toUnmodifiableSet;

public class LearningToRankRescorer implements Rescorer {

    private static final int MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK = 10;
    public static final LearningToRankRescorer INSTANCE = new LearningToRankRescorer();
    private static final Logger logger = LogManager.getLogger(LearningToRankRescorer.class);

    private static final Comparator<ScoreDoc> SCORE_DOC_COMPARATOR = (o1, o2) -> {
        int cmp = Float.compare(o2.score, o1.score);
        return cmp == 0 ? Integer.compare(o1.doc, o2.doc) : cmp;
    };

    private LearningToRankRescorer() {

    }

    @Override
    public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException {
        if (topDocs.scoreDocs.length == 0) {
            return topDocs;
        }
        LearningToRankRescorerContext ltrRescoreContext = (LearningToRankRescorerContext) rescoreContext;
        if (ltrRescoreContext.regressionModelDefinition == null) {
            throw new IllegalStateException("local model reference is null, missing rewriteAndFetch before rescore phase?");
        }

        LocalModel definition = ltrRescoreContext.regressionModelDefinition;

        // Because scores of the first-pass query and the LTR model are not comparable, there is no way to combine the results.
        // We will truncate the {@link TopDocs} to the window size so rescoring will be done on the full topDocs.
        topDocs = topN(topDocs, rescoreContext.getWindowSize());

        // Save doc IDs for which rescoring was applied to be used in score explanation
        Set<Integer> topDocIDs = Arrays.stream(topDocs.scoreDocs).map(scoreDoc -> scoreDoc.doc).collect(toUnmodifiableSet());
        rescoreContext.setRescoredDocs(topDocIDs);
        ScoreDoc[] hitsToRescore = topDocs.scoreDocs;
        Arrays.sort(hitsToRescore, Comparator.comparingInt(a -> a.doc));
        int hitUpto = 0;
        int readerUpto = -1;
        int endDoc = 0;
        int docBase = 0;
        List<LeafReaderContext> leaves = ltrRescoreContext.executionContext.searcher().getIndexReader().leaves();
        LeafReaderContext currentSegment = null;
        boolean changedSegment = true;
        List<FeatureExtractor> featureExtractors = ltrRescoreContext.buildFeatureExtractors(searcher);
        List<Map<String, Object>> docFeatures = new ArrayList<>(topDocIDs.size());
        int featureSize = featureExtractors.stream().mapToInt(fe -> fe.featureNames().size()).sum();
        int count = 0;
        while (hitUpto < hitsToRescore.length) {
            if (count % MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK == 0) {
                rescoreContext.checkCancellation();
            }
            count++;
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
            if (i % MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK == 0) {
                rescoreContext.checkCancellation();
            }
            Map<String, Object> features = docFeatures.get(i);
            try {
                InferenceResults results = definition.inferLtr(features, ltrRescoreContext.learningToRankConfig);
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
        if (sourceExplanation == null) {
            return Explanation.noMatch("no match found");
        }

        LearningToRankRescorerContext ltrContext = (LearningToRankRescorerContext) rescoreContext;
        LocalModel localModelDefinition = ltrContext.regressionModelDefinition;

        if (localModelDefinition == null) {
            throw new IllegalStateException("local model reference is null, missing rewriteAndFetch before rescore phase?");
        }

        List<LeafReaderContext> leaves = ltrContext.executionContext.searcher().getIndexReader().leaves();

        int endDoc = 0;
        int readerUpto = -1;
        LeafReaderContext currentSegment = null;

        while (topLevelDocId >= endDoc) {
            readerUpto++;
            currentSegment = leaves.get(readerUpto);
            endDoc = currentSegment.docBase + currentSegment.reader().maxDoc();
        }

        assert currentSegment != null : "Unexpected null segment";

        int targetDoc = topLevelDocId - currentSegment.docBase;

        List<FeatureExtractor> featureExtractors = ltrContext.buildFeatureExtractors(searcher);
        int featureSize = featureExtractors.stream().mapToInt(fe -> fe.featureNames().size()).sum();

        Map<String, Object> features = Maps.newMapWithExpectedSize(featureSize);

        for (FeatureExtractor featureExtractor : featureExtractors) {
            featureExtractor.setNextReader(currentSegment);
            featureExtractor.addFeatures(features, targetDoc);
        }

        // Predicting the value
        var ltrScore = ((Number) localModelDefinition.inferLtr(features, ltrContext.learningToRankConfig).predictedValue()).floatValue();

        List<Explanation> featureExplanations = new ArrayList<>();
        for (String featureName : features.keySet()) {
            Number featureValue = Objects.requireNonNullElse((Number) features.get(featureName), 0);
            featureExplanations.add(Explanation.match(featureValue, "feature value for [" + featureName + "]"));
        }

        return Explanation.match(
            ltrScore,
            "rescored using LTR model " + ltrContext.regressionModelDefinition.getModelId(),
            Explanation.match(sourceExplanation.getValue(), "first pass query score", sourceExplanation),
            Explanation.match(0f, "extracted features", featureExplanations)
        );
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

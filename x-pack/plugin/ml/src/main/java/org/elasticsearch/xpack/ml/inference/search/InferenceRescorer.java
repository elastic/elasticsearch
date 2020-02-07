/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.SingleValueInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InferenceRescorer implements Rescorer {

    private static final Logger logger = LogManager.getLogger(InferenceRescorer.class);

    private final LocalModel model;
    private final InferenceConfig inferenceConfig;
    private final Map<String, String> fieldMap;
    private final InferenceRescorerBuilder.ScoreModeSettings scoreModeSettings;

    InferenceRescorer(
        LocalModel model,
        InferenceConfig inferenceConfig,
        Map<String, String> fieldMap,
        InferenceRescorerBuilder.ScoreModeSettings scoreModeSettings
    ) {
        this.model = model;
        this.inferenceConfig = inferenceConfig;
        this.fieldMap = fieldMap;
        this.scoreModeSettings = scoreModeSettings;

        assert inferenceConfig instanceof RegressionConfig;
    }

    @Override
    public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException {

        // Copy ScoreDoc[] and sort by ascending docID:
        ScoreDoc[] sortedHits = topDocs.scoreDocs.clone();
        Comparator<ScoreDoc> docIdComparator = Comparator.comparingInt(sd -> sd.doc);
        Arrays.sort(sortedHits, docIdComparator);

        // field map is fieldname in doc -> fieldname expected by model
        Set<String> fieldsToRead = new HashSet<>(model.getFieldNames());
        for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
            if (fieldsToRead.contains(entry.getValue())) {
                // replace the model fieldname with the doc fieldname
                fieldsToRead.remove(entry.getValue());
                fieldsToRead.add(entry.getKey());
            }
        }

        List<LeafReaderContext> leaves = searcher.getIndexReader().getContext().leaves();
        Map<String, Object> fields = new HashMap<>();

        int currentReader = 0;
        int endDoc = 0;
        LeafReaderContext readerContext = null;

        for (int hitIndex = 0; hitIndex < sortedHits.length; hitIndex++) {
            ScoreDoc hit = sortedHits[hitIndex];
            int docId = hit.doc;

            while (docId >= endDoc) {
                readerContext = leaves.get(currentReader);
                currentReader++;
                endDoc = readerContext.docBase + readerContext.reader().maxDoc();
            }

            for (String field : fieldsToRead) {
                SortedNumericDocValues docValuesIter = DocValues.getSortedNumeric(readerContext.reader(), field);
                SortedNumericDoubleValues doubles = FieldData.sortableLongBitsToDoubles(docValuesIter);
                if (doubles.advanceExact(hit.doc)) {
                    double val = doubles.nextValue();
                    fields.put(fieldMap.getOrDefault(field, field), val);
                } else if (docValuesIter.docID() == DocIdSetIterator.NO_MORE_DOCS) {
                    logger.warn("No more docs for field {}, doc {}", field, hit.doc);
                    fields.remove(field);
                } else {
                    logger.warn("no value for field {}, doc {}", field, hit.doc);
                    fields.remove(field);
                }
            }

            InferenceResults infer = model.infer(fields, inferenceConfig);
            if (infer instanceof WarningInferenceResults) {
                logger.warn("inference error: " + ((WarningInferenceResults) infer).getWarning());
                // TODO how to propagate this error
            } else {
                SingleValueInferenceResults regressionResult = (SingleValueInferenceResults) infer;

                float combinedScore = scoreModeSettings.scoreMode.combine(
                    hit.score * scoreModeSettings.queryWeight,
                    regressionResult.value().floatValue() * scoreModeSettings.modelWeight
                );

                sortedHits[hitIndex] = new ScoreDoc(hit.doc, combinedScore);
            }
        }

        return new TopDocs(topDocs.totalHits, sortedHits);
    }

    @Override
    public Explanation explain(int topLevelDocId, IndexSearcher searcher, RescoreContext rescoreContext, Explanation sourceExplanation) {
        return Explanation.match(1.0, "because");
    }
}

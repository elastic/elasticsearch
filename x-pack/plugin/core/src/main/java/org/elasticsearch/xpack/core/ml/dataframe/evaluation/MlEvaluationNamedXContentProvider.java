/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification.AucRoc;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification.BinarySoftClassification;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification.ConfusionMatrix;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification.Precision;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification.Recall;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification.ScoreByThresholdResult;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.softclassification.SoftClassificationMetric;

import java.util.ArrayList;
import java.util.List;

public class MlEvaluationNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();

        // Evaluations
        namedXContent.add(new NamedXContentRegistry.Entry(Evaluation.class, BinarySoftClassification.NAME,
            BinarySoftClassification::fromXContent));

        // Soft classification metrics
        namedXContent.add(new NamedXContentRegistry.Entry(SoftClassificationMetric.class, AucRoc.NAME, AucRoc::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(SoftClassificationMetric.class, Precision.NAME, Precision::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(SoftClassificationMetric.class, Recall.NAME, Recall::fromXContent));
        namedXContent.add(new NamedXContentRegistry.Entry(SoftClassificationMetric.class, ConfusionMatrix.NAME,
            ConfusionMatrix::fromXContent));

        return namedXContent;
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();

        // Evaluations
        namedWriteables.add(new NamedWriteableRegistry.Entry(Evaluation.class, BinarySoftClassification.NAME.getPreferredName(),
            BinarySoftClassification::new));

        // Evaluation Metrics
        namedWriteables.add(new NamedWriteableRegistry.Entry(SoftClassificationMetric.class, AucRoc.NAME.getPreferredName(),
            AucRoc::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(SoftClassificationMetric.class, Precision.NAME.getPreferredName(),
            Precision::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(SoftClassificationMetric.class, Recall.NAME.getPreferredName(),
            Recall::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(SoftClassificationMetric.class, ConfusionMatrix.NAME.getPreferredName(),
            ConfusionMatrix::new));

        // Evaluation Metrics Results
        namedWriteables.add(new NamedWriteableRegistry.Entry(EvaluationMetricResult.class, AucRoc.NAME.getPreferredName(),
            AucRoc.Result::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(EvaluationMetricResult.class, ScoreByThresholdResult.NAME,
            ScoreByThresholdResult::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(EvaluationMetricResult.class, ConfusionMatrix.NAME.getPreferredName(),
            ConfusionMatrix.Result::new));

        return namedWriteables;
    }
}

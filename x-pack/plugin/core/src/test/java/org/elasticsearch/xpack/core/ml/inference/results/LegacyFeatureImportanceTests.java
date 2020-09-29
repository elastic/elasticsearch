/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class LegacyFeatureImportanceTests extends AbstractWireSerializingTestCase<LegacyFeatureImportance> {

    public static LegacyFeatureImportance createRandomInstance() {
        return createRandomInstance(randomBoolean());
    }

    public static LegacyFeatureImportance createRandomInstance(boolean hasClasses) {
        double importance = randomDouble();
        List<LegacyFeatureImportance.ClassImportance> classImportances = null;
        if (hasClasses) {
            classImportances = Stream.generate(() -> randomAlphaOfLength(10))
                .limit(randomLongBetween(2, 10))
                .map(featureName -> new LegacyFeatureImportance.ClassImportance(featureName, randomDouble()))
                .collect(Collectors.toList());

            importance = classImportances.stream().mapToDouble(LegacyFeatureImportance.ClassImportance::getImportance).map(Math::abs).sum();
        }
        return new LegacyFeatureImportance(randomAlphaOfLength(10), importance, classImportances);
    }

    @Override
    protected LegacyFeatureImportance createTestInstance() {
        return createRandomInstance();
    }

    @Override
    protected Writeable.Reader<LegacyFeatureImportance> instanceReader() {
        return LegacyFeatureImportance::new;
    }

    public void testClassificationConversion() {
        {
            ClassificationFeatureImportance classificationFeatureImportance = ClassificationFeatureImportanceTests.createRandomInstance();
            LegacyFeatureImportance legacyFeatureImportance = LegacyFeatureImportance.fromClassification(classificationFeatureImportance);
            ClassificationFeatureImportance convertedFeatureImportance = legacyFeatureImportance.forClassification();
            assertThat(convertedFeatureImportance, equalTo(classificationFeatureImportance));
        }
        {
            LegacyFeatureImportance legacyFeatureImportance = createRandomInstance(true);
            ClassificationFeatureImportance classificationFeatureImportance = legacyFeatureImportance.forClassification();
            LegacyFeatureImportance convertedFeatureImportance = LegacyFeatureImportance.fromClassification(
                classificationFeatureImportance);
            assertThat(convertedFeatureImportance, equalTo(legacyFeatureImportance));
        }
    }

    public void testRegressionConversion() {
        {
            RegressionFeatureImportance regressionFeatureImportance = RegressionFeatureImportanceTests.createRandomInstance();
            LegacyFeatureImportance legacyFeatureImportance = LegacyFeatureImportance.fromRegression(regressionFeatureImportance);
            RegressionFeatureImportance convertedFeatureImportance = legacyFeatureImportance.forRegression();
            assertThat(convertedFeatureImportance, equalTo(regressionFeatureImportance));
        }
        {
            LegacyFeatureImportance legacyFeatureImportance = createRandomInstance(false);
            RegressionFeatureImportance regressionFeatureImportance = legacyFeatureImportance.forRegression();
            LegacyFeatureImportance convertedFeatureImportance = LegacyFeatureImportance.fromRegression(regressionFeatureImportance);
            assertThat(convertedFeatureImportance, equalTo(legacyFeatureImportance));
        }
    }
}

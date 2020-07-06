/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.dataframe.process.crossvalidation;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class StratifiedCrossValidationSplitterTests extends ESTestCase {

    private static final int ROWS_COUNT = 500;

    private List<String> fields;
    private int dependentVariableIndex;
    private String dependentVariable;
    private long randomizeSeed;
    private Map<String, Long> classCounts;
    private String[] classValuesPerRow;
    private long trainingDocsCount;
    private long testDocsCount;

    @Before
    public void setUpTests() {
        int fieldCount = randomIntBetween(1, 5);
        fields = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            fields.add(randomAlphaOfLength(10));
        }
        dependentVariableIndex = randomIntBetween(0, fieldCount - 1);
        dependentVariable = fields.get(dependentVariableIndex);
        randomizeSeed = randomLong();

        long classA = 0;
        long classB = 0;
        long classC = 0;


        classValuesPerRow = new String[ROWS_COUNT];
        for (int i = 0; i < classValuesPerRow.length; i++) {
            double randomDouble = randomDoubleBetween(0.0, 1.0, true);
            if (randomDouble < 0.2) {
                classValuesPerRow[i] = "a";
                classA++;
            } else if (randomDouble < 0.5) {
                classValuesPerRow[i] = "b";
                classB++;
            } else {
                classValuesPerRow[i] = "c";
                classC++;
            }
        }

        classCounts = new HashMap<>();
        classCounts.put("a", classA);
        classCounts.put("b", classB);
        classCounts.put("c", classC);
    }

    public void testConstructor_GivenMissingDependentVariable() {
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> new StratifiedCrossValidationSplitter(
            Collections.emptyList(), "foo", Collections.emptyMap(), 100.0, 0));
        assertThat(e.getMessage(), equalTo("Could not find dependent variable [foo] in fields []"));
    }

    public void testProcess_GivenUnknownClass() {
        CrossValidationSplitter splitter = createSplitter(100.0);
        String[] row = new String[fields.size()];
        for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
            row[fieldIndex] = randomAlphaOfLength(5);
        }
        row[dependentVariableIndex] = "unknown_class";

        IllegalStateException e = expectThrows(IllegalStateException.class,
            () -> splitter.process(row, this::incrementTrainingDocsCount, this::incrementTestDocsCount));

        assertThat(e.getMessage(), equalTo("Unknown class [unknown_class]; expected one of [a, b, c]"));
    }

    public void testProcess_GivenRowsWithoutDependentVariableValue() {
        CrossValidationSplitter splitter = createSplitter(50.0);

        for (int i = 0; i < classValuesPerRow.length; i++) {
            String[] row = new String[fields.size()];
            for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                String value = fieldIndex == dependentVariableIndex ? DataFrameDataExtractor.NULL_VALUE : randomAlphaOfLength(10);
                row[fieldIndex] = value;
            }

            String[] processedRow = Arrays.copyOf(row, row.length);
            splitter.process(processedRow, this::incrementTrainingDocsCount, this::incrementTestDocsCount);

            // As all these rows have no dependent variable value, they're not for training and should be unaffected
            assertThat(Arrays.equals(processedRow, row), is(true));
        }
        assertThat(trainingDocsCount, equalTo(0L));
        assertThat(testDocsCount, equalTo(500L));
    }

    public void testProcess_GivenRowsWithDependentVariableValue_AndTrainingPercentIsHundred() {
        CrossValidationSplitter splitter = createSplitter(100.0);

        for (int i = 0; i < classValuesPerRow.length; i++) {
            String[] row = new String[fields.size()];
            for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                String value = fieldIndex == dependentVariableIndex ? classValuesPerRow[i] : randomAlphaOfLength(10);
                row[fieldIndex] = value;
            }

            String[] processedRow = Arrays.copyOf(row, row.length);
            splitter.process(processedRow, this::incrementTrainingDocsCount, this::incrementTestDocsCount);

            // As training percent is 100 all rows should be unaffected
            assertThat(Arrays.equals(processedRow, row), is(true));
        }
        assertThat(trainingDocsCount, equalTo(500L));
        assertThat(testDocsCount, equalTo(0L));
    }

    public void testProcess_GivenRowsWithDependentVariableValue_AndTrainingPercentIsRandom() {
        // We don't go too low here to avoid flakiness
        double trainingPercent = randomDoubleBetween(50.0, 100.0, true);

        CrossValidationSplitter splitter = createSplitter(trainingPercent);

        Map<String, Integer> totalRowsPerClass = new HashMap<>();
        Map<String, Integer> trainingRowsPerClass = new HashMap<>();

        for (String classValue : classCounts.keySet()) {
            totalRowsPerClass.put(classValue, 0);
            trainingRowsPerClass.put(classValue, 0);
        }

        for (int i = 0; i < classValuesPerRow.length; i++) {
            String[] row = new String[fields.size()];
            for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                String value = fieldIndex == dependentVariableIndex ? classValuesPerRow[i] : randomAlphaOfLength(10);
                row[fieldIndex] = value;
            }

            String[] processedRow = Arrays.copyOf(row, row.length);
            splitter.process(processedRow, this::incrementTrainingDocsCount, this::incrementTestDocsCount);

            for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                if (fieldIndex != dependentVariableIndex) {
                    assertThat(processedRow[fieldIndex], equalTo(row[fieldIndex]));
                }
            }

            String classValue = row[dependentVariableIndex];
            totalRowsPerClass.compute(classValue, (k, v) -> v + 1);

            if (DataFrameDataExtractor.NULL_VALUE.equals(processedRow[dependentVariableIndex]) == false) {
                assertThat(processedRow[dependentVariableIndex], equalTo(row[dependentVariableIndex]));
                trainingRowsPerClass.compute(classValue, (k, v) -> v + 1);
            }
        }

        double trainingFraction = trainingPercent / 100;

        // We can assert we're plus/minus 1 from rounding error

        long expectedTotalTrainingCount = 0;
        for (long classCount : classCounts.values()) {
            expectedTotalTrainingCount += trainingFraction * classCount;
        }
        assertThat(trainingDocsCount + testDocsCount, equalTo((long) ROWS_COUNT));
        assertThat(trainingDocsCount, greaterThanOrEqualTo(expectedTotalTrainingCount - 2));
        assertThat(trainingDocsCount, lessThanOrEqualTo(expectedTotalTrainingCount));

        for (String classValue : classCounts.keySet()) {
            double expectedClassTrainingCount = totalRowsPerClass.get(classValue) * trainingFraction;
            int classTrainingCount = trainingRowsPerClass.get(classValue);
            assertThat((double) classTrainingCount, is(closeTo(expectedClassTrainingCount, 1.0)));
        }
    }

    public void testProcess_SelectsTrainingRowsUniformly() {
        double trainingPercent = 50.0;
        int runCount = 500;

        int[] trainingCountPerRow = new int[ROWS_COUNT];

        for (int run = 0; run < runCount; run++) {

            randomizeSeed = randomLong();
            CrossValidationSplitter crossValidationSplitter = createSplitter(trainingPercent);

            for (int i = 0; i < classValuesPerRow.length; i++) {
                String[] row = new String[fields.size()];
                for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                    String value = fieldIndex == dependentVariableIndex ? classValuesPerRow[i] : randomAlphaOfLength(10);
                    row[fieldIndex] = value;
                }

                String[] processedRow = Arrays.copyOf(row, row.length);
                crossValidationSplitter.process(processedRow, this::incrementTrainingDocsCount, this::incrementTestDocsCount);

                if (processedRow[dependentVariableIndex] != DataFrameDataExtractor.NULL_VALUE) {
                    trainingCountPerRow[i]++;
                }
            }
        }

        // We expect each data row to be selected uniformly.
        // Thus the fraction of the row count where it's selected for training against the number of runs
        // should be close to the training percent, which is set to 0.5
        for (int rowTrainingCount : trainingCountPerRow) {
            double meanCount = rowTrainingCount / (double) runCount;
            assertThat(meanCount, is(closeTo(0.5, 0.13)));
        }
    }

    public void testProcess_GivenTwoClassesWithCountEqualToOne_ShouldUseForTraining() {
        dependentVariable = "dep_var";
        fields = Arrays.asList(dependentVariable, "feature");
        classCounts = new HashMap<>();
        classCounts.put("class_a", 1L);
        classCounts.put("class_b", 1L);
        CrossValidationSplitter splitter = createSplitter(80.0);

        {
            String[] row = new String[]{"class_a", "42.0"};

            String[] processedRow = Arrays.copyOf(row, row.length);
            splitter.process(processedRow, this::incrementTrainingDocsCount, this::incrementTestDocsCount);

            assertThat(Arrays.equals(processedRow, row), is(true));
        }
        {
            String[] row = new String[]{"class_b", "42.0"};

            String[] processedRow = Arrays.copyOf(row, row.length);
            splitter.process(processedRow, this::incrementTrainingDocsCount, this::incrementTestDocsCount);

            assertThat(Arrays.equals(processedRow, row), is(true));
        }

        assertThat(trainingDocsCount, equalTo(2L));
        assertThat(testDocsCount, equalTo(0L));
    }

    private CrossValidationSplitter createSplitter(double trainingPercent) {
        return new StratifiedCrossValidationSplitter(fields, dependentVariable, classCounts, trainingPercent, randomizeSeed);
    }

    private void incrementTrainingDocsCount() {
        trainingDocsCount++;
    }

    private void incrementTestDocsCount() {
        testDocsCount++;
    }
}

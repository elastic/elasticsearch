/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process.crossvalidation;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;

public class SingleClassReservoirCrossValidationSplitterTests extends ESTestCase {

    private List<String> fields;
    private int dependentVariableIndex;
    private String dependentVariable;
    private long randomizeSeed;
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
    }

    public void testProcess_GivenRowsWithoutDependentVariableValue() {
        CrossValidationSplitter crossValidationSplitter = createSplitter(50.0, 0);

        for (int i = 0; i < 100; i++) {
            String[] row = new String[fields.size()];
            for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                String value = fieldIndex == dependentVariableIndex ? DataFrameDataExtractor.NULL_VALUE : randomAlphaOfLength(10);
                row[fieldIndex] = value;
            }

            String[] processedRow = Arrays.copyOf(row, row.length);
            crossValidationSplitter.process(processedRow, this::incrementTrainingDocsCount, this::incrementTestDocsCount);

            // As all these rows have no dependent variable value, they're not for training and should be unaffected
            assertThat(Arrays.equals(processedRow, row), is(true));
        }
        assertThat(trainingDocsCount, equalTo(0L));
        assertThat(testDocsCount, equalTo(100L));
    }

    public void testProcess_GivenRowsWithDependentVariableValue_AndTrainingPercentIsHundred() {
        CrossValidationSplitter crossValidationSplitter = createSplitter(100.0, 100L);

        for (int i = 0; i < 100; i++) {
            String[] row = new String[fields.size()];
            for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                row[fieldIndex] = randomAlphaOfLength(10);
            }

            String[] processedRow = Arrays.copyOf(row, row.length);
            crossValidationSplitter.process(processedRow, this::incrementTrainingDocsCount, this::incrementTestDocsCount);

            // We should pick them all as training percent is 100
            assertThat(Arrays.equals(processedRow, row), is(true));
        }
        assertThat(trainingDocsCount, equalTo(100L));
        assertThat(testDocsCount, equalTo(0L));
    }

    public void testProcess_GivenRowsWithDependentVariableValue_AndTrainingPercentIsRandom() {
        double trainingPercent = randomDoubleBetween(1.0, 100.0, true);
        double trainingFraction = trainingPercent / 100;
        long rowCount = 1000;

        int runCount = 20;
        int[] trainingRowsPerRun = new int[runCount];
        for (int testIndex = 0; testIndex < runCount; testIndex++) {
            CrossValidationSplitter crossValidationSplitter = createSplitter(trainingPercent, rowCount);
            int trainingRows = 0;
            for (int i = 0; i < rowCount; i++) {
                String[] row = new String[fields.size()];
                for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                    row[fieldIndex] = randomAlphaOfLength(10);
                }

                String[] processedRow = Arrays.copyOf(row, row.length);
                crossValidationSplitter.process(processedRow, this::incrementTrainingDocsCount, this::incrementTestDocsCount);

                for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                    if (fieldIndex != dependentVariableIndex) {
                        assertThat(processedRow[fieldIndex], equalTo(row[fieldIndex]));
                    }
                }
                if (DataFrameDataExtractor.NULL_VALUE.equals(processedRow[dependentVariableIndex]) == false) {
                    assertThat(processedRow[dependentVariableIndex], equalTo(row[dependentVariableIndex]));
                    trainingRows++;
                }
            }
            trainingRowsPerRun[testIndex] = trainingRows;
        }

        double meanTrainingRows = IntStream.of(trainingRowsPerRun).average().getAsDouble();
        assertThat(meanTrainingRows, closeTo(trainingFraction * rowCount, 1.0));
    }

    public void testProcess_ShouldHaveAtLeastOneTrainingRow() {
        CrossValidationSplitter crossValidationSplitter = createSplitter(1.0, 1);

        // We have some non-training rows and then a training row to check
        // we maintain the first training row and not just the first row
        for (int i = 0; i < 10; i++) {
            String[] row = new String[fields.size()];
            for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                if (i < 9 && fieldIndex == dependentVariableIndex) {
                    row[fieldIndex] = DataFrameDataExtractor.NULL_VALUE;
                } else {
                    row[fieldIndex] = randomAlphaOfLength(10);
                }
            }

            String[] processedRow = Arrays.copyOf(row, row.length);
            crossValidationSplitter.process(processedRow, this::incrementTrainingDocsCount, this::incrementTestDocsCount);

            assertThat(Arrays.equals(processedRow, row), is(true));
        }
        assertThat(trainingDocsCount, equalTo(1L));
        assertThat(testDocsCount, equalTo(9L));
    }

    private CrossValidationSplitter createSplitter(double trainingPercent, long classCount) {
        return new SingleClassReservoirCrossValidationSplitter(fields, dependentVariable, trainingPercent, randomizeSeed, classCount);
    }

    private void incrementTrainingDocsCount() {
        trainingDocsCount++;
    }

    private void incrementTestDocsCount() {
        testDocsCount++;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.traintestsplit;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;

public class SingleClassReservoirTrainTestSplitterTests extends ESTestCase {

    private List<String> fields;
    private int dependentVariableIndex;
    private String dependentVariable;
    private long randomizeSeed;

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

    public void testIsTraining_GivenRowsWithoutDependentVariableValue() {
        TrainTestSplitter splitter = createSplitter(50.0, 0);

        for (int i = 0; i < 100; i++) {
            String[] row = new String[fields.size()];
            for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                String value = fieldIndex == dependentVariableIndex ? DataFrameDataExtractor.NULL_VALUE : randomAlphaOfLength(10);
                row[fieldIndex] = value;
            }

            String[] processedRow = Arrays.copyOf(row, row.length);
            assertThat(splitter.isTraining(processedRow), is(false));
            assertThat(Arrays.equals(processedRow, row), is(true));
        }
    }

    public void testIsTraining_GivenRowsWithDependentVariableValue_AndTrainingPercentIsHundred() {
        TrainTestSplitter splitter = createSplitter(100.0, 100L);

        for (int i = 0; i < 100; i++) {
            String[] row = new String[fields.size()];
            for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                row[fieldIndex] = randomAlphaOfLength(10);
            }

            String[] processedRow = Arrays.copyOf(row, row.length);
            assertThat(splitter.isTraining(processedRow), is(true));
            assertThat(Arrays.equals(processedRow, row), is(true));
        }
    }

    public void testIsTraining_GivenRowsWithDependentVariableValue_AndTrainingPercentIsRandom() {
        double trainingPercent = randomDoubleBetween(1.0, 100.0, true);
        double trainingFraction = trainingPercent / 100;
        long rowCount = 1000;

        int runCount = 20;
        int[] trainingRowsPerRun = new int[runCount];
        for (int testIndex = 0; testIndex < runCount; testIndex++) {
            TrainTestSplitter splitter = createSplitter(trainingPercent, rowCount);
            int trainingRows = 0;
            for (int i = 0; i < rowCount; i++) {
                String[] row = new String[fields.size()];
                for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                    row[fieldIndex] = randomAlphaOfLength(10);
                }

                String[] processedRow = Arrays.copyOf(row, row.length);
                boolean isTraining = splitter.isTraining(processedRow);
                assertThat(Arrays.equals(processedRow, row), is(true));

                if (isTraining) {
                    trainingRows++;
                }
            }
            trainingRowsPerRun[testIndex] = trainingRows;
        }

        double meanTrainingRows = IntStream.of(trainingRowsPerRun).average().getAsDouble();
        assertThat(meanTrainingRows, closeTo(trainingFraction * rowCount, 1.0));
    }

    public void testIsTraining_ShouldHaveAtLeastOneTrainingRow() {
        TrainTestSplitter splitter = createSplitter(1.0, 1);

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

            boolean shouldBeForTraining = row[dependentVariableIndex] != DataFrameDataExtractor.NULL_VALUE;
            assertThat(splitter.isTraining(processedRow), is(shouldBeForTraining));
            assertThat(Arrays.equals(processedRow, row), is(true));
        }
    }

    private TrainTestSplitter createSplitter(double trainingPercent, long classCount) {
        return new SingleClassReservoirTrainTestSplitter(fields, dependentVariable, trainingPercent, randomizeSeed, classCount);
    }
}

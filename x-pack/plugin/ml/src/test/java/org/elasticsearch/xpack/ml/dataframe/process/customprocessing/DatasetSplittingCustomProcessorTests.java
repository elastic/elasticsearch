/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process.customprocessing;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class DatasetSplittingCustomProcessorTests extends ESTestCase {

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

    public void testProcess_GivenRowsWithoutDependentVariableValue() {
        CustomProcessor customProcessor = new DatasetSplittingCustomProcessor(fields, dependentVariable, 50.0, randomizeSeed);

        for (int i = 0; i < 100; i++) {
            String[] row = new String[fields.size()];
            for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                String value = fieldIndex == dependentVariableIndex ? "" : randomAlphaOfLength(10);
                row[fieldIndex] = value;
            }

            String[] processedRow = Arrays.copyOf(row, row.length);
            customProcessor.process(processedRow);

            // As all these rows have no dependent variable value, they're not for training and should be unaffected
            assertThat(Arrays.equals(processedRow, row), is(true));
        }
    }

    public void testProcess_GivenRowsWithDependentVariableValue_AndTrainingPercentIsHundred() {
        CustomProcessor customProcessor = new DatasetSplittingCustomProcessor(fields, dependentVariable, 100.0, randomizeSeed);

        for (int i = 0; i < 100; i++) {
            String[] row = new String[fields.size()];
            for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                String value = fieldIndex == dependentVariableIndex ? "" : randomAlphaOfLength(10);
                row[fieldIndex] = value;
            }

            String[] processedRow = Arrays.copyOf(row, row.length);
            customProcessor.process(processedRow);

            // We should pick them all as training percent is 100
            assertThat(Arrays.equals(processedRow, row), is(true));
        }
    }

    public void testProcess_GivenRowsWithDependentVariableValue_AndTrainingPercentIsRandom() {
        double trainingPercent = randomDoubleBetween(1.0, 100.0, true);
        double trainingFraction = trainingPercent / 100;
        CustomProcessor customProcessor = new DatasetSplittingCustomProcessor(fields, dependentVariable, trainingPercent, randomizeSeed);

        int runCount = 20;
        int rowsCount = 1000;
        int[] trainingRowsPerRun = new int[runCount];
        for (int testIndex = 0; testIndex < runCount; testIndex++) {
            int trainingRows = 0;
            for (int i = 0; i < rowsCount; i++) {
                String[] row = new String[fields.size()];
                for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                    row[fieldIndex] = randomAlphaOfLength(10);
                }

                String[] processedRow = Arrays.copyOf(row, row.length);
                customProcessor.process(processedRow);

                for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                    if (fieldIndex != dependentVariableIndex) {
                        assertThat(processedRow[fieldIndex], equalTo(row[fieldIndex]));
                    }
                }
                if (processedRow[dependentVariableIndex].length() > 0) {
                    assertThat(processedRow[dependentVariableIndex], equalTo(row[dependentVariableIndex]));
                    trainingRows++;
                }
            }
            trainingRowsPerRun[testIndex] = trainingRows;
        }

        double meanTrainingRows = IntStream.of(trainingRowsPerRun).average().getAsDouble();

        // Now we need to calculate sensible bounds to assert against.
        // We'll use 5 variances which should mean the test only fails once in 7M
        // And, because we're doing multiple runs, we'll divide the variance with the number of runs to narrow the bounds
        double expectedTrainingRows = trainingFraction * rowsCount;
        double variance = rowsCount * (Math.pow(1 - trainingFraction, 2) * trainingFraction
            + Math.pow(trainingFraction, 2) * (1 - trainingFraction));
        double lowerBound = expectedTrainingRows - 5 * Math.sqrt(variance / runCount);
        double upperBound = expectedTrainingRows + 5 * Math.sqrt(variance / runCount);

        assertThat("Mean training rows [" + meanTrainingRows + "] was not within expected bounds of [" + lowerBound + ", "
            + upperBound + "] given training fraction was [" + trainingFraction + "]",
            meanTrainingRows, is(both(greaterThan(lowerBound)).and(lessThan(upperBound))));
    }

    public void testProcess_ShouldHaveAtLeastOneTrainingRow() {
        CustomProcessor customProcessor = new DatasetSplittingCustomProcessor(fields, dependentVariable, 1.0, randomizeSeed);

        // We have some non-training rows and then a training row to check
        // we maintain the first training row and not just the first row
        for (int i = 0; i < 10; i++) {
            String[] row = new String[fields.size()];
            for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                if (i < 9 && fieldIndex == dependentVariableIndex) {
                    row[fieldIndex] = "";
                } else {
                    row[fieldIndex] = randomAlphaOfLength(10);
                }
            }

            String[] processedRow = Arrays.copyOf(row, row.length);
            customProcessor.process(processedRow);

            assertThat(Arrays.equals(processedRow, row), is(true));
        }
    }
}

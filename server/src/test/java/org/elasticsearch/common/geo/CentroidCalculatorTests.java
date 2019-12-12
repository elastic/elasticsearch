/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.geo;

import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class CentroidCalculatorTests extends ESTestCase {

    public void test() {
        double[] y = new double[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        double[] x = new double[] { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };
        double[] yRunningAvg = new double[] { 1, 1.5, 2.0, 2.5, 3, 3.5, 4, 4.5, 5, 5.5 };
        double[] xRunningAvg = new double[] { 10, 15, 20, 25, 30, 35, 40, 45, 50, 55 };

        Point point = new Point(x[0], y[0]);
        CentroidCalculator calculator = new CentroidCalculator(point);
        assertThat(calculator.getX(), equalTo(xRunningAvg[0]));
        assertThat(calculator.getY(), equalTo(yRunningAvg[0]));
        for (int i = 1; i < 10; i++) {
            double[] subX = new double[i + 1];
            double[] subY = new double[i + 1];
            System.arraycopy(x, 0, subX, 0, i + 1);
            System.arraycopy(y, 0, subY, 0, i + 1);
            Geometry geometry  = new Line(subX, subY);
            calculator = new CentroidCalculator(geometry);
            assertThat(calculator.getX(), equalTo(xRunningAvg[i]));
            assertThat(calculator.getY(), equalTo(yRunningAvg[i]));
        }
        CentroidCalculator otherCalculator = new CentroidCalculator(new Point(0, 0));
        calculator.addFrom(otherCalculator);
        assertThat(calculator.getX(), equalTo(50.0));
        assertThat(calculator.getY(), equalTo(5.0));
    }
}

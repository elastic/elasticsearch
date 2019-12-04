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

/**
 * This class keeps a running Kahan-sum of coordinates
 * that are to be averaged in {@link TriangleTreeWriter} for use
 * as the centroid of a shape.
 */
public class CentroidCalculator {

    private double compX;
    private double compY;
    private double sumX;
    private double sumY;
    private int count;

    public CentroidCalculator() {
        this.sumX = 0.0;
        this.compX = 0.0;
        this.sumY = 0.0;
        this.compY = 0.0;
        this.count = 0;
    }

    /**
     * adds a single coordinate to the running sum and count of coordinates
     * for centroid calculation
     *
     * @param x the x-coordinate of the point
     * @param y the y-coordinate of the point
     */
    public void addCoordinate(double x, double y) {
        double correctedX = x - compX;
        double newSumX = sumX + correctedX;
        compX = (newSumX - sumX) - correctedX;
        sumX = newSumX;

        double correctedY = y - compY;
        double newSumY = sumY + correctedY;
        compY = (newSumY - sumY) - correctedY;
        sumY = newSumY;

        count += 1;
    }

    /**
     * Adjusts the existing calculator to add the running sum and count
     * from another {@link CentroidCalculator}. This is used to keep
     * a running count of points from different sub-shapes of a single
     * geo-shape field
     *
     * @param otherCalculator the other centroid calculator to add from
     */
    void addFrom(CentroidCalculator otherCalculator) {
        addCoordinate(otherCalculator.sumX, otherCalculator.sumY);
        // adjust count
        count += otherCalculator.count - 1;
    }

    /**
     * @return the x-coordinate centroid
     */
    public double getX() {
        return sumX / count;
    }

    /**
     * @return the y-coordinate centroid
     */
    public double getY() {
        return sumY / count;
    }
}

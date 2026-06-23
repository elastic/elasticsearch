/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

public interface IntConstraint {
    /**
     * Returns an ordered array of non-overlapping inclusive ranges that represent the valid values for this constraint.
     *
     * @return an array of Range objects representing the valid ranges
     */
    IntConstraints.Range[] trueRanges();

    /**
     * Tests if the given integer value satisfies this constraint.
     * @param value the integer value to test
     * @return true if the value satisfies the constraint, false otherwise
     */
    boolean isApplicable(int value);

    default IntConstraint and(IntConstraint constraint) {
        if (constraint == null) {
            throw new IllegalArgumentException("Constraint cannot be null");
        }

        return new IntConstraint() {
            @Override
            public IntConstraints.Range[] trueRanges() {
                IntConstraints.Range[] thisRanges = IntConstraint.this.trueRanges();
                IntConstraints.Range[] otherRanges = constraint.trueRanges();
                ArrayList<IntConstraints.Range> combinedRanges = new ArrayList<>();
                for (IntConstraints.Range r1 : thisRanges) {
                    for (IntConstraints.Range r2 : otherRanges) {
                        if (r1.upperBound() >= r2.lowerBound() && r1.lowerBound() <= r2.upperBound()) {
                            combinedRanges.add(
                                new IntConstraints.Range(
                                    Math.max(r1.lowerBound(), r2.lowerBound()),
                                    Math.min(r1.upperBound(), r2.upperBound())
                                )
                            );
                        }
                    }
                }
                return combinedRanges.toArray(new IntConstraints.Range[0]);
            }

            @Override
            public boolean isApplicable(int value) {
                return IntConstraint.this.isApplicable(value) && constraint.isApplicable(value);
            }
        };
    }

    default IntConstraint or(IntConstraint constraint) {
        if (constraint == null) {
            throw new IllegalArgumentException("Constraint cannot be null");
        }

        return new IntConstraint() {
            @Override
            public IntConstraints.Range[] trueRanges() {
                IntConstraints.Range[] thisRanges = IntConstraint.this.trueRanges();
                IntConstraints.Range[] otherRanges = constraint.trueRanges();
                ArrayList<IntConstraints.Range> combinedRanges = new ArrayList<>();
                combinedRanges.addAll(Arrays.asList(thisRanges));
                combinedRanges.addAll(Arrays.asList(otherRanges));
                combinedRanges.sort(Comparator.comparingInt(IntConstraints.Range::lowerBound));

                // merge overlapping ranges
                ArrayList<IntConstraints.Range> mergedRanges = new ArrayList<>();
                IntConstraints.Range current = null;
                for (IntConstraints.Range range : combinedRanges) {
                    if (current == null) {
                        current = range;
                    } else if (current.upperBound() >= range.lowerBound()) {
                        // Overlapping ranges, merge them
                        current = new IntConstraints.Range(
                            Math.min(current.lowerBound(), range.lowerBound()),
                            Math.max(current.upperBound(), range.upperBound())
                        );
                    } else {
                        // no overlap, add the current range and make the new range current
                        mergedRanges.add(current);
                        current = range;
                    }
                }
                if (current != null) {
                    mergedRanges.add(current);
                }
                return mergedRanges.toArray(new IntConstraints.Range[0]);
            }

            @Override
            public boolean isApplicable(int value) {
                return IntConstraint.this.isApplicable(value) || constraint.isApplicable(value);
            }
        };
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

import org.elasticsearch.test.ESTestCase;

public class IntConstraintsTests extends ESTestCase {

    public void testEqualsConstraint() {
        IntConstraint predicate = IntConstraints.parseIntConstraint("==5");
        assertTrue(predicate.isApplicable(5));
        assertFalse(predicate.isApplicable(4));
        assertFalse(predicate.isApplicable(6));
        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(5, 5) }, predicate.trueRanges());
    }

    public void testLessThanConstraint() {
        IntConstraint predicate = IntConstraints.parseIntConstraint("<10");
        assertTrue(predicate.isApplicable(9));
        assertTrue(predicate.isApplicable(-100));
        assertFalse(predicate.isApplicable(10));
        assertFalse(predicate.isApplicable(11));
        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(Integer.MIN_VALUE, 9) }, predicate.trueRanges());
    }

    public void testGreaterThanConstraint() {
        IntConstraint predicate = IntConstraints.parseIntConstraint(">20");
        assertTrue(predicate.isApplicable(21));
        assertTrue(predicate.isApplicable(100));
        assertFalse(predicate.isApplicable(20));
        assertFalse(predicate.isApplicable(19));
        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(21, Integer.MAX_VALUE) }, predicate.trueRanges());
    }

    public void testLessThanOrEqualConstraint() {
        IntConstraint predicate = IntConstraints.parseIntConstraint("<=15");
        assertTrue(predicate.isApplicable(15));
        assertTrue(predicate.isApplicable(14));
        assertTrue(predicate.isApplicable(-50));
        assertFalse(predicate.isApplicable(16));
        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(Integer.MIN_VALUE, 15) }, predicate.trueRanges());
    }

    public void testGreaterThanOrEqualConstraint() {
        IntConstraint predicate = IntConstraints.parseIntConstraint(">=25");
        assertTrue(predicate.isApplicable(25));
        assertTrue(predicate.isApplicable(26));
        assertTrue(predicate.isApplicable(100));
        assertFalse(predicate.isApplicable(24));
        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(25, Integer.MAX_VALUE) }, predicate.trueRanges());
    }

    public void testNotEqualConstraint() {
        IntConstraint predicate = IntConstraints.parseIntConstraint("!=30");
        assertTrue(predicate.isApplicable(29));
        assertTrue(predicate.isApplicable(31));
        assertFalse(predicate.isApplicable(30));
        assertArrayEquals(
            new IntConstraints.Range[] { new IntConstraints.Range(Integer.MIN_VALUE, 29), new IntConstraints.Range(31, Integer.MAX_VALUE) },
            predicate.trueRanges()
        );
    }

    public void testRangeConstraint() {
        IntConstraint predicate = IntConstraints.parseIntConstraint("1-10");
        assertTrue(predicate.isApplicable(1));
        assertTrue(predicate.isApplicable(5));
        assertTrue(predicate.isApplicable(10));
        assertFalse(predicate.isApplicable(0));
        assertFalse(predicate.isApplicable(11));
        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(1, 10) }, predicate.trueRanges());
    }

    public void testSetConstraint() {
        IntConstraint predicate = IntConstraints.parseIntConstraint("1|3|5|7");
        assertTrue(predicate.isApplicable(1));
        assertTrue(predicate.isApplicable(3));
        assertTrue(predicate.isApplicable(5));
        assertTrue(predicate.isApplicable(7));
        assertFalse(predicate.isApplicable(2));
        assertFalse(predicate.isApplicable(4));
        assertFalse(predicate.isApplicable(8));
        assertArrayEquals(
            new IntConstraints.Range[] {
                new IntConstraints.Range(1, 1),
                new IntConstraints.Range(3, 3),
                new IntConstraints.Range(5, 5),
                new IntConstraints.Range(7, 7) },
            predicate.trueRanges()
        );
    }

    public void testLengthConstraint() {
        IntConstraint predicate = IntConstraints.parseIntConstraint("{3}");
        assertTrue(predicate.isApplicable(123));
        assertTrue(predicate.isApplicable(100));
        assertTrue(predicate.isApplicable(999));
        assertFalse(predicate.isApplicable(1000));
        assertFalse(predicate.isApplicable(99));
        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(100, 999) }, predicate.trueRanges());
    }

    public void testAndConstraint() {
        IntConstraint predicate = IntConstraints.parseIntConstraint(">5 && <10");
        assertTrue(predicate.isApplicable(6));
        assertTrue(predicate.isApplicable(9));
        assertFalse(predicate.isApplicable(5));
        assertFalse(predicate.isApplicable(10));
        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(6, 9) }, predicate.trueRanges());
    }

    public void testOrConstraint() {
        IntConstraint predicate = IntConstraints.parseIntConstraint("<5 || >10");
        assertTrue(predicate.isApplicable(4));
        assertTrue(predicate.isApplicable(11));
        assertTrue(predicate.isApplicable(0));
        assertTrue(predicate.isApplicable(20));
        assertFalse(predicate.isApplicable(5));
        assertFalse(predicate.isApplicable(10));
        assertArrayEquals(
            new IntConstraints.Range[] { new IntConstraints.Range(Integer.MIN_VALUE, 4), new IntConstraints.Range(11, Integer.MAX_VALUE) },
            predicate.trueRanges()
        );
    }

    public void testComplexNestedConstraint() {
        IntConstraint predicate = IntConstraints.parseIntConstraint("==5 || >=10 && <=20");
        assertTrue(predicate.isApplicable(5));
        assertTrue(predicate.isApplicable(10));
        assertTrue(predicate.isApplicable(15));
        assertTrue(predicate.isApplicable(20));
        assertFalse(predicate.isApplicable(4));
        assertFalse(predicate.isApplicable(6));
        assertFalse(predicate.isApplicable(9));
        assertFalse(predicate.isApplicable(21));
        assertArrayEquals(
            new IntConstraints.Range[] { new IntConstraints.Range(5, 5), new IntConstraints.Range(10, 20) },
            predicate.trueRanges()
        );
    }

    public void testIntegerBoundaries() {
        // Test at Integer.MAX_VALUE and Integer.MIN_VALUE
        IntConstraint maxValuePredicate = IntConstraints.parseIntConstraint("==" + Integer.MAX_VALUE);
        assertTrue(maxValuePredicate.isApplicable(Integer.MAX_VALUE));
        assertFalse(maxValuePredicate.isApplicable(Integer.MAX_VALUE - 1));
        assertArrayEquals(
            new IntConstraints.Range[] { new IntConstraints.Range(Integer.MAX_VALUE, Integer.MAX_VALUE) },
            maxValuePredicate.trueRanges()
        );

        IntConstraint minValuePredicate = IntConstraints.parseIntConstraint("==" + Integer.MIN_VALUE);
        assertTrue(minValuePredicate.isApplicable(Integer.MIN_VALUE));
        assertFalse(minValuePredicate.isApplicable(Integer.MIN_VALUE + 1));
        assertArrayEquals(
            new IntConstraints.Range[] { new IntConstraints.Range(Integer.MIN_VALUE, Integer.MIN_VALUE) },
            minValuePredicate.trueRanges()
        );
    }

    public void testNegativeNumbers() {
        IntConstraint predicate = IntConstraints.parseIntConstraint("(-10)-(-5)");
        assertTrue(predicate.isApplicable(-10));
        assertTrue(predicate.isApplicable(-7));
        assertTrue(predicate.isApplicable(-5));
        assertFalse(predicate.isApplicable(-11));
        assertFalse(predicate.isApplicable(-4));
        assertFalse(predicate.isApplicable(0));
        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(-10, -5) }, predicate.trueRanges());
    }

    public void testZeroHandling() {
        IntConstraint equalsZero = IntConstraints.parseIntConstraint("==0");
        assertTrue(equalsZero.isApplicable(0));
        assertFalse(equalsZero.isApplicable(1));
        assertFalse(equalsZero.isApplicable(-1));
        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(0, 0) }, equalsZero.trueRanges());

        IntConstraint rangeIncludingZero = IntConstraints.parseIntConstraint("(-5)-5");
        assertTrue(rangeIncludingZero.isApplicable(0));
        assertTrue(rangeIncludingZero.isApplicable(-5));
        assertTrue(rangeIncludingZero.isApplicable(5));
        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(-5, 5) }, rangeIncludingZero.trueRanges());
    }

    public void testNullOrEmptyConstraint() {
        assertEquals(AnyInteger.INSTANCE, IntConstraints.parseIntConstraint(null));
        assertEquals(AnyInteger.INSTANCE, IntConstraints.parseIntConstraint(""));
        assertEquals(AnyInteger.INSTANCE, IntConstraints.parseIntConstraint("   "));
    }

    public void testInvalidConstraintFormat() {
        // Invalid operator
        assertThrows(IllegalArgumentException.class, () -> IntConstraints.parseIntConstraint("=>5"));

        // Missing operand
        assertThrows(IllegalArgumentException.class, () -> IntConstraints.parseIntConstraint("=="));

        // Invalid number format
        assertThrows(IllegalArgumentException.class, () -> IntConstraints.parseIntConstraint("==abc"));

        // Incorrectly formatted range
        assertThrows(IllegalArgumentException.class, () -> IntConstraints.parseIntConstraint("5-"));
        assertThrows(IllegalArgumentException.class, () -> IntConstraints.parseIntConstraint("-5"));

        // Invalid range (upper bound < lower bound)
        assertThrows(IllegalArgumentException.class, () -> IntConstraints.parseIntConstraint("10-5"));
    }

    public void testWhitespaceHandling() {
        // Test different whitespace formats
        IntConstraint standard = IntConstraints.parseIntConstraint("5-10");
        IntConstraint withSpaces = IntConstraints.parseIntConstraint(" 5 - 10 ");
        IntConstraint manySpaces = IntConstraints.parseIntConstraint("   5    -    10   ");

        // All should behave the same
        assertTrue(standard.isApplicable(7));
        assertTrue(withSpaces.isApplicable(7));
        assertTrue(manySpaces.isApplicable(7));

        assertFalse(standard.isApplicable(11));
        assertFalse(withSpaces.isApplicable(11));
        assertFalse(manySpaces.isApplicable(11));

        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(5, 10) }, standard.trueRanges());
        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(5, 10) }, withSpaces.trueRanges());
        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(5, 10) }, manySpaces.trueRanges());
    }

    public void testAndWithOverlappingRanges() {
        IntConstraint range1 = IntConstraints.parseIntConstraint("1-10");
        IntConstraint range2 = IntConstraints.parseIntConstraint("5-15");
        IntConstraint combined = range1.and(range2);

        // Overlapping range is 5-10
        assertTrue(combined.isApplicable(5));
        assertTrue(combined.isApplicable(10));
        assertFalse(combined.isApplicable(4));
        assertFalse(combined.isApplicable(11));

        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(5, 10) }, combined.trueRanges());
    }

    public void testAndWithNonOverlappingRanges() {
        IntConstraint range1 = IntConstraints.parseIntConstraint("1-5");
        IntConstraint range2 = IntConstraints.parseIntConstraint("10-15");
        IntConstraint combined = range1.and(range2);

        // No overlap, should accept no values
        assertFalse(combined.isApplicable(3));
        assertFalse(combined.isApplicable(12));

        assertArrayEquals(new IntConstraints.Range[] {}, combined.trueRanges());
    }

    public void testOrWithOverlappingRanges() {
        IntConstraint range1 = IntConstraints.parseIntConstraint("1-10");
        IntConstraint range2 = IntConstraints.parseIntConstraint("5-15");
        IntConstraint combined = range1.or(range2);

        // Combined range is 1-15
        assertTrue(combined.isApplicable(1));
        assertTrue(combined.isApplicable(10));
        assertTrue(combined.isApplicable(15));
        assertFalse(combined.isApplicable(0));
        assertFalse(combined.isApplicable(16));

        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(1, 15) }, combined.trueRanges());
    }

    public void testOrWithNonOverlappingRanges() {
        IntConstraint range1 = IntConstraints.parseIntConstraint("1-5");
        IntConstraint range2 = IntConstraints.parseIntConstraint("10-15");
        IntConstraint combined = range1.or(range2);

        // Combined range is 1-5 and 10-15
        assertTrue(combined.isApplicable(3));
        assertTrue(combined.isApplicable(12));
        assertFalse(combined.isApplicable(6));
        assertFalse(combined.isApplicable(9));

        assertArrayEquals(
            new IntConstraints.Range[] { new IntConstraints.Range(1, 5), new IntConstraints.Range(10, 15) },
            combined.trueRanges()
        );
    }

    public void testComplexAndOrCombination() {
        IntConstraint range1 = IntConstraints.parseIntConstraint("1-10");
        IntConstraint range2 = IntConstraints.parseIntConstraint("5-15");
        IntConstraint range3 = IntConstraints.parseIntConstraint("20-25");
        IntConstraint range4 = IntConstraints.parseIntConstraint("8-22");

        // (1-10 AND 5-15) OR (20-25 AND 8-22)
        IntConstraint combined = range1.and(range2).or(range3.and(range4));

        // Valid ranges: 5-10 (from AND of 1-10 and 5-15) and 20-22 (from AND of 20-25 and 8-22)
        assertTrue(combined.isApplicable(5));
        assertTrue(combined.isApplicable(10));
        assertTrue(combined.isApplicable(20));
        assertTrue(combined.isApplicable(22));
        assertFalse(combined.isApplicable(4));
        assertFalse(combined.isApplicable(11));
        assertFalse(combined.isApplicable(19));
        assertFalse(combined.isApplicable(23));

        assertArrayEquals(
            new IntConstraints.Range[] { new IntConstraints.Range(5, 10), new IntConstraints.Range(20, 22) },
            combined.trueRanges()
        );
    }

    public void testMultipleChainedAnd() {
        IntConstraint range1 = IntConstraints.parseIntConstraint("10-30");
        IntConstraint range2 = IntConstraints.parseIntConstraint("1-20");
        IntConstraint range3 = IntConstraints.parseIntConstraint("15-25");

        // 1-20 AND 10-30 AND 15-25
        IntConstraint combined = range1.and(range2).and(range3);

        // Valid range: 15-20
        assertTrue(combined.isApplicable(15));
        assertTrue(combined.isApplicable(20));
        assertFalse(combined.isApplicable(14));
        assertFalse(combined.isApplicable(21));

        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(15, 20) }, combined.trueRanges());
    }

    public void testMultipleChainedOr() {
        IntConstraint range1 = IntConstraints.parseIntConstraint("20-25");
        IntConstraint range2 = IntConstraints.parseIntConstraint("1-5");
        IntConstraint range3 = IntConstraints.parseIntConstraint("10-15");

        // 1-5 OR 10-15 OR 20-25
        IntConstraint combined = range1.or(range2).or(range3);

        // Valid ranges: 1-5, 10-15, 20-25
        assertTrue(combined.isApplicable(3));
        assertTrue(combined.isApplicable(12));
        assertTrue(combined.isApplicable(22));
        assertFalse(combined.isApplicable(6));
        assertFalse(combined.isApplicable(9));
        assertFalse(combined.isApplicable(26));

        assertArrayEquals(
            new IntConstraints.Range[] {
                new IntConstraints.Range(1, 5),
                new IntConstraints.Range(10, 15),
                new IntConstraints.Range(20, 25) },
            combined.trueRanges()
        );
    }

    public void testComplexNestedCombination_1() {
        IntConstraint range1 = IntConstraints.parseIntConstraint("1-10");
        IntConstraint range2 = IntConstraints.parseIntConstraint("5-15");
        IntConstraint range3 = IntConstraints.parseIntConstraint("20-30");
        IntConstraint range4 = IntConstraints.parseIntConstraint("25-35");

        // ((1-10 AND 5-15) OR 20-30) AND 25-35
        IntConstraint combined = range1.and(range2).or(range3).and(range4);

        // Valid range: 25-30 (from OR of 1-10 AND 5-15 and 20-30, intersected with 25-35)
        assertTrue(combined.isApplicable(25));
        assertTrue(combined.isApplicable(30));
        assertFalse(combined.isApplicable(24));
        assertFalse(combined.isApplicable(31));

        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(25, 30) }, combined.trueRanges());
    }

    public void testComplexNestedCombination_2() {
        IntConstraint range1 = IntConstraints.parseIntConstraint("5-10");
        IntConstraint range2 = IntConstraints.parseIntConstraint("8-12");
        IntConstraint range3 = IntConstraints.parseIntConstraint("18-30");
        IntConstraint range4 = IntConstraints.parseIntConstraint("1-20");

        // (5-10 OR 8-12 OR 18-30) AND (1-20)
        IntConstraint combined = range1.or(range2).or(range3).and(range4);

        // Valid ranges: 5-12 and 18-20
        assertTrue(combined.isApplicable(5));
        assertTrue(combined.isApplicable(8));
        assertTrue(combined.isApplicable(9));
        assertTrue(combined.isApplicable(10));
        assertTrue(combined.isApplicable(12));
        assertTrue(combined.isApplicable(18));
        assertTrue(combined.isApplicable(20));
        assertFalse(combined.isApplicable(4));
        assertFalse(combined.isApplicable(13));
        assertFalse(combined.isApplicable(17));
        assertFalse(combined.isApplicable(21));

        assertArrayEquals(
            new IntConstraints.Range[] { new IntConstraints.Range(5, 12), new IntConstraints.Range(18, 20) },
            combined.trueRanges()
        );
    }

    public void testComplexNestedCombination_3() {
        IntConstraint range1 = IntConstraints.parseIntConstraint("(-50)-(-10)");
        IntConstraint range2 = IntConstraints.parseIntConstraint("(-30)-(-10)");
        IntConstraint range3 = IntConstraints.parseIntConstraint("(-10)-100");
        IntConstraint equals1 = IntConstraints.parseIntConstraint("==(-10)");

        // (-50)-(-10) AND (-30)-(-10) AND ==(-10) AND (-10)-100
        IntConstraint combined = range1.and(range2).and(equals1).and(range3);

        // only -10 is valid
        assertTrue(combined.isApplicable(-10));
        assertFalse(combined.isApplicable(-50));
        assertFalse(combined.isApplicable(-30));
        assertFalse(combined.isApplicable(-11));
        assertFalse(combined.isApplicable(-9));

        assertArrayEquals(new IntConstraints.Range[] { new IntConstraints.Range(-10, -10) }, combined.trueRanges());

        IntConstraint notEquals1 = IntConstraints.parseIntConstraint("!=10");
        combined = combined.or(notEquals1);

        // everything except 10 is valid
        assertFalse(combined.isApplicable(10));
        assertTrue(combined.isApplicable(9));
        assertTrue(combined.isApplicable(11));
        assertTrue(combined.isApplicable(-10));
        assertTrue(combined.isApplicable(-50));
        assertTrue(combined.isApplicable(Integer.MIN_VALUE));
        assertTrue(combined.isApplicable(Integer.MAX_VALUE));

        assertArrayEquals(
            new IntConstraints.Range[] { new IntConstraints.Range(Integer.MIN_VALUE, 9), new IntConstraints.Range(11, Integer.MAX_VALUE) },
            combined.trueRanges()
        );
    }
}

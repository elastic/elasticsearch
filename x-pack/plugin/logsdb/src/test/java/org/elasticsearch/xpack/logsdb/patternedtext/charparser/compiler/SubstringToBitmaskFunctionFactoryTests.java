/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.compiler;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubstringView;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.AndStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.AnyString;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.EqualsStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.LengthStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.NotEqualsStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.OrStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.StringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.StringSetConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.StringToIntMapConstraint;

import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;

public class SubstringToBitmaskFunctionFactoryTests extends ESTestCase {

    // Simple constraint tests

    public void testEqualsConstraint() {
        StringConstraint constraint = new EqualsStringConstraint("test");
        ToIntFunction<SubstringView> function = SubstringToBitmaskFunctionFactory.from(0x01, constraint);

        assertEquals(0x01, function.applyAsInt(new SubstringView("test")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("other")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("Test")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("")));
    }

    public void testNotEqualsConstraint() {
        StringConstraint constraint = new NotEqualsStringConstraint("test");
        ToIntFunction<SubstringView> function = SubstringToBitmaskFunctionFactory.from(0x02, constraint);

        assertEquals(0x00, function.applyAsInt(new SubstringView("test")));
        assertEquals(0x02, function.applyAsInt(new SubstringView("other")));
        assertEquals(0x02, function.applyAsInt(new SubstringView("Test")));
        assertEquals(0x02, function.applyAsInt(new SubstringView("")));
    }

    public void testLengthConstraint() {
        StringConstraint constraint = new LengthStringConstraint(3);
        ToIntFunction<SubstringView> function = SubstringToBitmaskFunctionFactory.from(0x04, constraint);

        assertEquals(0x04, function.applyAsInt(new SubstringView("abc")));
        assertEquals(0x04, function.applyAsInt(new SubstringView("123")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("ab")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("abcd")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("")));
    }

    public void testAnyStringConstraint() {
        StringConstraint constraint = new AnyString();
        ToIntFunction<SubstringView> function = SubstringToBitmaskFunctionFactory.from(0x08, constraint);

        assertEquals(0x08, function.applyAsInt(new SubstringView("anything")));
        assertEquals(0x08, function.applyAsInt(new SubstringView("")));
        assertEquals(0x08, function.applyAsInt(new SubstringView("123")));
        assertEquals(0x08, function.applyAsInt(new SubstringView("!@#$%")));
    }

    public void testStringSetConstraint() {
        StringConstraint constraint = new StringSetConstraint(Set.of("One", "Two", "Three"));
        ToIntFunction<SubstringView> function = SubstringToBitmaskFunctionFactory.from(0x10, constraint);

        assertEquals(0x10, function.applyAsInt(new SubstringView("One")));
        assertEquals(0x10, function.applyAsInt(new SubstringView("Two")));
        assertEquals(0x10, function.applyAsInt(new SubstringView("Three")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("Four")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("one")));
    }

    public void testStringToIntMapConstraint() {
        StringConstraint constraint = new StringToIntMapConstraint(Map.of("Jan", 1, "Feb", 2, "Mar", 3));
        ToIntFunction<SubstringView> function = SubstringToBitmaskFunctionFactory.from(0x20, constraint);

        assertEquals(0x20, function.applyAsInt(new SubstringView("Jan")));
        assertEquals(0x20, function.applyAsInt(new SubstringView("Feb")));
        assertEquals(0x20, function.applyAsInt(new SubstringView("Mar")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("Apr")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("jan")));
    }

    // Composite constraint tests - OR

    public void testOrConstraint_Simple() {
        StringConstraint constraint1 = new EqualsStringConstraint("test");
        StringConstraint constraint2 = new EqualsStringConstraint("Test");
        StringConstraint orConstraint = new OrStringConstraint(constraint1, constraint2);
        ToIntFunction<SubstringView> function = SubstringToBitmaskFunctionFactory.from(0x40, orConstraint);

        assertEquals(0x40, function.applyAsInt(new SubstringView("test")));
        assertEquals(0x40, function.applyAsInt(new SubstringView("Test")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("TEST")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("other")));
    }

    public void testOrConstraint_WithLength() {
        StringConstraint constraint1 = new EqualsStringConstraint("a");
        StringConstraint constraint2 = new LengthStringConstraint(3);
        StringConstraint orConstraint = new OrStringConstraint(constraint1, constraint2);
        ToIntFunction<SubstringView> function = SubstringToBitmaskFunctionFactory.from(0x80, orConstraint);

        assertEquals(0x80, function.applyAsInt(new SubstringView("a")));
        assertEquals(0x80, function.applyAsInt(new SubstringView("abc")));
        assertEquals(0x80, function.applyAsInt(new SubstringView("xyz")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("ab")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("abcd")));
    }

    public void testOrConstraint_MultipleOrs() {
        // (equals "a") OR (equals "b") OR (equals "c")
        StringConstraint constraint1 = new EqualsStringConstraint("a");
        StringConstraint constraint2 = new EqualsStringConstraint("b");
        StringConstraint constraint3 = new EqualsStringConstraint("c");
        StringConstraint orConstraint = new OrStringConstraint(new OrStringConstraint(constraint1, constraint2), constraint3);
        ToIntFunction<SubstringView> function = SubstringToBitmaskFunctionFactory.from(0x100, orConstraint);

        assertEquals(0x100, function.applyAsInt(new SubstringView("a")));
        assertEquals(0x100, function.applyAsInt(new SubstringView("b")));
        assertEquals(0x100, function.applyAsInt(new SubstringView("c")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("d")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("ab")));
    }

    // Composite constraint tests - AND

    public void testAndConstraint_Simple() {
        StringConstraint constraint1 = new NotEqualsStringConstraint("test");
        StringConstraint constraint2 = new LengthStringConstraint(4);
        StringConstraint andConstraint = new AndStringConstraint(constraint1, constraint2);
        ToIntFunction<SubstringView> function = SubstringToBitmaskFunctionFactory.from(0x200, andConstraint);

        assertEquals(0x00, function.applyAsInt(new SubstringView("test"))); // fails constraint1
        assertEquals(0x200, function.applyAsInt(new SubstringView("pass")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("abc"))); // fails constraint2
        assertEquals(0x00, function.applyAsInt(new SubstringView("abcde"))); // fails constraint2
    }

    public void testAndConstraint_MultipleAnds() {
        // (length 4) AND (not equals "test") AND (not equals "pass")
        StringConstraint constraint1 = new LengthStringConstraint(4);
        StringConstraint constraint2 = new NotEqualsStringConstraint("test");
        StringConstraint constraint3 = new NotEqualsStringConstraint("pass");
        StringConstraint andConstraint = new AndStringConstraint(new AndStringConstraint(constraint1, constraint2), constraint3);
        ToIntFunction<SubstringView> function = SubstringToBitmaskFunctionFactory.from(0x400, andConstraint);

        assertEquals(0x400, function.applyAsInt(new SubstringView("okay")));
        assertEquals(0x400, function.applyAsInt(new SubstringView("good")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("test")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("pass")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("abc")));
    }

    // Complex composite constraints - mixing AND and OR

    public void testAndOrConstraint_Simple() {
        // (equals "a" OR equals "b") AND (length 1)
        StringConstraint equals_a = new EqualsStringConstraint("a");
        StringConstraint equals_b = new EqualsStringConstraint("b");
        StringConstraint length_1 = new LengthStringConstraint(1);
        StringConstraint orConstraint = new OrStringConstraint(equals_a, equals_b);
        StringConstraint andConstraint = new AndStringConstraint(orConstraint, length_1);
        ToIntFunction<SubstringView> function = SubstringToBitmaskFunctionFactory.from(0x800, andConstraint);

        assertEquals(0x800, function.applyAsInt(new SubstringView("a")));
        assertEquals(0x800, function.applyAsInt(new SubstringView("b")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("c")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("ab")));
    }

    public void testAndOrConstraint_Complex() {
        // ((equals "Jan" OR equals "Feb") AND (length 3)) OR (equals "March")
        StringConstraint equals_jan = new EqualsStringConstraint("Jan");
        StringConstraint equals_feb = new EqualsStringConstraint("Feb");
        StringConstraint equals_march = new EqualsStringConstraint("March");
        StringConstraint length_3 = new LengthStringConstraint(3);

        StringConstraint orJanFeb = new OrStringConstraint(equals_jan, equals_feb);
        StringConstraint andWithLength = new AndStringConstraint(orJanFeb, length_3);
        StringConstraint finalOr = new OrStringConstraint(andWithLength, equals_march);

        ToIntFunction<SubstringView> function = SubstringToBitmaskFunctionFactory.from(0x1000, finalOr);

        assertEquals(0x1000, function.applyAsInt(new SubstringView("Jan")));
        assertEquals(0x1000, function.applyAsInt(new SubstringView("Feb")));
        assertEquals(0x1000, function.applyAsInt(new SubstringView("March")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("Apr")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("January")));
    }

    public void testDeeplyNestedConstraints() {
        // (((equals "a") OR (equals "b")) AND ((not equals "x") OR (not equals "y"))) AND (length 1)
        StringConstraint equals_a = new EqualsStringConstraint("a");
        StringConstraint equals_b = new EqualsStringConstraint("b");
        StringConstraint not_x = new NotEqualsStringConstraint("x");
        StringConstraint not_y = new NotEqualsStringConstraint("y");
        StringConstraint length_1 = new LengthStringConstraint(1);

        StringConstraint orAB = new OrStringConstraint(equals_a, equals_b);
        StringConstraint orNotXY = new OrStringConstraint(not_x, not_y);
        StringConstraint andFirst = new AndStringConstraint(orAB, orNotXY);
        StringConstraint finalAnd = new AndStringConstraint(andFirst, length_1);

        ToIntFunction<SubstringView> function = SubstringToBitmaskFunctionFactory.from(0x2000, finalAnd);

        assertEquals(0x2000, function.applyAsInt(new SubstringView("a")));
        assertEquals(0x2000, function.applyAsInt(new SubstringView("b")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("c")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("x")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("y")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("ab")));
    }

    // Edge cases

    public void testEmptyString() {
        StringConstraint lengthZero = new LengthStringConstraint(0);
        ToIntFunction<SubstringView> function = SubstringToBitmaskFunctionFactory.from(0x4000, lengthZero);

        assertEquals(0x4000, function.applyAsInt(new SubstringView("")));
        assertEquals(0x00, function.applyAsInt(new SubstringView("a")));
    }

    public void testMultipleBitmasks() {
        StringConstraint constraint = new EqualsStringConstraint("test");

        // Test with different bitmask values
        ToIntFunction<SubstringView> function1 = SubstringToBitmaskFunctionFactory.from(0x01, constraint);
        ToIntFunction<SubstringView> function2 = SubstringToBitmaskFunctionFactory.from(0xFF, constraint);
        ToIntFunction<SubstringView> function3 = SubstringToBitmaskFunctionFactory.from(0x80000000, constraint);

        SubstringView test = new SubstringView("test");
        SubstringView other = new SubstringView("other");

        assertEquals(0x01, function1.applyAsInt(test));
        assertEquals(0xFF, function2.applyAsInt(test));
        assertEquals(0x80000000, function3.applyAsInt(test));

        assertEquals(0x00, function1.applyAsInt(other));
        assertEquals(0x00, function2.applyAsInt(other));
        assertEquals(0x00, function3.applyAsInt(other));
    }
}

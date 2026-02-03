/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.compiler;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubstringToIntegerMap;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.SubstringView;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.AndStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.AnyString;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.EqualsStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.LengthStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.NotEqualsStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.OrStringConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.StringSetConstraint;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints.StringToIntMapConstraint;

import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;

public class SubstringToBitmaskChainTests extends ESTestCase {

    // Single constraint tests

    public void testChain_SingleMapConstraint() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();
        builder.add(new StringToIntMapConstraint(Map.of("Jan", 1, "Feb", 2)), 0x01);

        ToIntFunction<SubstringView> chain = builder.build();

        // Should optimize to just a map
        assertTrue(chain instanceof SubstringToIntegerMap);

        assertEquals(0x01, chain.applyAsInt(new SubstringView("Jan")));
        assertEquals(0x01, chain.applyAsInt(new SubstringView("Feb")));
        assertEquals(0x00, chain.applyAsInt(new SubstringView("Mar")));
    }

    public void testChain_SingleSetConstraint() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();
        builder.add(new StringSetConstraint(Set.of("One", "Two")), 0x02);

        ToIntFunction<SubstringView> chain = builder.build();

        // Should optimize to just a map
        assertTrue(chain instanceof SubstringToIntegerMap);

        assertEquals(0x02, chain.applyAsInt(new SubstringView("One")));
        assertEquals(0x02, chain.applyAsInt(new SubstringView("Two")));
        assertEquals(0x00, chain.applyAsInt(new SubstringView("Three")));
    }

    public void testChain_SingleEqualsConstraint() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();
        builder.add(new EqualsStringConstraint("test"), 0x04);

        ToIntFunction<SubstringView> chain = builder.build();

        // Should optimize to just a map - equals can be represented as a map with a single entry
        assertTrue(chain instanceof SubstringToIntegerMap);

        assertEquals(0x04, chain.applyAsInt(new SubstringView("test")));
        assertEquals(0x00, chain.applyAsInt(new SubstringView("other")));
    }

    public void testChain_SingleNonMapConstraint() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();
        builder.add(new LengthStringConstraint(3), 0x08);

        ToIntFunction<SubstringView> chain = builder.build();

        // Should not be a map or chain, just the function itself
        assertFalse(chain instanceof SubstringToIntegerMap);
        assertFalse(chain instanceof SubstringToBitmaskChain);

        assertEquals(0x08, chain.applyAsInt(new SubstringView("abc")));
        assertEquals(0x00, chain.applyAsInt(new SubstringView("ab")));
    }

    public void testChain_SingleAnyStringConstraint() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();
        builder.add(new AnyString(), 0x10);

        ToIntFunction<SubstringView> chain = builder.build();

        // Should optimize to a simple function that always returns the bitmask
        assertFalse(chain instanceof SubstringToIntegerMap);
        assertFalse(chain instanceof SubstringToBitmaskChain);

        assertEquals(0x10, chain.applyAsInt(new SubstringView("anything")));
        assertEquals(0x10, chain.applyAsInt(new SubstringView("")));
        assertEquals(0x10, chain.applyAsInt(new SubstringView("test123")));
    }

    // Multiple constraints - all map-based

    public void testChain_MultipleMapConstraints() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();
        builder.add(new StringToIntMapConstraint(Map.of("Jan", 1)), 0x01);
        builder.add(new StringSetConstraint(Set.of("Mon", "Tue")), 0x02);
        builder.add(new EqualsStringConstraint("test"), 0x04);

        ToIntFunction<SubstringView> chain = builder.build();

        // Should optimize to just a map
        assertTrue(chain instanceof SubstringToIntegerMap);

        assertEquals(0x01, chain.applyAsInt(new SubstringView("Jan")));
        assertEquals(0x02, chain.applyAsInt(new SubstringView("Mon")));
        assertEquals(0x04, chain.applyAsInt(new SubstringView("test")));
        assertEquals(0x00, chain.applyAsInt(new SubstringView("other")));
    }

    public void testChain_MapConstraintsWithOverlap() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();
        builder.add(new StringToIntMapConstraint(Map.of("A", 1, "B", 2, "D", 4)), 0x01);
        builder.add(new StringSetConstraint(Set.of("A", "B", "C")), 0x02);
        builder.add(new EqualsStringConstraint("B"), 0x04);

        ToIntFunction<SubstringView> chain = builder.build();

        assertTrue(chain instanceof SubstringToIntegerMap);

        assertEquals(0x03, chain.applyAsInt(new SubstringView("A"))); // First two bitmasks ORed together
        assertEquals(0x07, chain.applyAsInt(new SubstringView("B"))); // All three bitmasks ORed together
        assertEquals(0x02, chain.applyAsInt(new SubstringView("C")));
        assertEquals(0x01, chain.applyAsInt(new SubstringView("D")));
        assertEquals(0x00, chain.applyAsInt(new SubstringView("E")));
    }

    // Multiple constraints - mixed types

    public void testChain_MapAndNonMapConstraints() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();
        builder.add(new StringSetConstraint(Set.of("short")), 0x01);
        builder.add(new LengthStringConstraint(5), 0x02);

        ToIntFunction<SubstringView> chain = builder.build();

        assertTrue(chain instanceof SubstringToBitmaskChain);

        assertEquals(0x03, chain.applyAsInt(new SubstringView("short"))); // Matches both
        assertEquals(0x02, chain.applyAsInt(new SubstringView("other"))); // Only length
        assertEquals(0x00, chain.applyAsInt(new SubstringView("no")));
    }

    public void testChain_MapAndAnyString() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();
        builder.add(new StringSetConstraint(Set.of("special")), 0x01);
        builder.add(new AnyString(), 0x02);

        ToIntFunction<SubstringView> chain = builder.build();

        assertTrue(chain instanceof SubstringToBitmaskChain);

        assertEquals(0x03, chain.applyAsInt(new SubstringView("special"))); // Map + AnyString
        assertEquals(0x02, chain.applyAsInt(new SubstringView("anything"))); // Just AnyString
        assertEquals(0x02, chain.applyAsInt(new SubstringView("")));
    }

    public void testChain_MultipleNonMapConstraints() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();
        builder.add(new LengthStringConstraint(3), 0x01);
        builder.add(new NotEqualsStringConstraint("foo"), 0x02);

        ToIntFunction<SubstringView> chain = builder.build();

        assertTrue(chain instanceof SubstringToBitmaskChain);

        assertEquals(0x03, chain.applyAsInt(new SubstringView("bar"))); // Both match
        assertEquals(0x01, chain.applyAsInt(new SubstringView("foo"))); // Only length matches
        assertEquals(0x02, chain.applyAsInt(new SubstringView("test"))); // Only not-equals matches
        assertEquals(0x02, chain.applyAsInt(new SubstringView("ab")));
    }

    // Composite constraints in chains

    public void testChain_WithOrConstraint() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();
        // OR constraints should be decomposed into multiple chain entries
        builder.add(new OrStringConstraint(new EqualsStringConstraint("a"), new EqualsStringConstraint("b")), 0x01);
        builder.add(new LengthStringConstraint(1), 0x02);

        ToIntFunction<SubstringView> chain = builder.build();

        assertTrue(chain instanceof SubstringToBitmaskChain);

        assertEquals(0x03, chain.applyAsInt(new SubstringView("a")));
        assertEquals(0x03, chain.applyAsInt(new SubstringView("b")));
        assertEquals(0x02, chain.applyAsInt(new SubstringView("c")));
        assertEquals(0x00, chain.applyAsInt(new SubstringView("ab")));
    }

    public void testChain_WithAndConstraint() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();
        // AND constraints should remain as a single chain entry
        builder.add(new AndStringConstraint(new LengthStringConstraint(3), new NotEqualsStringConstraint("foo")), 0x01);
        builder.add(new EqualsStringConstraint("bar"), 0x02);

        ToIntFunction<SubstringView> chain = builder.build();

        assertTrue(chain instanceof SubstringToBitmaskChain);

        assertEquals(0x03, chain.applyAsInt(new SubstringView("bar"))); // Both constraints
        assertEquals(0x01, chain.applyAsInt(new SubstringView("baz"))); // Only AND constraint
        assertEquals(0x00, chain.applyAsInt(new SubstringView("foo")));
        assertEquals(0x00, chain.applyAsInt(new SubstringView("ab")));
    }

    public void testChain_ComplexCompositeConstraints() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();

        // First constraint: (equals "Jan" OR equals "Feb") with bitmask 0x01
        builder.add(new OrStringConstraint(new EqualsStringConstraint("Jan"), new EqualsStringConstraint("Feb")), 0x01);

        // Second constraint: (length 3 AND not equals "Mar") with bitmask 0x02
        builder.add(new AndStringConstraint(new LengthStringConstraint(3), new NotEqualsStringConstraint("Mar")), 0x02);

        // Third constraint: Set with bitmask 0x04
        builder.add(new StringSetConstraint(Set.of("Apr", "May")), 0x04);

        ToIntFunction<SubstringView> chain = builder.build();

        assertTrue(chain instanceof SubstringToBitmaskChain);

        assertEquals(0x03, chain.applyAsInt(new SubstringView("Jan"))); // OR + AND
        assertEquals(0x03, chain.applyAsInt(new SubstringView("Feb"))); // OR + AND
        assertEquals(0x00, chain.applyAsInt(new SubstringView("Mar"))); // Excluded by AND
        assertEquals(0x06, chain.applyAsInt(new SubstringView("Apr"))); // Only Set
        assertEquals(0x02, chain.applyAsInt(new SubstringView("Jun"))); // Only AND
        assertEquals(0x00, chain.applyAsInt(new SubstringView("December")));
    }

    // Multiple bitmasks for same constraint

    public void testChain_SameConstraintDifferentBitmasks() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();
        builder.add(new EqualsStringConstraint("test"), 0x01);
        builder.add(new EqualsStringConstraint("test"), 0x02);
        builder.add(new EqualsStringConstraint("test"), 0x04);

        ToIntFunction<SubstringView> chain = builder.build();

        // Should be optimized to a map with ORed bitmask
        assertTrue(chain instanceof SubstringToIntegerMap);

        assertEquals(0x07, chain.applyAsInt(new SubstringView("test")));
        assertEquals(0x00, chain.applyAsInt(new SubstringView("other")));
    }

    // Real-world scenarios

    public void testChain_MonthNames() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();

        // Full month names with one bitmask
        builder.add(new StringSetConstraint(Set.of("January", "February", "March")), 0x01);

        // Abbreviated month names with another bitmask
        builder.add(new StringToIntMapConstraint(Map.of("Jan", 1, "Feb", 2, "Mar", 3)), 0x02);

        // Length constraint for any 3-letter month
        builder.add(new LengthStringConstraint(3), 0x04);

        ToIntFunction<SubstringView> chain = builder.build();

        assertEquals(0x01, chain.applyAsInt(new SubstringView("January")));
        assertEquals(0x06, chain.applyAsInt(new SubstringView("Jan"))); // Map + Length
        assertEquals(0x04, chain.applyAsInt(new SubstringView("Apr"))); // Just length
        assertEquals(0x00, chain.applyAsInt(new SubstringView("December")));
    }

    public void testChain_IPv4Octets() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();

        // Common values as a set
        builder.add(new StringSetConstraint(Set.of("0", "1", "127", "255")), 0x01);

        // Any 3-digit value
        builder.add(new LengthStringConstraint(3), 0x02);

        // Not certain invalid values
        builder.add(new NotEqualsStringConstraint("256"), 0x04);

        ToIntFunction<SubstringView> chain = builder.build();

        assertEquals(0x05, chain.applyAsInt(new SubstringView("0"))); // Set + NotEquals
        assertEquals(0x07, chain.applyAsInt(new SubstringView("127"))); // All three
        assertEquals(0x06, chain.applyAsInt(new SubstringView("192"))); // Length + NotEquals
        assertEquals(0x02, chain.applyAsInt(new SubstringView("256"))); // Length only
    }

    public void testChain_EmptyBuilder() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();
        assertNull(builder.build());
    }

    public void testChain_OnlyAnyString() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();
        builder.add(new AnyString(), 0xFF);

        ToIntFunction<SubstringView> chain = builder.build();

        // Should be optimized to a simple function
        assertFalse(chain instanceof SubstringToBitmaskChain);
        assertFalse(chain instanceof SubstringToIntegerMap);

        assertEquals(0xFF, chain.applyAsInt(new SubstringView("anything")));
        assertEquals(0xFF, chain.applyAsInt(new SubstringView("")));
    }

    public void testChain_MultipleAnyStrings() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();
        builder.add(new AnyString(), 0x01);
        builder.add(new AnyString(), 0x02);
        builder.add(new AnyString(), 0x04);

        ToIntFunction<SubstringView> chain = builder.build();

        // All AnyString bitmasks should be ORed together
        assertEquals(0x07, chain.applyAsInt(new SubstringView("anything")));
        assertEquals(0x07, chain.applyAsInt(new SubstringView("")));
    }

    public void testChain_LargeNumberOfConstraints() {
        SubstringToBitmaskChain.Builder builder = SubstringToBitmaskChain.builder();

        // Add many different constraints
        for (int i = 0; i < 10; i++) {
            builder.add(new EqualsStringConstraint("val" + i), 1 << i);
        }
        builder.add(new LengthStringConstraint(4), 1 << 10);
        builder.add(new NotEqualsStringConstraint("none"), 1 << 11);

        ToIntFunction<SubstringView> chain = builder.build();

        assertTrue(chain instanceof SubstringToBitmaskChain);

        assertEquals((1) | (1 << 10) | (1 << 11), chain.applyAsInt(new SubstringView("val0")));
        assertEquals((1 << 5) | (1 << 10) | (1 << 11), chain.applyAsInt(new SubstringView("val5")));
        assertEquals((1 << 10) | (1 << 11), chain.applyAsInt(new SubstringView("test")));
        assertEquals(1 << 11, chain.applyAsInt(new SubstringView("testing")));
        assertEquals(1 << 10, chain.applyAsInt(new SubstringView("none")));
    }
}

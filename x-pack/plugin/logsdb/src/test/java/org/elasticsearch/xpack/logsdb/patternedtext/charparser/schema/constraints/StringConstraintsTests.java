/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.constraints;

import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.instanceOf;

public class StringConstraintsTests extends ESTestCase {

    public void testEqualsConstraint() {
        StringConstraint constraint = new EqualsStringConstraint("test");
        assertTrue(constraint.isApplicable("test"));
        assertFalse(constraint.isApplicable("Test"));
        assertFalse(constraint.isApplicable("testing"));
    }

    public void testNotEqualsConstraint() {
        StringConstraint constraint = new NotEqualsStringConstraint("test");
        assertTrue(constraint.isApplicable("Test"));
        assertTrue(constraint.isApplicable("testing"));
        assertFalse(constraint.isApplicable("test"));
    }

    public void testSetConstraint() {
        StringConstraint constraint = new StringSetConstraint(Set.of("One", "Two", "Three"));
        assertTrue(constraint.isApplicable("One"));
        assertTrue(constraint.isApplicable("Two"));
        assertTrue(constraint.isApplicable("Three"));
        assertFalse(constraint.isApplicable("Four"));
    }

    public void testMapConstraintWithParsing() {
        StringConstraint constraint = StringConstraints.parseStringConstraint("one=1| two=2| three = 3   ");
        assertThat(constraint, instanceOf(StringToIntMapConstraint.class));
        assertTrue(constraint.isApplicable("one"));
        assertTrue(constraint.isApplicable("two"));
        assertTrue(constraint.isApplicable("three"));
        assertFalse(constraint.isApplicable("four"));
    }

    public void testLengthConstraint() {
        StringConstraint constraint = new LengthStringConstraint(3);
        assertTrue(constraint.isApplicable("abc"));
        assertTrue(constraint.isApplicable("123"));
        assertFalse(constraint.isApplicable("abcd"));
        assertFalse(constraint.isApplicable("ab"));
    }

    public void testAndConstraint() {
        StringConstraint constraint1 = new EqualsStringConstraint("test");
        StringConstraint constraint2 = new LengthStringConstraint(4);
        StringConstraint combined = constraint1.and(constraint2);

        assertThat(combined, instanceOf(AndStringConstraint.class));
        AndStringConstraint andConstraint = (AndStringConstraint) combined;
        assertEquals(constraint1, andConstraint.first());
        assertEquals(constraint2, andConstraint.second());

        assertTrue(combined.isApplicable("test"));
        assertFalse(combined.isApplicable("Test"));
        assertFalse(combined.isApplicable("testing"));
    }

    public void testOrConstraint() {
        StringConstraint constraint1 = new EqualsStringConstraint("test");
        StringConstraint constraint2 = new EqualsStringConstraint("Test");
        StringConstraint combined = constraint1.or(constraint2);

        assertThat(combined, instanceOf(OrStringConstraint.class));
        OrStringConstraint orConstraint = (OrStringConstraint) combined;
        assertEquals(constraint1, orConstraint.first());
        assertEquals(constraint2, orConstraint.second());

        assertTrue(combined.isApplicable("test"));
        assertTrue(combined.isApplicable("Test"));
        assertFalse(combined.isApplicable("testing"));
    }

    public void testNullOrEmptyConstraint() {
        assertEquals(AnyString.INSTANCE, StringConstraints.parseStringConstraint(null));
        assertEquals(AnyString.INSTANCE, StringConstraints.parseStringConstraint(""));
        assertEquals(AnyString.INSTANCE, StringConstraints.parseStringConstraint("   "));
    }

    public void testWhitespaceHandling() {
        StringConstraint standard = StringConstraints.parseStringConstraint("One|Two");
        assertThat(standard, instanceOf(StringSetConstraint.class));
        StringConstraint withSpaces = StringConstraints.parseStringConstraint(" One | Two ");
        assertThat(withSpaces, instanceOf(StringSetConstraint.class));
        StringConstraint manySpaces = StringConstraints.parseStringConstraint("   One    |    Two   ");
        assertThat(manySpaces, instanceOf(StringSetConstraint.class));

        assertTrue(standard.isApplicable("One"));
        assertTrue(withSpaces.isApplicable("Two"));
        assertTrue(manySpaces.isApplicable("One"));
        assertFalse(standard.isApplicable("Three"));
        assertFalse(withSpaces.isApplicable("Three"));
        assertFalse(manySpaces.isApplicable("Three"));
    }

    public void testComplexNestedConstraint() {
        StringConstraint constraint1 = StringConstraints.parseStringConstraint("One|Two");
        StringConstraint constraint2 = StringConstraints.parseStringConstraint("Three|Four");
        StringConstraint combined = constraint1.or(constraint2);

        assertTrue(combined.isApplicable("One"));
        assertTrue(combined.isApplicable("Three"));
        assertFalse(combined.isApplicable("Five"));
    }

    public void testGetValidCharactersEqualsConstraint() {
        StringConstraint constraint = new EqualsStringConstraint("test");
        char[] validChars = constraint.getValidCharacters();

        assertNotNull(validChars);
        assertEquals(3, validChars.length); // 't', 'e', 's'
        String result = new String(validChars);
        assertTrue(result.contains("t"));
        assertTrue(result.contains("e"));
        assertTrue(result.contains("s"));
    }

    public void testGetValidCharactersSetConstraint() {
        StringConstraint constraint = new StringSetConstraint(Set.of("One", "Two", "Three"));
        char[] validChars = constraint.getValidCharacters();

        assertNotNull(validChars);
        assertEquals(8, validChars.length); // 'O', 'n', 'e', 'T', 'w', 'o', 'h', 'r'
        String result = new String(validChars);
        assertTrue(result.contains("O"));
        assertTrue(result.contains("n"));
        assertTrue(result.contains("e"));
        assertTrue(result.contains("T"));
        assertTrue(result.contains("w"));
        assertTrue(result.contains("o"));
        assertTrue(result.contains("h"));
        assertTrue(result.contains("r"));
    }

    public void testGetValidCharactersLengthConstraint() {
        StringConstraint constraint = new LengthStringConstraint(3);
        char[] validChars = constraint.getValidCharacters();

        assertNull(validChars); // Length constraint does not define valid characters
    }

    public void testGetValidCharactersAndConstraint() {
        StringConstraint constraint1 = new EqualsStringConstraint("test");
        StringConstraint constraint2 = new StringSetConstraint(Set.of("test", "testing"));
        StringConstraint combined = constraint1.and(constraint2);

        char[] validChars = combined.getValidCharacters();

        assertNotNull(validChars);
        assertEquals(3, validChars.length); // Intersection: 't', 'e', 's'
        String result = new String(validChars);
        assertTrue(result.contains("t"));
        assertTrue(result.contains("e"));
        assertTrue(result.contains("s"));
    }

    public void testGetValidCharactersOrConstraint() {
        StringConstraint constraint1 = new EqualsStringConstraint("test");
        StringConstraint constraint2 = new StringSetConstraint(Set.of("One", "Two"));
        StringConstraint combined = constraint1.or(constraint2);

        char[] validChars = combined.getValidCharacters();

        assertNotNull(validChars);
        assertEquals(8, validChars.length); // Union: 't', 'e', 's', 'O', 'n', 'T', 'w', 'o'
        String result = new String(validChars);
        assertTrue(result.contains("t"));
        assertTrue(result.contains("e"));
        assertTrue(result.contains("s"));
        assertTrue(result.contains("O"));
        assertTrue(result.contains("n"));
        assertTrue(result.contains("T"));
        assertTrue(result.contains("w"));
        assertTrue(result.contains("o"));
    }
}

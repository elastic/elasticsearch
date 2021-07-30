/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Utility class that encapsulates standard checks and assertions around testing the equals() and hashCode()
 * methods of objects that implement them.
 */
public class EqualsHashCodeTestUtils {

    private static Object[] someObjects = new Object[] { "some string", Integer.valueOf(1), Double.valueOf(1.0) };

    /**
     * A function that makes a copy of its input argument
     */
    public interface CopyFunction<T> {
        T copy(T t) throws IOException;
    }

    /**
     * A function that creates a copy of its input argument that is different from its
     * input in exactly one aspect (e.g. one parameter of a class instance should change)
     */
    public interface MutateFunction<T> {
        T mutate(T t) throws IOException;
    }

    /**
     * Perform common equality and hashCode checks on the input object
     * @param original the object under test
     * @param copyFunction a function that creates a deep copy of the input object
     */
    public static <T> void checkEqualsAndHashCode(T original, CopyFunction<T> copyFunction) {
        checkEqualsAndHashCode(original, copyFunction, null);
    }

    /**
     * Perform common equality and hashCode checks on the input object
     * @param original the object under test
     * @param copyFunction a function that creates a deep copy of the input object
     * @param mutationFunction a function that creates a copy of the input object that is different
     * from the input in one aspect. The output of this call is used to check that it is not equal()
     * to the input object
     */
    public static <T> void checkEqualsAndHashCode(T original, CopyFunction<T> copyFunction,
            MutateFunction<T> mutationFunction) {
        try {
            String objectName = original.getClass().getSimpleName();
            assertFalse(objectName + " is equal to null", original.equals(null));
            // TODO not sure how useful the following test is
            assertFalse(objectName + " is equal to incompatible type", original.equals(ESTestCase.randomFrom(someObjects)));
            assertTrue(objectName + " is not equal to self", original.equals(original));
            assertThat(objectName + " hashcode returns different values if called multiple times", original.hashCode(),
                    equalTo(original.hashCode()));
            if (mutationFunction != null) {
                T mutation = mutationFunction.mutate(original);
                assertThat(objectName + " mutation should not be equal to original", mutation,
                        not(equalTo(original)));
            }

            T copy = copyFunction.copy(original);
            assertTrue(objectName + " copy is not equal to self", copy.equals(copy));
            assertTrue(objectName + " is not equal to its copy", original.equals(copy));
            assertTrue("equals is not symmetric", copy.equals(original));
            assertThat(objectName + " hashcode is different from copies hashcode", copy.hashCode(), equalTo(original.hashCode()));

            T secondCopy = copyFunction.copy(copy);
            assertTrue("second copy is not equal to self", secondCopy.equals(secondCopy));
            assertTrue("copy is not equal to its second copy", copy.equals(secondCopy));
            assertThat("second copy's hashcode is different from original hashcode", copy.hashCode(), equalTo(secondCopy.hashCode()));
            assertTrue("equals is not transitive", original.equals(secondCopy));
            assertTrue("equals is not symmetric", secondCopy.equals(copy));
            assertTrue("equals is not symmetric", secondCopy.equals(original));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

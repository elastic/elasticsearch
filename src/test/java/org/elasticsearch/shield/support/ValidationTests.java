/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.commons.lang3.ArrayUtils.add;
import static org.apache.commons.lang3.ArrayUtils.addAll;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class ValidationTests extends ElasticsearchTestCase {


    private static final char[] alphabet = {
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'
    };

    private static final char[] numbers = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };

    private static final char[] allowedFirstChars = add(alphabet, '_');

    private static final char[] allowedSubsequent = addAll(addAll(alphabet, numbers), new char[] { '_', '@', '-', '$' });

    static {
        Arrays.sort(allowedFirstChars);
        Arrays.sort(allowedSubsequent);
    }

    @Test
    @Repeat(iterations = 100)
    public void testESUsers_validateUsername() throws Exception {
        int length = randomIntBetween(1, 30);
        String name = new String(generateValidName(length));
        assertThat(Validation.ESUsers.validateUsername(name), nullValue());
    }

    @Test @Repeat(iterations = 100)
    public void testESUsers_validateUsername_Invalid_Length() throws Exception {
        int length = frequently() ? randomIntBetween(31, 200) : 0; // invalid length
        char[] name = new char[length];
        if (length > 0) {
            name = generateValidName(length);
        }
        assertThat(Validation.ESUsers.validateUsername(new String(name)), notNullValue());
    }

    @Test @Repeat(iterations = 100)
    public void testESUsers_validateUsername_Invalid_Characters() throws Exception {
        int length = randomIntBetween(1, 30); // valid length
        String name = new String(generateInvalidName(length));
        assertThat(Validation.ESUsers.validateUsername(name), notNullValue());
    }

    @Test @Repeat(iterations = 100)
    public void testESUsers_validatePassword() throws Exception {
        String passwd = randomAsciiOfLength(randomIntBetween(0, 20));
        logger.info(passwd + "[{}]", passwd.length());
        if (passwd.length() >= 6) {
            assertThat(Validation.ESUsers.validatePassword(passwd.toCharArray()), nullValue());
        } else {
            assertThat(Validation.ESUsers.validatePassword(passwd.toCharArray()), notNullValue());
        }
    }

    @Test @Repeat(iterations = 100)
    public void testRoles_validateRoleName() throws Exception {
        int length = randomIntBetween(1, 30);
        String name = new String(generateValidName(length));
        assertThat(Validation.Roles.validateRoleName(name), nullValue());
    }

    @Test @Repeat(iterations = 100)
    public void testRoles_validateRoleName_Invalid_Length() throws Exception {
        int length = frequently() ? randomIntBetween(31, 200) : 0; // invalid length
        char[] name = new char[length];
        if (length > 0) {
            name = generateValidName(length);
        }
        assertThat(Validation.ESUsers.validateUsername(new String(name)), notNullValue());
    }

    @Test @Repeat(iterations = 100)
    public void testRoles_validateRoleName_Invalid_Characters() throws Exception {
        int length = randomIntBetween(1, 30); // valid length
        String name = new String(generateInvalidName(length));
        assertThat(Validation.ESUsers.validateUsername(name), notNullValue());
    }

    private static char[] generateValidName(int length) {
        char first = allowedFirstChars[randomIntBetween(0, allowedFirstChars.length - 1)];
        char[] subsequent = new char[length - 1];
        for (int i = 0; i < subsequent.length; i++) {
            subsequent[i] = allowedSubsequent[randomIntBetween(0, allowedSubsequent.length - 1)];
        }
        return addAll(new char[] { first }, subsequent);
    }

    private static char[] generateInvalidName(int length) {

        if (length == 1 || randomBoolean()) {

            // invalid name due to charaters not allowed in the beginning of the name

            char first;
            while (true) {
                first = randomUnicodeOfLength(1).charAt(0);
                if (Arrays.binarySearch(allowedFirstChars, first) < 0) {
                    break;
                }
            }
            char[] subsequent = new char[length - 1];
            for (int i = 0; i < subsequent.length; i++) {
                subsequent[i] = allowedSubsequent[randomIntBetween(0, allowedSubsequent.length - 1)];
            }
            return addAll(new char[] { first }, subsequent);
        }

        // invalid name due to charaters not allowed within the name itself

        char first = allowedFirstChars[randomIntBetween(0, allowedFirstChars.length - 1)];
        char[] subsequent = new char[length - 1];
        for (int i = 0; i < subsequent.length; i++) {
            char c;
            while (true) {
                c = randomUnicodeOfLength(1).charAt(0);
                if (Arrays.binarySearch(allowedSubsequent, c) < 0) {
                    break;
                }
            }
            subsequent[i] = c;
        }
        return addAll(new char[] { first }, subsequent);
    }


}

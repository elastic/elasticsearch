/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ServiceAccountTokenTests extends ESTestCase {

    private static final Set<Character> VALID_TOKEN_NAME_CHARS = Set.of(
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
        'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
        'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        '-', '_'
    );

    private static final Set<Character> INVALID_TOKEN_NAME_CHARS = Set.of(
        '!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '.', '/', ':', ';', '<', '=', '>', '?', '@', '[',
        '\\', ']', '^', '`', '{', '|', '}', '~', ' ', '\t', '\n', '\r');

    public void testIsValidTokenName() {
        final String tokenName1 = randomTokenName();
        assertThat(ServiceAccountToken.isValidTokenName(tokenName1), is(true));

        final String tokenName2 = "_" + randomTokenName().substring(1);
        assertThat(ServiceAccountToken.isValidTokenName(tokenName2), is(false));

        assertThat(ServiceAccountToken.isValidTokenName(null), is(false));

        final String tokenName3 = randomInvalidTokenName();
        assertThat(ServiceAccountToken.isValidTokenName(tokenName3), is(false));
    }

    public void testNewToken() {
        final ServiceAccountId accountId = new ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        ServiceAccountToken.newToken(accountId, randomTokenName());

        final IllegalArgumentException e1 =
            expectThrows(IllegalArgumentException.class, () -> ServiceAccountToken.newToken(accountId, randomInvalidTokenName()));
        assertThat(e1.getMessage(), containsString(ServiceAccountToken.INVALID_TOKEN_NAME_MESSAGE));

        final NullPointerException e2 =
            expectThrows(NullPointerException.class, () -> ServiceAccountToken.newToken(null, randomTokenName()));
        assertThat(e2.getMessage(), containsString("service account ID cannot be null"));
    }

    public void testServiceAccountTokenNew() {
        final ServiceAccountId accountId = new ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        final SecureString secret = new SecureString(randomAlphaOfLength(20).toCharArray());
        new ServiceAccountToken(accountId, randomTokenName(), secret);

        final NullPointerException e1 =
            expectThrows(NullPointerException.class, () -> new ServiceAccountToken(null, randomTokenName(), secret));
        assertThat(e1.getMessage(), containsString("service account ID cannot be null"));

        final IllegalArgumentException e2 =
            expectThrows(IllegalArgumentException.class, () -> new ServiceAccountToken(accountId, randomInvalidTokenName(), secret));
        assertThat(e2.getMessage(), containsString(ServiceAccountToken.INVALID_TOKEN_NAME_MESSAGE));

        final NullPointerException e3 =
            expectThrows(NullPointerException.class, () -> new ServiceAccountToken(accountId, randomTokenName(), null));
        assertThat(e3.getMessage(), containsString("service account token secret cannot be null"));
    }

    public void testBearerString() throws IOException {
        final ServiceAccountToken serviceAccountToken =
            new ServiceAccountToken(new ServiceAccountId("elastic", "fleet"),
                "token1", new SecureString("supersecret".toCharArray()));
        assertThat(serviceAccountToken.asBearerString(), equalTo("AAEAAWVsYXN0aWMvZmxlZXQvdG9rZW4xOnN1cGVyc2VjcmV0"));

        assertThat(ServiceAccountToken.fromBearerString(new SecureString("AAEAAWVsYXN0aWMvZmxlZXQvdG9rZW4xOnN1cGVyc2VjcmV0".toCharArray())),
            equalTo(serviceAccountToken));

        final ServiceAccountId accountId = new ServiceAccountId(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8));
        final ServiceAccountToken serviceAccountToken1 = ServiceAccountToken.newToken(accountId, randomTokenName());
        assertThat(ServiceAccountToken.fromBearerString(serviceAccountToken1.asBearerString()), equalTo(serviceAccountToken1));
    }

    public static String randomTokenName() {
        final Character[] chars = randomArray(
            1,
            256,
            Character[]::new,
            () -> randomFrom(VALID_TOKEN_NAME_CHARS));
        final String name = Arrays.stream(chars).map(String::valueOf).collect(Collectors.joining());
        return name.startsWith("_") ? "-" + name.substring(1) : name;
    }

    public static String randomInvalidTokenName() {
        if (randomBoolean()) {
            final String tokenName = randomTokenName();
            final char[] chars = tokenName.toCharArray();
            IntStream.rangeClosed(1, randomIntBetween(1, chars.length))
                .forEach(i -> chars[randomIntBetween(0, chars.length - 1)] = randomFrom(INVALID_TOKEN_NAME_CHARS));
            return new String(chars);
        } else {
            return randomFrom("", " ", randomAlphaOfLength(257));
        }
    }
}

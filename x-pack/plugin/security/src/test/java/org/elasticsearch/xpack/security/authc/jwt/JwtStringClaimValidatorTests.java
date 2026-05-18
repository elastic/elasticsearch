/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jwt.JWTClaimsSet;

import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;

import java.text.ParseException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class JwtStringClaimValidatorTests extends ESTestCase {

    public void testClaimIsNotString() throws ParseException {
        final String claimName = randomAlphaOfLength(10);
        final String fallbackClaimName = randomAlphaOfLength(12);

        final JwtStringClaimValidator validator;
        final JWTClaimsSet jwtClaimsSet;
        if (randomBoolean()) {
            validator = new JwtStringClaimValidator(claimName, randomBoolean(), List.of(), List.of());
            // fallback claim is ignored
            jwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, List.of(42), fallbackClaimName, randomAlphaOfLength(8)));
        } else {
            validator = new JwtStringClaimValidator(claimName, randomBoolean(), Map.of(claimName, fallbackClaimName), List.of(), List.of());
            jwtClaimsSet = JWTClaimsSet.parse(Map.of(fallbackClaimName, List.of(42)));
        }

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(getJwsHeader(), jwtClaimsSet)
        );
        assertThat(e.getMessage(), containsString("cannot parse string claim"));
        assertThat(e.getCause(), instanceOf(ParseException.class));
    }

    public void testClaimIsNotSingleValued() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final String fallbackClaimName = randomAlphaOfLength(8);

        final JwtStringClaimValidator validator;
        final JWTClaimsSet jwtClaimsSet;
        if (randomBoolean()) {
            validator = new JwtStringClaimValidator(claimName, true, List.of(), List.of());
            // fallback claim is ignored
            jwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, List.of("foo", "bar"), fallbackClaimName, randomAlphaOfLength(8)));
        } else {
            validator = new JwtStringClaimValidator(claimName, true, Map.of(claimName, fallbackClaimName), List.of(), List.of());
            jwtClaimsSet = JWTClaimsSet.parse(Map.of(fallbackClaimName, List.of("foo", "bar")));
        }

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(getJwsHeader(), jwtClaimsSet)
        );
        assertThat(e.getMessage(), containsString("cannot parse string claim"));
        assertThat(e.getCause(), instanceOf(ParseException.class));
    }

    public void testClaimDoesNotExist() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final String fallbackClaimName = randomAlphaOfLength(8);

        final JwtStringClaimValidator validator;
        final JWTClaimsSet jwtClaimsSet;
        if (randomBoolean()) {
            validator = new JwtStringClaimValidator(claimName, randomBoolean(), List.of(), List.of());
        } else {
            validator = new JwtStringClaimValidator(claimName, randomBoolean(), Map.of(claimName, fallbackClaimName), List.of(), List.of());
        }
        jwtClaimsSet = JWTClaimsSet.parse(Map.of());

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(getJwsHeader(), jwtClaimsSet)
        );
        assertThat(e.getMessage(), containsString("missing required string claim"));
    }

    public void testMatchingClaimValues() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final String fallbackClaimName = randomAlphaOfLength(8);
        final String claimValue = randomAlphaOfLength(10);
        final boolean singleValuedClaim = randomBoolean();
        final List<String> allowedClaimValues = List.of(claimValue, randomAlphaOfLengthBetween(11, 20));
        final Object incomingClaimValue = singleValuedClaim ? claimValue : randomFrom(claimValue, List.of(claimValue, "other-stuff"));

        final JwtStringClaimValidator validator;
        final JWTClaimsSet validJwtClaimsSet;
        final boolean noFallback = randomBoolean();
        if (noFallback) {
            validator = new JwtStringClaimValidator(claimName, singleValuedClaim, allowedClaimValues, List.of());
            // fallback claim is ignored
            validJwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, incomingClaimValue, fallbackClaimName, List.of(42)));
        } else {
            validator = new JwtStringClaimValidator(
                claimName,
                singleValuedClaim,
                Map.of(claimName, fallbackClaimName),
                allowedClaimValues,
                List.of()
            );
            validJwtClaimsSet = JWTClaimsSet.parse(Map.of(fallbackClaimName, incomingClaimValue));
        }

        try {
            validator.validate(getJwsHeader(), validJwtClaimsSet);
        } catch (Exception e) {
            throw new AssertionError("validation should have passed without exception", e);
        }

        String invalidClaimValue;
        if (randomBoolean()) {
            invalidClaimValue = "not-" + claimValue;
        } else {
            // letter case mismatch: invert case at pos i
            int i = randomIntBetween(0, claimValue.length() - 1);
            invalidClaimValue = claimValue.substring(0, i);
            if (Character.isUpperCase(claimValue.charAt(i))) {
                invalidClaimValue += claimValue.substring(i, i).toLowerCase(Locale.ROOT);
            } else if (Character.isLowerCase(claimValue.charAt(i))) {
                invalidClaimValue += claimValue.substring(i, i).toUpperCase(Locale.ROOT);
            } else {
                throw new AssertionError("Unrecognized case");
            }
            invalidClaimValue += claimValue.substring(i + 1);
        }
        {
            final JWTClaimsSet invalidJwtClaimsSet;
            if (noFallback) {
                // fallback is ignored (even when it has a valid value) since the main claim exists
                invalidJwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, invalidClaimValue, fallbackClaimName, claimValue));
            } else {
                invalidJwtClaimsSet = JWTClaimsSet.parse(Map.of(fallbackClaimName, invalidClaimValue));
            }

            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> validator.validate(getJwsHeader(), invalidJwtClaimsSet)
            );
            assertThat(e.getMessage(), containsString("does not match allowed claim values"));
        }
    }

    public void testWildcardAndRegexMatchingClaimValues() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final String fallbackClaimName = randomAlphaOfLength(8);
        final List<String> allowedClaimPatterns = List.of("a?\\**", "/https?://[^/]+/?/");

        final boolean noFallback = randomBoolean();
        final JwtStringClaimValidator validator;
        if (noFallback) {
            validator = new JwtStringClaimValidator(claimName, false, List.of(), allowedClaimPatterns);
        } else {
            validator = new JwtStringClaimValidator(
                claimName,
                false,
                Map.of(claimName, fallbackClaimName),
                List.of(),
                allowedClaimPatterns
            );
        }
        for (String incomingClaimValue : List.of("a1*", "ab*whatever", "https://elastic.co/")) {
            final JWTClaimsSet validJwtClaimsSet;
            if (noFallback) {
                // fallback claim is ignored
                validJwtClaimsSet = JWTClaimsSet.parse(
                    Map.of(
                        claimName,
                        randomBoolean() ? incomingClaimValue : List.of(incomingClaimValue, "other_stuff"),
                        fallbackClaimName,
                        List.of(42)
                    )
                );
            } else {
                validJwtClaimsSet = JWTClaimsSet.parse(
                    Map.of(fallbackClaimName, randomBoolean() ? incomingClaimValue : List.of(incomingClaimValue, "other_stuff"))
                );
            }
            try {
                validator.validate(getJwsHeader(), validJwtClaimsSet);
            } catch (Exception e) {
                throw new AssertionError("validation should have passed without exception", e);
            }
        }
        for (String invalidIncomingClaimValue : List.of("a", "abc", "abc*", "https://elastic.co/guide")) {
            final JWTClaimsSet invalidJwtClaimsSet;
            if (noFallback) {
                // fallback claim is ignored
                invalidJwtClaimsSet = JWTClaimsSet.parse(
                    Map.of(
                        claimName,
                        randomBoolean() ? invalidIncomingClaimValue : List.of(invalidIncomingClaimValue, "other_stuff"),
                        fallbackClaimName,
                        List.of(42)
                    )
                );
            } else {
                invalidJwtClaimsSet = JWTClaimsSet.parse(
                    Map.of(
                        fallbackClaimName,
                        randomBoolean() ? invalidIncomingClaimValue : List.of(invalidIncomingClaimValue, "other_stuff")
                    )
                );
            }
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> validator.validate(getJwsHeader(), invalidJwtClaimsSet)
            );
            assertThat(e.getMessage(), containsString("does not match allowed claim values"));
        }
    }

    public void testValueAllowSettingDoesNotSupportWildcardOrRegex() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final String fallbackClaimName = randomAlphaOfLength(8);
        final String claimValue = randomFrom("*", "/.*/");

        final JwtStringClaimValidator validator;
        final JWTClaimsSet invalidJwtClaimsSet;
        final boolean noFallback = randomBoolean();
        if (noFallback) {
            validator = new JwtStringClaimValidator(claimName, randomBoolean(), List.of(claimValue), List.of());
            // fallback is ignored (even when it has a valid value) since the main claim exists
            invalidJwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, randomAlphaOfLengthBetween(1, 10), fallbackClaimName, claimValue));
        } else {
            validator = new JwtStringClaimValidator(
                claimName,
                randomBoolean(),
                Map.of(claimName, fallbackClaimName),
                List.of(claimValue),
                List.of()
            );
            invalidJwtClaimsSet = JWTClaimsSet.parse(Map.of(fallbackClaimName, randomAlphaOfLengthBetween(1, 10)));
        }

        // It should not match arbitrary claim value because wildcard or regex is not supported
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validate(getJwsHeader(), invalidJwtClaimsSet)
        );
        assertThat(e.getMessage(), containsString("does not match allowed claim values"));

        // It should support literal matching
        final JWTClaimsSet validJwtClaimsSet;
        if (noFallback) {
            // fallback claim is ignored
            validJwtClaimsSet = JWTClaimsSet.parse(Map.of(claimName, claimValue, fallbackClaimName, randomAlphaOfLength(10)));
        } else {
            validJwtClaimsSet = JWTClaimsSet.parse(Map.of(fallbackClaimName, claimValue));
        }
        try {
            validator.validate(getJwsHeader(), validJwtClaimsSet);
        } catch (Exception e2) {
            throw new AssertionError("validation should have passed without exception", e2);
        }
    }

    public void testSinglePatternSingleClaim() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final String fallbackClaimName = randomAlphaOfLength(8);
        final String claimPattern = randomFrom("a*", "/a.*/");
        final JwtStringClaimValidator validator;
        final JWTClaimsSet singleValueClaimSet;
        final boolean noFallback = randomBoolean();
        if (noFallback) {
            validator = new JwtStringClaimValidator(claimName, randomBoolean(), List.of(), List.of(claimPattern));
            singleValueClaimSet = JWTClaimsSet.parse(
                Map.of(claimName, "a_claim", fallbackClaimName, randomFrom(List.of("invalid", "invalid2"), "invalid"), "something", "else")
            );
        } else {
            validator = new JwtStringClaimValidator(
                claimName,
                randomBoolean(),
                Map.of(claimName, fallbackClaimName),
                List.of(),
                List.of(claimPattern)
            );
            singleValueClaimSet = JWTClaimsSet.parse(Map.of(fallbackClaimName, "a_fallback_claim", "something", "else"));
        }
        try {
            validator.validate(getJwsHeader(), singleValueClaimSet);
        } catch (Exception e) {
            throw new AssertionError("validation should have passed without exception", e);
        }
        {
            JWTClaimsSet invalidClaimSet = JWTClaimsSet.parse(
                Map.of(claimName, "invalid", fallbackClaimName, randomFrom(List.of("a_claim", "a_claim2"), "a_claim"), "something", "else")
            );
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> validator.validate(getJwsHeader(), invalidClaimSet)
            );
            assertThat(e.getMessage(), containsString("does not match allowed claim values"));
        }
        {
            JWTClaimsSet invalidClaimSet = JWTClaimsSet.parse(Map.of(fallbackClaimName, "invalid", "something", "else"));
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> validator.validate(getJwsHeader(), invalidClaimSet)
            );
            if (noFallback) {
                assertThat(e.getMessage(), containsString("missing required string claim"));
            } else {
                assertThat(e.getMessage(), containsString("does not match allowed claim values"));
            }
        }
    }

    public void testPatternListSingleClaim() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final String fallbackClaimName = randomAlphaOfLength(8);
        final List<String> claimPatterns = List.of("a*", "/b.*b/");
        final JwtStringClaimValidator validator;
        final JWTClaimsSet singleValueClaimSet;
        final boolean noFallback = randomBoolean();
        if (noFallback) {
            validator = new JwtStringClaimValidator(claimName, randomBoolean(), List.of(), claimPatterns);
            singleValueClaimSet = JWTClaimsSet.parse(
                Map.of(
                    claimName,
                    "b_claim_b",
                    fallbackClaimName,
                    randomFrom(List.of("invalid", "invalid2"), "invalid"),
                    "something",
                    "else"
                )
            );
        } else {
            validator = new JwtStringClaimValidator(
                claimName,
                randomBoolean(),
                Map.of(claimName, fallbackClaimName),
                List.of(),
                claimPatterns
            );
            singleValueClaimSet = JWTClaimsSet.parse(Map.of(fallbackClaimName, "b_fallback_claim_b", "something", "else"));
        }
        try {
            validator.validate(getJwsHeader(), singleValueClaimSet);
        } catch (Exception e) {
            throw new AssertionError("validation should have passed without exception", e);
        }
        {
            JWTClaimsSet invalidClaimSet = JWTClaimsSet.parse(
                Map.of(
                    claimName,
                    "invalid",
                    fallbackClaimName,
                    randomFrom(List.of("b_claim_b", "b_claim2_b"), "b_claim_b"),
                    "something",
                    "else"
                )
            );
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> validator.validate(getJwsHeader(), invalidClaimSet)
            );
            assertThat(e.getMessage(), containsString("does not match allowed claim values"));
        }
        {
            JWTClaimsSet invalidClaimSet = JWTClaimsSet.parse(Map.of(fallbackClaimName, "invalid", "something", "else"));
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> validator.validate(getJwsHeader(), invalidClaimSet)
            );
            if (noFallback) {
                assertThat(e.getMessage(), containsString("missing required string claim"));
            } else {
                assertThat(e.getMessage(), containsString("does not match allowed claim values"));
            }
        }
    }

    public void testPatternListClaimList() throws ParseException {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final String fallbackClaimName = randomAlphaOfLength(8);
        final List<String> claimPatterns = List.of("a*", "/b.*b/");
        final JwtStringClaimValidator validator;
        final JWTClaimsSet singleValueClaimSet;
        final boolean noFallback = randomBoolean();
        if (noFallback) {
            validator = new JwtStringClaimValidator(claimName, false, List.of(), claimPatterns);
            singleValueClaimSet = JWTClaimsSet.parse(
                Map.of(
                    claimName,
                    List.of("invalid", "b_claim_b"),
                    fallbackClaimName,
                    randomFrom(List.of("invalid", "invalid2"), "invalid"),
                    "something",
                    "else"
                )
            );
        } else {
            validator = new JwtStringClaimValidator(claimName, false, Map.of(claimName, fallbackClaimName), List.of(), claimPatterns);
            singleValueClaimSet = JWTClaimsSet.parse(
                Map.of(fallbackClaimName, List.of("invalid", "b_fallback_claim_b"), "something", "else")
            );
        }
        try {
            validator.validate(getJwsHeader(), singleValueClaimSet);
        } catch (Exception e) {
            throw new AssertionError("validation should have passed without exception", e);
        }
        {
            JWTClaimsSet invalidClaimSet = JWTClaimsSet.parse(
                Map.of(
                    claimName,
                    List.of("invalid", "invalid2"),
                    fallbackClaimName,
                    randomFrom(List.of("b_claim_b", "a_claim"), "b_claim_b"),
                    "something",
                    "else"
                )
            );
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> validator.validate(getJwsHeader(), invalidClaimSet)
            );
            assertThat(e.getMessage(), containsString("does not match allowed claim values"));
        }
        {
            JWTClaimsSet invalidClaimSet = JWTClaimsSet.parse(
                Map.of(fallbackClaimName, List.of("invalid", "invalid2"), "something", "else")
            );
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> validator.validate(getJwsHeader(), invalidClaimSet)
            );
            if (noFallback) {
                assertThat(e.getMessage(), containsString("missing required string claim"));
            } else {
                assertThat(e.getMessage(), containsString("does not match allowed claim values"));
            }
        }
    }

    public void testBothPatternAndSimpleValue() {
        final String claimName = randomAlphaOfLengthBetween(10, 18);
        final String fallbackClaimName = randomFrom(randomAlphaOfLength(8), null);
        final List<String> claimPatterns = List.of("a*", "/.*Z.*/", "*b");
        final List<String> claimValues = List.of("c", "dd", "eZe");
        final JwtStringClaimValidator singleValueValidator = new JwtStringClaimValidator(
            claimName,
            randomBoolean(),
            fallbackClaimName == null ? null : Map.of(claimName, fallbackClaimName),
            claimValues,
            claimPatterns
        );
        for (String claimValue : List.of("a_claim", "anotZer_claim", "Z", "claim_b", "c", "dd", "eZe")) {
            if (fallbackClaimName != null) {
                try {
                    singleValueValidator.validate(
                        getJwsHeader(),
                        JWTClaimsSet.parse(Map.of(fallbackClaimName, claimValue, "something", "else"))
                    );
                } catch (Exception e) {
                    throw new AssertionError("validation should have passed without exception", e);
                }
            } else {
                try {
                    singleValueValidator.validate(getJwsHeader(), JWTClaimsSet.parse(Map.of(claimName, claimValue, "something", "else")));
                } catch (Exception e) {
                    throw new AssertionError("validation should have passed without exception", e);
                }
            }
        }
        for (String invalidClaimValue : List.of("invalid", "cc", "ca", "dda", "ba")) {
            IllegalArgumentException e;
            if (fallbackClaimName != null) {
                e = expectThrows(
                    IllegalArgumentException.class,
                    () -> singleValueValidator.validate(
                        getJwsHeader(),
                        JWTClaimsSet.parse(Map.of(fallbackClaimName, invalidClaimValue, "something", "else"))
                    )
                );
            } else {
                e = expectThrows(
                    IllegalArgumentException.class,
                    () -> singleValueValidator.validate(
                        getJwsHeader(),
                        JWTClaimsSet.parse(Map.of(claimName, invalidClaimValue, "something", "else"))
                    )
                );
            }
            assertThat(e.getMessage(), containsString("does not match allowed claim values"));
        }
    }

    public void testInvalidPatternThrows() {
        String claimName = randomAlphaOfLength(4);
        SettingsException e = expectThrows(
            SettingsException.class,
            () -> new JwtStringClaimValidator(
                claimName,
                randomBoolean(),
                randomBoolean() ? null : Map.of(randomAlphaOfLength(4), randomAlphaOfLength(8)),
                randomBoolean() ? List.of() : List.of("dummy"),
                List.of("/invalid pattern")
            )
        );
        assertThat(e.getMessage(), containsString("Invalid patterns for allowed claim values for [" + claimName + "]."));
    }

    public void testAllowAllSubjects() {
        try {
            JwtStringClaimValidator.ALLOW_ALL_SUBJECTS.validate(
                getJwsHeader(),
                JWTClaimsSet.parse(Map.of("sub", randomAlphaOfLengthBetween(1, 10)))
            );
        } catch (Exception e) {
            throw new AssertionError("validation should have passed without exception", e);
        }

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> JwtStringClaimValidator.ALLOW_ALL_SUBJECTS.validate(getJwsHeader(), JWTClaimsSet.parse(Map.of()))
        );
        assertThat(e.getMessage(), containsString("missing required string claim"));
    }

    private JWSHeader getJwsHeader() throws ParseException {
        return JWSHeader.parse(Map.of("alg", randomAlphaOfLengthBetween(3, 8)));
    }
}

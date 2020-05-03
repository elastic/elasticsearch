/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.core.security.test.BcryptTestSupport.runWithInvalidRevisions;
import static org.elasticsearch.xpack.core.security.test.BcryptTestSupport.runWithValidRevisions;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class BCryptTests extends ESTestCase {

    private static final SecureString PASSWORD = new SecureString("U21VniQwdEWATfqO".toCharArray());
    private static final String[] VALID_HASHES = {
        "$2a$04$OLNTeJiq3vjYqTZwgDi62OU5MvzkV3Jqz.KiR3pwgQv70pD6bUsGa",
        "$2a$05$XNLcDk8PSYbU70A4bWjY1ugWlNSVM.VPMp6lb9qLotOB9oPV5TyM6",
        "$2a$06$KMO7CTXk.rzWPve.dRYXgu8x028/6QlBmRTCijvbwFH5Xx4Xhn4tW",
        "$2a$07$tr.C.OmBfdIBg7gcMruQX.UHZtmoZfi6xNpK6A0/oa.ulR4rXj6Ny",
        "$2a$08$Er.JIbUaPM7JmIN0iFEhW.H2hgtRT9weKtLdqEgSMAzmEe2xZ0B7a",
        "$2a$09$OmkfXJKIWhUmnrIlOy9Cd.SOu337FXAKcbB10nMUwpKSez5G4jz8e",
        "$2a$10$qyfYQcOK13wQmGO3Y.nVj.he5w1.Z0WV81HqBW6NlV.nkmg90utxO",
        "$2a$11$oNdrIn9.RBEg.XXnZkqwk..2wBrU6SjEJkQTLyxEXVQQcw4BokSaa",
        "$2a$12$WMLT/yjmMvBTgBnnZw1EhO6r4g7cWoxEOhS9ln4dNVg8gK3es/BZm",
        "$2a$13$WHkGwOCLz8SnX13tYH0Ez.qwKK0YFD8DA4Anz0a0Laozw75vqmBee",
        "$2a$14$8Urbk50As1LIgDBWPmXcFOpMWJfy3ddFLgvDlH3G1y4TFo4sLXU9y"
    };

    public void testVerifyHash() {
        for (String hash : VALID_HASHES) {
            testVerifyHashWithValidHash(hash);
        }

        final String[] INVALID_HASHES = {
            "$2a$04$OLNTeJiq3vjYqTZwgDi62OU5MvzkV3Jqz.KiR3pwgQv70pD6bUsGd",
            "$2a$05$XNLcDk8PSYbU70A4bWjY1ugWlNSVM.VPMp6lb9qLotOB9oPV5Tys6",
            "$2a$06$KMO7CTXk.rzWPve.dRYXgu8x028/6QlBmRTCijvbwFH5Xx4XhnttW",
            "$2a$07$tr.C.OmBfdIBg7gcMruQX.UHZtmoZfi6xNpK6A0/oa.ulR4rXh6Ny",
            "$2a$08$Er.JIbUaPM7JmIN0iFEhW.H2hgtRT9weKtLdqEgSMAzmEe2xi0B7a",
            "$2a$09$OmkfXJKIWhUmnrIlOy9Cd.SOu337FXAKcbB10nMUwpKSez5R4jz8e",
            "$2a$10$qyfYQcOK13wQmGO3Y.nVj.he5w1.Z0WV81HqBW6NlV.nkmL90utxO",
            "$2a$11$oNdrIn9.RBEg.XXnZkqwk..2wBrU6SjEJkQTLyxEXVQQc34BokSaa",
            "$2a$12$WMLT/yjmMvBTgBnnZw1EhO6r4g7cWoxEOhS9ln4dNVg8lK3es/BZm",
            "$2a$13$WHkGwOCLz8SnX13tYH0Ez.qwKK0YFD8DA4Anz0a0Lao3w75vqmBee",
            "$2a$14$8Urbk50As1LIgDBWPmXcFOpMWJfy3ddFLgvDlH3G1yPTFo4sLXU9y"
        };
        for (String hash : INVALID_HASHES) {
            testVerifyHashWithInvalidHash(hash);
        }
    }

    private void testVerifyHashWithValidHash(String baseHash) {
        runWithValidRevisions(baseHash, hash -> assertTrue(BCrypt.verifyHash(PASSWORD, hash)));
        runWithInvalidRevisions(baseHash, hash -> assertFalse(BCrypt.verifyHash(PASSWORD, hash)));
    }

    private void testVerifyHashWithInvalidHash(String baseHash) {
        runWithValidRevisions(baseHash, hash -> assertFalse(BCrypt.verifyHash(PASSWORD, hash)));
        runWithInvalidRevisions(baseHash, hash -> assertFalse(BCrypt.verifyHash(PASSWORD, hash)));
    }

    public void testIsPrefixValid() {
        testInvalidPrefixSeparators();
        testInvalidPrefixVersion();
        testInvalidPrefixRevisions();

        assertFalse(BCrypt.isPrefixValid(null));
    }

    private void testInvalidPrefixSeparators() {
        final String[] HASHES_WITH_INVALID_SEPARATORS = {
            "{2a$04$OLNTeJiq3vjYqTZwgDi62OU5MvzkV3Jqz.KiR3pwgQv70pD6bUsGa",
            "$2a#04$OLNTeJiq3vjYqTZwgDi62OU5MvzkV3Jqz.KiR3pwgQv70pD6bUsGa",
            "[2a]04$OLNTeJiq3vjYqTZwgDi62OU5MvzkV3Jqz.KiR3pwgQv70pD6bUsGa",
            ""
        };
        for (String hash : HASHES_WITH_INVALID_SEPARATORS) {
            assertFalse(BCrypt.isPrefixValid(hash.toCharArray()));
        }
    }

    private void testInvalidPrefixVersion() {
        final String[] HASHES_WITH_INVALID_VERSION = {
            "$3a$04$OLNTeJiq3vjYqTZwgDi62OU5MvzkV3Jqz.KiR3pwgQv70pD6bUsGa",
            "$"
        };
        for (String hash : HASHES_WITH_INVALID_VERSION) {
            assertFalse(BCrypt.isPrefixValid(hash.toCharArray()));
        }
    }

    private void testInvalidPrefixRevisions() {
        final String baseHash = VALID_HASHES[0];
        runWithValidRevisions(baseHash, hash -> assertTrue(BCrypt.isPrefixValid(hash)));
        runWithInvalidRevisions(baseHash, hash -> assertFalse(BCrypt.isPrefixValid(hash)));
    }

    public void testGetLogRounds() {
        final int MIN_ROUNDS = 4;
        final int MAX_ROUNDS = 14;
        for (int rounds = MIN_ROUNDS, index = 0; rounds <= MAX_ROUNDS; rounds++, index++) {
            assertThat(BCrypt.getLogRounds(VALID_HASHES[index].toCharArray()), equalTo(rounds));
        }

        final String HASH_WITH_TRUNCATED_ROUNDS = "$2a$1";
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> BCrypt.getLogRounds(HASH_WITH_TRUNCATED_ROUNDS.toCharArray()));
        assertThat(e.getMessage(), containsString("Invalid hash length."));

        final String HASH_WITH_INVALID_ROUNDS = "$2a$w4$OLNTeJiq3vjYqTZwgDi62OU5MvzkV3Jqz.KiR3pwgQv70pD6bUsGa";
        e = expectThrows(IllegalArgumentException.class,
            () -> BCrypt.getLogRounds(HASH_WITH_INVALID_ROUNDS.toCharArray()));
        assertThat(e.getMessage(), containsString("Hash log rounds should be a two digit integer."));
    }

    public void testGenerateHash() {
        for (String hash : VALID_HASHES) {
            assertThat(BCrypt.generateHash(PASSWORD, hash), equalTo(hash));
        }
    }
}

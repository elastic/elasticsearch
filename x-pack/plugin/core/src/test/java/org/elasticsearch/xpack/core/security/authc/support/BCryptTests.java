/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;

import java.util.function.Consumer;

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
        "$2a$14$8Urbk50As1LIgDBWPmXcFOpMWJfy3ddFLgvDlH3G1y4TFo4sLXU9y" };

    public void testVerifyHash() {
        for (String hash : VALID_HASHES) {
            runWithValidRevisions(hash, h -> assertTrue("Hash " + h, BCrypt.checkpw(PASSWORD, h)));
            runWithInvalidRevisions(hash, h -> expectThrows(IllegalArgumentException.class, () -> BCrypt.checkpw(PASSWORD, h)));

            // Replace a random character in the hash
            int index = randomIntBetween(10, hash.length() - 1);
            String replace = randomValueOtherThan(hash.substring(index, index + 1), () -> randomAlphaOfLength(1));
            String invalid = hash.substring(0, index) + replace + hash.substring(index + 1);
            assertThat(invalid.length(), equalTo(hash.length()));
            runWithValidRevisions(invalid, h -> assertFalse("Hash " + h, BCrypt.checkpw(PASSWORD, h)));
        }
    }

    static void runWithValidRevisions(String baseHash, Consumer<String> action) {
        for (String revision : new String[] { "$2a$", "$2b$", "$2y$" }) {
            action.accept(revision + baseHash.substring(4));
        }
    }

    static void runWithInvalidRevisions(String baseHash, Consumer<String> action) {
        for (String revision : new String[] { "$2c$", "$2x$", "$2z$" }) {
            action.accept(revision + baseHash.substring(4));
        }
    }

}

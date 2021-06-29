/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.saml;

import static org.elasticsearch.test.TestMatchers.matchesPattern;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class SamlUtilsTests extends SamlTestCase {

    public void testGenerateSecureNCName() {
        int previousLength = 0;
        for (int bytes = randomIntBetween(1, 10); bytes <= 30; bytes += 5) {
            final String name = SamlUtils.generateSecureNCName(bytes);
            // See: http://www.datypic.com/sc/xsd/t-xsd_NCName.html
            assertThat(name, matchesPattern("^[a-zA-Z_][a-zA-Z0-9_.-]*$"));
            assertThat(name.length(), greaterThanOrEqualTo(bytes));
            assertThat(name.length(), greaterThan(previousLength));
            previousLength = name.length();
        }
    }

}

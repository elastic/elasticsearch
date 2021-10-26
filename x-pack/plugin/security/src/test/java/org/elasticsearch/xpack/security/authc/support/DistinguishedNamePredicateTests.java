/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.support;

import com.unboundid.ldap.sdk.DN;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression.FieldValue;

import java.util.Locale;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;

public class DistinguishedNamePredicateTests extends ESTestCase {

    public void testMatching() throws Exception {
        String randomDn = "CN=" + randomAlphaOfLengthBetween(3, 12)
                + ",OU=" + randomAlphaOfLength(4)
                + ", O=" + randomAlphaOfLengthBetween(2, 6);

        // Randomly enter the DN in mixed case, lower case or upper case;
        final String inputDn;
        if (randomBoolean()) {
            inputDn = randomBoolean() ? randomDn.toLowerCase(Locale.ENGLISH) : randomDn.toUpperCase(Locale.ENGLISH);
        } else {
            inputDn = randomDn;
        }
        final Predicate<FieldValue> predicate = new UserRoleMapper.DistinguishedNamePredicate(inputDn);

        assertPredicate(predicate, randomDn, true);
        assertPredicate(predicate, randomDn.toLowerCase(Locale.ROOT), true);
        assertPredicate(predicate, randomDn.toUpperCase(Locale.ROOT), true);
        assertPredicate(predicate, "/" + inputDn + "/", true);
        assertPredicate(predicate, new DN(randomDn).toNormalizedString(), true);
        assertPredicate(predicate, "*," + new DN(randomDn).getParent().toNormalizedString(), true);
        assertPredicate(predicate, "*," + new DN(inputDn).getParent().getParent().toNormalizedString(), true);
        assertPredicate(predicate, randomDn.replaceFirst(".*,", "*,"), true);
        assertPredicate(predicate, randomDn.replaceFirst("[^,]*,", "*, "), true);

        assertPredicate(predicate, randomDn + ",CN=AU", false);
        assertPredicate(predicate, "X" + randomDn, false);
        assertPredicate(predicate, "", false);
        assertPredicate(predicate, 1.23, false);
        assertPredicate(predicate, true, false);
        assertPredicate(predicate, null, false);
    }

    public void testParsingMalformedInput() {
        Predicate<FieldValue> predicate = new UserRoleMapper.DistinguishedNamePredicate("");
        assertPredicate(predicate, null, false);
        assertPredicate(predicate, "", true);
        assertPredicate(predicate, randomAlphaOfLengthBetween(1, 8), false);
        assertPredicate(predicate, randomAlphaOfLengthBetween(1, 8) + "*", false);

        predicate = new UserRoleMapper.DistinguishedNamePredicate("foo=");
        assertPredicate(predicate, null, false);
        assertPredicate(predicate, "foo", false);
        assertPredicate(predicate, "foo=", true);
        assertPredicate(predicate, randomAlphaOfLengthBetween(5, 12), false);
        assertPredicate(predicate, randomAlphaOfLengthBetween(5, 12) + "*", false);

        predicate = new UserRoleMapper.DistinguishedNamePredicate("=bar");
        assertPredicate(predicate, null, false);
        assertPredicate(predicate, "bar", false);
        assertPredicate(predicate, "=bar", true);
        assertPredicate(predicate, randomAlphaOfLengthBetween(5, 12), false);
        assertPredicate(predicate, randomAlphaOfLengthBetween(5, 12) + "*", false);
    }

    private void assertPredicate(Predicate<FieldValue> predicate, Object value, boolean expected) {
        assertThat("Predicate [" + predicate + "] match [" + value + "]", predicate.test(new FieldValue(value)), equalTo(expected));
    }
}

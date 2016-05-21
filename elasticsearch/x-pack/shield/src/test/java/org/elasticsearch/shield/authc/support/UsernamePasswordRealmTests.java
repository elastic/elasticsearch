/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

import static org.hamcrest.core.Is.is;

/**
 *
 */
public class UsernamePasswordRealmTests extends ESTestCase {

    public void testUserbaseScaelResolve() throws Exception {
        int count = randomIntBetween(0, 1000);
        UsernamePasswordRealm.UserbaseSize size = UsernamePasswordRealm.UserbaseSize.resolve(count);
        if (count < 10) {
            assertThat(size, is(UsernamePasswordRealm.UserbaseSize.SMALL));
        } else if (count < 100) {
            assertThat(size, is(UsernamePasswordRealm.UserbaseSize.SMALL));
        } else if (count < 500) {
            assertThat(size, is(UsernamePasswordRealm.UserbaseSize.MEDIUM));
        } else if (count < 1000) {
            assertThat(size, is(UsernamePasswordRealm.UserbaseSize.LARGE));
        } else {
            assertThat(size, is(UsernamePasswordRealm.UserbaseSize.XLARGE));
        }
    }

    public void testUserbaseScaleToString() throws Exception {
        UsernamePasswordRealm.UserbaseSize size = randomFrom(UsernamePasswordRealm.UserbaseSize.values());
        String value = size.toString();
        if (size == UsernamePasswordRealm.UserbaseSize.XLARGE) {
            assertThat(value , is("x-large"));
        } else {
            assertThat(value , is(size.name().toLowerCase(Locale.ROOT)));
        }
    }
}

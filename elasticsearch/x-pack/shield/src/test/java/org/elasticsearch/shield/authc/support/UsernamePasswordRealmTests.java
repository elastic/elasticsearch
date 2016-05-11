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
        UsernamePasswordRealm.UserbaseScale scale = UsernamePasswordRealm.UserbaseScale.resolve(count);
        if (count < 10) {
            assertThat(scale, is(UsernamePasswordRealm.UserbaseScale.SMALL));
        } else if (count < 50) {
            assertThat(scale, is(UsernamePasswordRealm.UserbaseScale.MEDIUM));
        } else if (count < 250) {
            assertThat(scale, is(UsernamePasswordRealm.UserbaseScale.LARGE));
        } else {
            assertThat(scale, is(UsernamePasswordRealm.UserbaseScale.XLARGE));
        }
    }

    public void testUserbaseScaleToString() throws Exception {
        UsernamePasswordRealm.UserbaseScale scale = randomFrom(UsernamePasswordRealm.UserbaseScale.values());
        String value = scale.toString();
        if (scale == UsernamePasswordRealm.UserbaseScale.XLARGE) {
            assertThat(value , is("x-large"));
        } else {
            assertThat(value , is(scale.name().toLowerCase(Locale.ROOT)));
        }
    }
}

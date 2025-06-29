/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.security.Provider;
import java.security.Security;

import static org.hamcrest.Matchers.notNullValue;

public class Md5UnavailableTests extends ESTestCase {

    public void testMd5Unavailable() {
        assumeFalse("We run with different security providers in FIPS, and changing them at runtime is more complicated", inFipsJvm());
        final Provider sunProvider = Security.getProvider("SUN");
        final Md5 md5;
        try {
            Security.removeProvider("SUN");
            md5 = new Md5(Source.EMPTY, Literal.NULL);
            expectThrows(VerificationException.class, md5::getHashFunction);
        } finally {
            Security.addProvider(sunProvider);
        }

        assertThat(md5.getHashFunction(), notNullValue());
    }

}

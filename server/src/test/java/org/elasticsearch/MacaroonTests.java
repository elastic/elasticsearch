/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import com.github.nitram509.jmacaroons.Macaroon;
import com.github.nitram509.jmacaroons.MacaroonsBuilder;

import com.github.nitram509.jmacaroons.MacaroonsVerifier;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class MacaroonTests extends ESTestCase {
    public void testMacaroons() {
        String location = "http://www.example.org";
        String secretKey = "potato";
        String identifier = "tomato";
        Macaroon macaroon = MacaroonsBuilder.create(location, secretKey, identifier);
        MacaroonsVerifier verifier = new MacaroonsVerifier(macaroon);
        assertThat(verifier.isValid(secretKey), is(true));
    }
}

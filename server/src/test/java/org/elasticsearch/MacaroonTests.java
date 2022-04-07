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

    public void testAddCaveatFromSerialized() {
        String caveat = """
            {
               "match": {
                    "message": "potato"
                }
            }""";
        Macaroon m = MacaroonsBuilder.modify(
            MacaroonsBuilder.deserialize(
                "MDAwZWxvY2F0aW9uIAowMDI5aWRlbnRpZmllciA0M3VwdTJ3Nm83bno4Mm1ocnhtdzRxbjRoCjAwYzBjaWQgcGl0X2lkID0gW2s0X3FBd0VQYlhrdGFXNWtaWGd0TURBd01EQXhGblpsVlhoeWFrYzVVa0pQY2poaFgyTjFOVmxpVUdjQUZqVTRXa1ZEVDBKaFUyOHlZVTFtVWtSUk9YRlVNbEVBQUFBQUFBQUFBQ3dXZW5OSlZUQlBOa2hVTjA5U1Rrb3RiMmM1UmtoblVRQUJGblpsVlhoeWFrYzVVa0pQY2poaFgyTjFOVmxpVUdjQUFBPT1dCjAwMmZzaWduYXR1cmUgkYjISdm79VWZVXPNvNLUxIGqYhZ-miqGi--IZ-p_jFsK"
            )
        ).add_first_party_caveat(caveat).getMacaroon();
        System.out.println(m.serialize());
    }

    public void testAddCaveat() {
        String caveat = """
            {
              "match" : {
                "message" : {
                  "query" : "potato"
                }
              }
            }""";
        Macaroon m = new MacaroonsBuilder("", "key", "123").add_first_party_caveat(caveat).getMacaroon();
        System.out.println(m.serialize());
    }
}

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
                "MDAwZWxvY2F0aW9uIAowMDI5aWRlbnRpZmllciA5NHV0d2F1ZWQ2NGs1czk0Z2VveDdpcnNtCjAxNjBjaWQgcGl0X2lkID0gW2s0X3FBd0lQYlhrdGFXNWtaWGd0TURBd01EQXhGblpMWlV4ZmNsWkhVelpUVEdSVVIxUnZTekpIV21jQUZrVmxXa0pPYURnMlZGVnBkR2xhVURRMU56WjRUa0VBQUFBQUFBQUFBQ3dXUm1reVdqaGhhRWhTWW5WVU4wSnNVVGhvZEZSS2R3QVBiWGt0YVc1a1pYZ3RNREF3TURBeUZtTlNhVVF3TVd3M1ZHdEhUV3R5YW0xSldGVmtZMEVBRmtWbFdrSk9hRGcyVkZWcGRHbGFVRFExTnpaNFRrRUFBQUFBQUFBQUFDMFdSbWt5V2poaGFFaFNZblZVTjBKc1VUaG9kRlJLZHdBQ0ZuWkxaVXhmY2xaSFV6WlRUR1JVUjFSdlN6SkhXbWNBQUJaalVtbEVNREZzTjFSclIwMXJjbXB0U1ZoVlpHTkJBQUE9XQowMDJmc2lnbmF0dXJlIOs6SOXwCg8tMVc-kLyoGyQ7Ou1HAiDskePZcrqgoPyVCg"
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

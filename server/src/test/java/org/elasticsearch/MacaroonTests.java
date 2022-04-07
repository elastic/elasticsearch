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
               "match" : {
                 "bar" : {
                   "query" : "foo"
                 }
               }
            }""";
        Macaroon m = MacaroonsBuilder.modify(
            MacaroonsBuilder.deserialize(
                "MDAwZWxvY2F0aW9uIAowMDI5aWRlbnRpZmllciBjZmJiZ25kdGFlYmtwMGptcGt1aHUxd3JtCjAxNjBjaWQgcGl0X2lkID0gW2s0X3FBd0lQYlhrdGFXNWtaWGd0TURBd01EQXlGbGxETVdZMVdrMXRWRUZQUzI5T2RtMVpNVGRVT0ZFQUZrbFZibEJaWjI1WlUxcFhOVVl0WlVWRlZtWk5jR2NBQUFBQUFBQUFBQzBXYVdOWVQwNVJjMWhVTUVOSE5VODJjemh0Y21ST1VRQVBiWGt0YVc1a1pYZ3RNREF3TURBeEZtTnNUbVJXV0Y5alUyOXhibU42WW01WFN6ZFhhSGNBRmtsVmJsQlpaMjVaVTFwWE5VWXRaVVZGVm1aTmNHY0FBQUFBQUFBQUFDd1dhV05ZVDA1UmMxaFVNRU5ITlU4MmN6aHRjbVJPVVFBQ0ZsbERNV1kxV2sxdFZFRlBTMjlPZG0xWk1UZFVPRkVBQUJaamJFNWtWbGhmWTFOdmNXNWplbUp1VjBzM1YyaDNBQUE9XQowMDJmc2lnbmF0dXJlIEmljszeq6rm-Y-QscPUeTA4QWBKhfYyA8RnIdWMD7akCg"
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

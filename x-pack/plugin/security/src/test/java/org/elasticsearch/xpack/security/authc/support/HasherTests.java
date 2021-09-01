/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class HasherTests extends ESTestCase {
    public void testBcryptFamilySelfGenerated() throws Exception {
        testHasherSelfGenerated(Hasher.BCRYPT);
        testHasherSelfGenerated(Hasher.BCRYPT4);
        testHasherSelfGenerated(Hasher.BCRYPT5);
        testHasherSelfGenerated(Hasher.BCRYPT6);
        testHasherSelfGenerated(Hasher.BCRYPT7);
        testHasherSelfGenerated(Hasher.BCRYPT8);
        testHasherSelfGenerated(Hasher.BCRYPT9);
        testHasherSelfGenerated(Hasher.BCRYPT10);
        testHasherSelfGenerated(Hasher.BCRYPT11);
        testHasherSelfGenerated(Hasher.BCRYPT12);
        testHasherSelfGenerated(Hasher.BCRYPT13);
        testHasherSelfGenerated(Hasher.BCRYPT14);
    }

    public void testBcryptFromExternalSources() throws Exception {
        check("$2b$12$0313KXrdhWp6HLREsKxW/OWJQCxy2uYprv44b8MBk6dOj3PY6WSFG", "my-password", true);
        check("$2b$12$0313KXrdhWp6HLREsKxW/OWJQCxy2uYprv44b8MBk6dOj3PY6WSFG", "not-the-password", false);

        check("$2b$12$4bf6s1NIUhyA5FtLn1UrpuZTByjNCC7f0r5OFJP9ra8U2LtcpcK7C", "changeme", true);
        check("$2b$12$4bf6s1NIUhyA5FtLn1UrpuZTByjNCC7f0r5OFJP9ra8U2LtcpcK7C", "changed-it", false);

        check("$2b$09$OLrfKSXQJxohtFnkU.VZCO1gKywaTSFi4KPHqhuyY3qetAbI8v6/S", "NjJmOWRmMjJmODEyZmQ1NjFhNWVmZmIwOWMwNjk4MmMK", true);
        check("$2b$09$OLrfKSXQJxohtFnkU.VZCO1gKywaTSFi4KPHqhuyY3qetAbI8v6/S", "nJjMowrMmJjModeYzMq1nJfHnwvMzMiWowmWnJK4mMmk", false);

        check("$2b$14$azbTD0EotrQtoSsxFpbx/.HG8hCAojDFmN4hsD8khuevk/9j0yPlK", "python 3.9.6; bcrypt 3.2.0", true);
        check("$2a$06$aQVRp5ajsIn3fzx2MnXKy.KlhxFLHaCOh8jSElqCtmYFbRkmTy..C", "https://bcrypt-generator.com/", true);
        check("$2y$10$flKBxak./o.7Hql0il/98ejdZyob67TmPhHbRy3qOnMtCBosAVSRy", "php", true);
    }

    public void testPBKDF2FamilySelfGenerated() throws Exception {
        testHasherSelfGenerated(Hasher.PBKDF2);
        testHasherSelfGenerated(Hasher.PBKDF2_1000);
        testHasherSelfGenerated(Hasher.PBKDF2_10000);
        testHasherSelfGenerated(Hasher.PBKDF2_50000);
        testHasherSelfGenerated(Hasher.PBKDF2_100000);
        testHasherSelfGenerated(Hasher.PBKDF2_500000);
        testHasherSelfGenerated(Hasher.PBKDF2_1000000);
        testHasherSelfGenerated(Hasher.PBKDF2_STRETCH);
        testHasherSelfGenerated(Hasher.PBKDF2_STRETCH_1000);
        testHasherSelfGenerated(Hasher.PBKDF2_STRETCH_10000);
        testHasherSelfGenerated(Hasher.PBKDF2_STRETCH_50000);
        testHasherSelfGenerated(Hasher.PBKDF2_STRETCH_100000);
        testHasherSelfGenerated(Hasher.PBKDF2_STRETCH_500000);
        testHasherSelfGenerated(Hasher.PBKDF2_STRETCH_1000000);
    }

    public void testMd5SelfGenerated() throws Exception {
        testHasherSelfGenerated(Hasher.MD5);
    }

    public void testSha1SelfGenerated() throws Exception {
        testHasherSelfGenerated(Hasher.SHA1);
    }

    public void testSSHA256SelfGenerated() throws Exception {
        testHasherSelfGenerated(Hasher.SSHA256);
    }

    public void testSHA256SelfGenerated() throws Exception {
        testHasherSelfGenerated(Hasher.SHA256);
    }

    public void testNoopSelfGenerated() throws Exception {
        testHasherSelfGenerated(Hasher.NOOP);
    }

    public void testResolve() {
        assertThat(Hasher.resolve("bcrypt"), sameInstance(Hasher.BCRYPT));
        assertThat(Hasher.resolve("bcrypt4"), sameInstance(Hasher.BCRYPT4));
        assertThat(Hasher.resolve("bcrypt5"), sameInstance(Hasher.BCRYPT5));
        assertThat(Hasher.resolve("bcrypt6"), sameInstance(Hasher.BCRYPT6));
        assertThat(Hasher.resolve("bcrypt7"), sameInstance(Hasher.BCRYPT7));
        assertThat(Hasher.resolve("bcrypt8"), sameInstance(Hasher.BCRYPT8));
        assertThat(Hasher.resolve("bcrypt9"), sameInstance(Hasher.BCRYPT9));
        assertThat(Hasher.resolve("bcrypt10"), sameInstance(Hasher.BCRYPT));
        assertThat(Hasher.resolve("bcrypt11"), sameInstance(Hasher.BCRYPT11));
        assertThat(Hasher.resolve("bcrypt12"), sameInstance(Hasher.BCRYPT12));
        assertThat(Hasher.resolve("bcrypt13"), sameInstance(Hasher.BCRYPT13));
        assertThat(Hasher.resolve("bcrypt14"), sameInstance(Hasher.BCRYPT14));
        assertThat(Hasher.resolve("pbkdf2"), sameInstance(Hasher.PBKDF2));
        assertThat(Hasher.resolve("pbkdf2_1000"), sameInstance(Hasher.PBKDF2_1000));
        assertThat(Hasher.resolve("pbkdf2_10000"), sameInstance(Hasher.PBKDF2));
        assertThat(Hasher.resolve("pbkdf2_50000"), sameInstance(Hasher.PBKDF2_50000));
        assertThat(Hasher.resolve("pbkdf2_100000"), sameInstance(Hasher.PBKDF2_100000));
        assertThat(Hasher.resolve("pbkdf2_500000"), sameInstance(Hasher.PBKDF2_500000));
        assertThat(Hasher.resolve("pbkdf2_1000000"), sameInstance(Hasher.PBKDF2_1000000));
        assertThat(Hasher.resolve("pbkdf2_stretch"), sameInstance(Hasher.PBKDF2_STRETCH));
        assertThat(Hasher.resolve("pbkdf2_stretch_1000"), sameInstance(Hasher.PBKDF2_STRETCH_1000));
        assertThat(Hasher.resolve("pbkdf2_stretch_10000"), sameInstance(Hasher.PBKDF2_STRETCH_10000));
        assertThat(Hasher.resolve("pbkdf2_stretch_50000"), sameInstance(Hasher.PBKDF2_STRETCH_50000));
        assertThat(Hasher.resolve("pbkdf2_stretch_100000"), sameInstance(Hasher.PBKDF2_STRETCH_100000));
        assertThat(Hasher.resolve("pbkdf2_stretch_500000"), sameInstance(Hasher.PBKDF2_STRETCH_500000));
        assertThat(Hasher.resolve("pbkdf2_stretch_1000000"), sameInstance(Hasher.PBKDF2_STRETCH_1000000));
        assertThat(Hasher.resolve("sha1"), sameInstance(Hasher.SHA1));
        assertThat(Hasher.resolve("md5"), sameInstance(Hasher.MD5));
        assertThat(Hasher.resolve("ssha256"), sameInstance(Hasher.SSHA256));
        assertThat(Hasher.resolve("noop"), sameInstance(Hasher.NOOP));
        assertThat(Hasher.resolve("clear_text"), sameInstance(Hasher.NOOP));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            Hasher.resolve("unknown_hasher");
        });
        assertThat(e.getMessage(), containsString("unknown hash function "));
    }

    public void testResolveFromHash() {
        assertThat(Hasher.resolveFromHash("$2a$10$1oZj.8KmlwiCy4DWKvDH3OU0Ko4WRF4FknyvCh3j/ZtaRCNYA6Xzm".toCharArray()),
            sameInstance(Hasher.BCRYPT));
        assertThat(Hasher.resolveFromHash("$2a$04$GwJtIQiGMHASEYphMiCpjeZh1cDyYC5U.DKfNKa4i/y0IbOvc2LiG".toCharArray()),
            sameInstance(Hasher.BCRYPT4));
        assertThat(Hasher.resolveFromHash("$2a$05$xLmwSB7Nw7PcqP.6hXdc4eUZbT.4.iAZ3CTPzSaUibrrYjC6Vwq1m".toCharArray()),
            sameInstance(Hasher.BCRYPT5));
        assertThat(Hasher.resolveFromHash("$2a$06$WQX1MALAjVOhR2YKmLcHYed2oROzBl3OZPtvq3FkVZYwm9X2LVKYm".toCharArray()),
            sameInstance(Hasher.BCRYPT6));
        assertThat(Hasher.resolveFromHash("$2a$07$Satxnu2fCvwYXpHIk8A2sO2uwROrsV7WrNiRJPq1oXEl5lc9FE.7S".toCharArray()),
            sameInstance(Hasher.BCRYPT7));
        assertThat(Hasher.resolveFromHash("$2a$08$LLfkTt2C9TUl5sDtgqmE3uRw9nHt748d3eMSGfbFYgQQQhjbXHFo2".toCharArray()),
            sameInstance(Hasher.BCRYPT8));
        assertThat(Hasher.resolveFromHash("$2a$09$.VCWA3yFVdd6gfI526TUrufb4TvxMuhW0jIuMfhd4/fy1Ak/zrSFe".toCharArray()),
            sameInstance(Hasher.BCRYPT9));
        assertThat(Hasher.resolveFromHash("$2a$10$OEiXFrUUY02Nm7YsEgzFuuJ3yO3HAYzJUU7omseluy28s7FYaictu".toCharArray()),
            sameInstance(Hasher.BCRYPT));
        assertThat(Hasher.resolveFromHash("$2a$11$Ya53LCozFlKABu05xsAbj.9xmrczyuAY/fTvxKkDiHOJc5GYcaNRy".toCharArray()),
            sameInstance(Hasher.BCRYPT11));
        assertThat(Hasher.resolveFromHash("$2a$12$oUW2hiWBHYwbJamWi6YDPeKS2NBCvD4GR50zh9QZCcgssNFcbpg/a".toCharArray()),
            sameInstance(Hasher.BCRYPT12));
        assertThat(Hasher.resolveFromHash("$2a$13$0PDx6mxKK4bLSgpc5H6eaeylWub7UFghjxV03lFYSz4WS4slDT30q".toCharArray()),
            sameInstance(Hasher.BCRYPT13));
        assertThat(Hasher.resolveFromHash("$2a$14$lFyXmX7p9/FHr7W4nxTnfuCkjAoBHv6awQlv8jlKZ/YCMI65i38e6".toCharArray()),
            sameInstance(Hasher.BCRYPT14));
        assertThat(Hasher.resolveFromHash(
            "{PBKDF2}1000$oNl3JWiDZhXqhrpk9Kl+T0tKpVNNV3UHNxENPePpo2M=$g9lERDX5op20eX534bHdQy7ySRwobxwtaxxsz3AYPIU=".toCharArray()),
            sameInstance(Hasher.PBKDF2_1000));
        assertThat(Hasher.resolveFromHash(
            "{PBKDF2}10000$UrwrHBY4GA1na9KxRpoFkUiICTeZe+mMZCZOg6bRSLc=$1Wl32wRQ9Q3Sv1IFoNwgSrUa5YifLv0MoxAO6leyip8=".toCharArray()),
            sameInstance(Hasher.PBKDF2));
        assertThat(Hasher.resolveFromHash(
            "{PBKDF2}50000$mxa5m9AlgtKLUXKi/pE5+4w7ZexGSOtlUHD043NHVdc=$LE5Ncph672M8PtugfRgk2k3ue9qY2cKgiguuAd+e3I0=".toCharArray()),
            sameInstance(Hasher.PBKDF2_50000));
        assertThat(Hasher.resolveFromHash(
            "{PBKDF2}100000$qFs8H0FjietnI7sgr/1Av4H+Z7d/9dehfZ2ptU474jk=$OFj40Ha0XcHWUXSspRx6EeXnTcuN0Nva2/i2c/hvnZE=".toCharArray()),
            sameInstance(Hasher.PBKDF2_100000));
        assertThat(Hasher.resolveFromHash(
            "{PBKDF2}500000$wyttuDlppd5KYD35uDZN6vudB50Cjshm5efZhOxZZQI=$ADZpOHY6llJZsjupZCn6s4Eocg0dKKdBiNjDBYqhlzA=".toCharArray()),
            sameInstance(Hasher.PBKDF2_500000));
        assertThat(Hasher.resolveFromHash(
            "{PBKDF2}1000000$UuyhtjDEzWmE2wyY80akZKPWWpy2r2X50so41YML82U=$WFasYLelqbjQwt3EqFlUcwHiC38EZC45Iu/Iz0xL1GQ=".toCharArray()),
            sameInstance(Hasher.PBKDF2_1000000));
        assertThat(Hasher.resolveFromHash(
            "{PBKDF2_STRETCH}1000$sTyix9e0zNINzq2aDZ+GD5+QlO94xVyf/bv4pWNhBxo=$4KuzGPy9HXnhY3ANHn8rcIRQuJHPB6cEtLwnOhDI5d4="
                .toCharArray()),
            sameInstance(Hasher.PBKDF2_STRETCH_1000));
        assertThat(Hasher.resolveFromHash(
            "{PBKDF2_STRETCH}10000$8M9+Ww0xkdY250CROEutsd8UP6CrJESw7ZAFu1NGORo=$ai0gxBPtHTfZU/nbNGwL5zjC+eo2/ANQM17L/tllVeo="
                .toCharArray()),
            sameInstance(Hasher.PBKDF2_STRETCH));
        assertThat(Hasher.resolveFromHash(
            "{PBKDF2_STRETCH}50000$uupwXiq8W0+jrLtC3/aqzuvyZlRarlmx1+CQGEnomlk=$by8q/+oRPPWwDE6an7B9/ndz7UZ1UQpaGY4CGurtPTI="
                .toCharArray()),
            sameInstance(Hasher.PBKDF2_STRETCH_50000));
        assertThat(Hasher.resolveFromHash(
            "{PBKDF2_STRETCH}100000$E9VqtV76PcrQuCZ6wOMMNvs4CMPcANTpzRw8Wjd24PU=$j56uKUvwbvmgQgNFkbV7SRQVZ2QOarokAgBeA8xcFD8="
                .toCharArray()),
            sameInstance(Hasher.PBKDF2_STRETCH_100000));
        assertThat(Hasher.resolveFromHash(
            "{PBKDF2_STRETCH}500000$4dpTEbu4jfjhDOjWY6xdsnxuQs4dg4QbNzZJ0Z1Tm4s=$Us/yrlCxVaW7mz0go1qIygFqGgcfUMgCZfIl2AvI4I8="
                .toCharArray()),
            sameInstance(Hasher.PBKDF2_STRETCH_500000));
        assertThat(Hasher.resolveFromHash(
            "{PBKDF2_STRETCH}1000000$eKeQvMztiIcqBynTNDFBseOBww3GBpHDZI6EPPVHYUw=$4587yrxUa02RZ1jeW1WOaMjRn5qT9iQ5/DIHk0nW2bE="
                .toCharArray()),
            sameInstance(Hasher.PBKDF2_STRETCH_1000000));
        assertThat(Hasher.resolveFromHash("notavalidhashformat".toCharArray()), sameInstance(Hasher.NOOP));
    }

    public void testPbkdf2WithShortPasswordThrowsInFips() {
        assumeTrue("This should run only in FIPS mode", inFipsJvm());
        SecureString passwd = new SecureString(randomAlphaOfLength(between(6, 13)).toCharArray());
        Hasher pbkdfHasher = randomFrom(Hasher.PBKDF2, Hasher.PBKDF2_50000, Hasher.PBKDF2_1000000);
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> pbkdfHasher.hash(passwd));
        assertThat(e.getMessage(), containsString("Error using PBKDF2 implementation from the selected Security Provider"));
    }

    private static void testHasherSelfGenerated(Hasher hasher) {
        // In FIPS 140 mode, passwords for PBKDF2 need to be at least 14 chars
        SecureString passwd = new SecureString(randomAlphaOfLength(between(14, 18)).toCharArray());
        char[] hash = hasher.hash(passwd);
        assertTrue(hasher.verify(passwd, hash));

        SecureString incorrectPasswd = randomValueOtherThan(
            passwd,
            () -> new SecureString(randomAlphaOfLength(between(14, 18)).toCharArray())
        );
        assertFalse(hasher.verify(incorrectPasswd, hash));
    }

    private void check(String hash, String password, boolean shouldMatch) {
        char[] hashChars = hash.toCharArray();
        Hasher hasher = Hasher.resolveFromHash(hashChars);
        assertThat(
            "Verify " + password + " against " + hash + " using " + hasher.name(),
            hasher.verify(new SecureString(password.toCharArray()), hashChars),
            equalTo(shouldMatch)
        );
    }
}

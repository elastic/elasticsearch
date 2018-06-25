/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;

import static org.hamcrest.Matchers.containsString;
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

    public void testPBKDF2FamilySelfGenerated() throws Exception {
        testHasherSelfGenerated(Hasher.PBKDF2);
        testHasherSelfGenerated(Hasher.PBKDF2_1000);
        testHasherSelfGenerated(Hasher.PBKDF2_10000);
        testHasherSelfGenerated(Hasher.PBKDF2_50000);
        testHasherSelfGenerated(Hasher.PBKDF2_100000);
        testHasherSelfGenerated(Hasher.PBKDF2_500000);
        testHasherSelfGenerated(Hasher.PBKDF2_1000000);
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
        assertThat(Hasher.resolve("bcrypt10"), sameInstance(Hasher.BCRYPT10));
        assertThat(Hasher.resolve("bcrypt11"), sameInstance(Hasher.BCRYPT11));
        assertThat(Hasher.resolve("bcrypt12"), sameInstance(Hasher.BCRYPT12));
        assertThat(Hasher.resolve("bcrypt13"), sameInstance(Hasher.BCRYPT13));
        assertThat(Hasher.resolve("bcrypt14"), sameInstance(Hasher.BCRYPT14));
        assertThat(Hasher.resolve("pbkdf2"), sameInstance(Hasher.PBKDF2));
        assertThat(Hasher.resolve("pbkdf2_1000"), sameInstance(Hasher.PBKDF2_1000));
        assertThat(Hasher.resolve("pbkdf2_10000"), sameInstance(Hasher.PBKDF2_10000));
        assertThat(Hasher.resolve("pbkdf2_50000"), sameInstance(Hasher.PBKDF2_50000));
        assertThat(Hasher.resolve("pbkdf2_100000"), sameInstance(Hasher.PBKDF2_100000));
        assertThat(Hasher.resolve("pbkdf2_500000"), sameInstance(Hasher.PBKDF2_500000));
        assertThat(Hasher.resolve("pbkdf2_1000000"), sameInstance(Hasher.PBKDF2_1000000));
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
            sameInstance(Hasher.BCRYPT));
        assertThat(Hasher.resolveFromHash("$2a$05$xLmwSB7Nw7PcqP.6hXdc4eUZbT.4.iAZ3CTPzSaUibrrYjC6Vwq1m".toCharArray()),
            sameInstance(Hasher.BCRYPT));
        assertThat(Hasher.resolveFromHash("$2a$06$WQX1MALAjVOhR2YKmLcHYed2oROzBl3OZPtvq3FkVZYwm9X2LVKYm".toCharArray()),
            sameInstance(Hasher.BCRYPT));
        assertThat(Hasher.resolveFromHash("$2a$07$Satxnu2fCvwYXpHIk8A2sO2uwROrsV7WrNiRJPq1oXEl5lc9FE.7S".toCharArray()),
            sameInstance(Hasher.BCRYPT));
        assertThat(Hasher.resolveFromHash("$2a$08$LLfkTt2C9TUl5sDtgqmE3uRw9nHt748d3eMSGfbFYgQQQhjbXHFo2".toCharArray()),
            sameInstance(Hasher.BCRYPT));
        assertThat(Hasher.resolveFromHash("$2a$09$.VCWA3yFVdd6gfI526TUrufb4TvxMuhW0jIuMfhd4/fy1Ak/zrSFe".toCharArray()),
            sameInstance(Hasher.BCRYPT));
        assertThat(Hasher.resolveFromHash("$2a$10$OEiXFrUUY02Nm7YsEgzFuuJ3yO3HAYzJUU7omseluy28s7FYaictu".toCharArray()),
            sameInstance(Hasher.BCRYPT));
        assertThat(Hasher.resolveFromHash("$2a$11$Ya53LCozFlKABu05xsAbj.9xmrczyuAY/fTvxKkDiHOJc5GYcaNRy".toCharArray()),
            sameInstance(Hasher.BCRYPT));
        assertThat(Hasher.resolveFromHash("$2a$12$oUW2hiWBHYwbJamWi6YDPeKS2NBCvD4GR50zh9QZCcgssNFcbpg/a".toCharArray()),
            sameInstance(Hasher.BCRYPT));
        assertThat(Hasher.resolveFromHash("$2a$13$0PDx6mxKK4bLSgpc5H6eaeylWub7UFghjxV03lFYSz4WS4slDT30q".toCharArray()),
            sameInstance(Hasher.BCRYPT));
        assertThat(Hasher.resolveFromHash("$2a$14$lFyXmX7p9/FHr7W4nxTnfuCkjAoBHv6awQlv8jlKZ/YCMI65i38e6".toCharArray()),
            sameInstance(Hasher.BCRYPT));
        assertThat(Hasher.resolveFromHash("{PBKDF2}10000$IoqTZ/Mazky3nmaMr2sb4HZKxcPE3.ap$3FLAeH690WnfqObrA/2P8UZHnTofFqvqpbb9XZ3PaLs="
            .toCharArray()), sameInstance(Hasher.PBKDF2));
        assertThat(Hasher.resolveFromHash("{PBKDF2}1000$S3CJH450PGQQJ9m8hmDHhMzcFyLO971X$eTFoAKRGp7AdRsJ0RDXFESjtSPBr/lwi7SlbzFv+yEU="
            .toCharArray()), sameInstance
            (Hasher.PBKDF2));
        assertThat(Hasher.resolveFromHash("{PBKDF2}10000$gXysXCH2Ddu8rPhNpvYxLIEJP4YElmJ7$EUV0S8yuk44JJ+4CuSVPP03GGJF5wNL8UDMwK8QYS7c="
            .toCharArray()), sameInstance
            (Hasher.PBKDF2));
        assertThat(Hasher.resolveFromHash("{PBKDF2}50000$v33u7dSLlT4F7et9/g7bQyStPGlekHmz$p22u7Pgey3IjDwQikozGbFTDWSA7S9QvXacTJHMBkFY="
            .toCharArray()), sameInstance
            (Hasher.PBKDF2));
        assertThat(Hasher.resolveFromHash("{PBKDF2}100000$iOW1SQ.fqRGpolnB6mN65YZK6Cv0.2Uq$Sam2uNnVCw/zVzrQP4HLX1EfvntZvzakPA8EkMxfmHA="
            .toCharArray()), sameInstance
            (Hasher.PBKDF2));
        assertThat(Hasher.resolveFromHash("{PBKDF2}500000$CwY2WD90z1Cw06dEGlhuT2UCCg0o./XI$Hsus6TS1iwAnCzbwMhwTnFC2406oLe09sncOMlOYGu0="
            .toCharArray()), sameInstance
            (Hasher.PBKDF2));
        assertThat(Hasher.resolveFromHash("{PBKDF2}1000000$IKYyQB5bcGR8S2vr3FuFuOpQitRQq5T/$ZOfkmm36GGfEeoQGRg9hoh2WrhKkmHFMoJT+48zYRE4="
            .toCharArray()), sameInstance
            (Hasher.PBKDF2));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            Hasher.resolveFromHash("{GBGN}cGR8S2vr3FuFuOpQitR".toCharArray());
        });
        assertThat(e.getMessage(), containsString("unknown hash format for hash"));
    }

    private static void testHasherSelfGenerated(Hasher hasher) {
        SecureString passwd = new SecureString(randomAlphaOfLength(10).toCharArray());
        char[] hash = hasher.hash(passwd);
        assertTrue(hasher.verify(passwd, hash));
    }
}

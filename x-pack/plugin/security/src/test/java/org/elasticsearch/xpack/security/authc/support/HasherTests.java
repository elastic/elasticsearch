/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.HasherFactory;

public class HasherTests extends ESTestCase {
    public void testBcryptFamilySelfGenerated() throws Exception {
        testHasherSelfGenerated(HasherFactory.getHasher("bcrypt"));
        testHasherSelfGenerated(HasherFactory.getHasher("bcrypt4"));
        testHasherSelfGenerated(HasherFactory.getHasher("bcrypt5"));
        testHasherSelfGenerated(HasherFactory.getHasher("bcrypt6"));
        testHasherSelfGenerated(HasherFactory.getHasher("bcrypt7"));
        testHasherSelfGenerated(HasherFactory.getHasher("bcrypt8"));
        testHasherSelfGenerated(HasherFactory.getHasher("bcrypt9"));
        testHasherSelfGenerated(HasherFactory.getHasher("bcrypt10"));
        testHasherSelfGenerated(HasherFactory.getHasher("bcrypt11"));
        testHasherSelfGenerated(HasherFactory.getHasher("bcrypt12"));
        testHasherSelfGenerated(HasherFactory.getHasher("bcrypt13"));
        testHasherSelfGenerated(HasherFactory.getHasher("bcrypt14"));
    }

    public void testMd5SelfGenerated() throws Exception {
        testHasherSelfGenerated(HasherFactory.getHasher("md5"));
    }

    public void testSha1SelfGenerated() throws Exception {
        testHasherSelfGenerated(HasherFactory.getHasher("sha1"));
    }

    public void testSSHA256SelfGenerated() throws Exception {
        testHasherSelfGenerated(HasherFactory.getHasher("ssha256"));
    }

    public void testPBKDF2SelfGenerated() throws Exception {
        testHasherSelfGenerated(HasherFactory.getHasher("pbkdf2"));
    }

    public void testNoopSelfGenerated() throws Exception {
        testHasherSelfGenerated(HasherFactory.getHasher("noop"));
    }

    public void testHasherFactory() throws Exception {
        assertEquals("bcrypt", HasherFactory.getHasher("bcrypt").getAlgorithm());
        assertEquals("bcrypt", HasherFactory.getHasher("bcrypt4").getAlgorithm());
        assertEquals("bcrypt", HasherFactory.getHasher("bcrypt5").getAlgorithm());
        assertEquals("bcrypt", HasherFactory.getHasher("bcrypt6").getAlgorithm());
        assertEquals("bcrypt", HasherFactory.getHasher("bcrypt7").getAlgorithm());
        assertEquals("bcrypt", HasherFactory.getHasher("bcrypt8").getAlgorithm());
        assertEquals("bcrypt", HasherFactory.getHasher("bcrypt9").getAlgorithm());
        assertEquals("sha1", HasherFactory.getHasher("sha1").getAlgorithm());
        assertEquals("md5", HasherFactory.getHasher("md5").getAlgorithm());
        assertEquals("ssha256", HasherFactory.getHasher("ssha256").getAlgorithm());
        assertEquals("pbkdf2", HasherFactory.getHasher("pbkdf2").getAlgorithm());
        assertEquals("noop", HasherFactory.getHasher("noop").getAlgorithm());
        assertEquals("noop", HasherFactory.getHasher("clear_text").getAlgorithm());
        try {
            HasherFactory.getHasher("unknown_hasher");
            fail("expected a settings error when trying to resolve an unknown hasher");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private static void testHasherSelfGenerated(Hasher hasher) throws Exception {
        SecureString passwd = new SecureString(randomAlphaOfLength(10));
        char[] hash = hasher.hash(passwd);
        assertTrue(hasher.verify(passwd, hash));
    }
}

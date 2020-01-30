/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.security.SecureRandom;

public class PasswordBasedEncryptionTests extends ESTestCase {

    public void testEncryptAndDecryptEmpty() throws Exception {
        PasswordBasedEncryption encryptor = new PasswordBasedEncryption(new char[] {'p', 'a', 's', 's'},
                SecureRandom.getInstance("SHA1PRNG"));
        byte[] emptyEncrypted = encryptor.encrypt(new byte[0]);
        byte[] ans = encryptor.decrypt(emptyEncrypted);
        assertThat(ans.length, Matchers.is(0));
    }

}

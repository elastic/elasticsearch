/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.test.ESTestCase;

import javax.net.ssl.KeyManagerFactory;
import java.nio.file.Path;
import java.security.KeyStoreException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class Pkcs11KeyConfigTests extends ESTestCase {

    public void testTryLoadPkcs11Keystore() throws Exception {
        assumeFalse("Can't run in a FIPS JVM", inFipsJvm());
        char[] password = "password".toCharArray();
        final Path configPath = getDataPath(".");
        final String algorithm = KeyManagerFactory.getDefaultAlgorithm();
        final Pkcs11KeyConfig pkcs11KeyConfig = new Pkcs11KeyConfig(password, password, algorithm, configPath);
        final SslConfigException ee = expectThrows(SslConfigException.class, pkcs11KeyConfig::createKeyManager);
        assertThat(ee.getMessage(), containsString("cannot load [PKCS11] keystore"));
        assertThat(ee.getCause(), instanceOf(KeyStoreException.class));
        assertThat(ee.getCause().getMessage(), containsString("PKCS11 not found"));
    }

}

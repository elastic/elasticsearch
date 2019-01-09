/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.jwt;

import java.security.GeneralSecurityException;

public interface JwtSigner {

    /**
     * Signs the data byte array using the specified algorithm and private or secret key
     *
     * @param data the data to be signed
     * @return the signature bytes
     * @throws GeneralSecurityException if any error was encountered while signing
     */
    byte[] sign(byte[] data) throws GeneralSecurityException;
}

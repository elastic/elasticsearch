/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.core;

import net.nicholaswilliams.java.licensing.encryption.PublicKeyDataProvider;
import net.nicholaswilliams.java.licensing.exception.KeyNotFoundException;
import org.elasticsearch.common.io.Streams;

import java.io.IOException;
import java.io.InputStream;

/**
 */
public class ResourcePublicKeyDataProvider implements PublicKeyDataProvider {

    private final String resource;

    public ResourcePublicKeyDataProvider(String resource) {
        this.resource = resource;
    }

    @Override
    public byte[] getEncryptedPublicKeyData() throws KeyNotFoundException {
        try(InputStream inputStream = this.getClass().getResourceAsStream(resource)) {
            return Streams.copyToByteArray(inputStream);
        } catch (IOException ex) {
            throw new KeyNotFoundException(ex);
        }
    }
}

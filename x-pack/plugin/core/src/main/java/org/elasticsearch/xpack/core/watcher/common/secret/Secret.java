/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.common.secret;

import org.elasticsearch.xpack.core.watcher.crypto.CryptoService;

import java.io.IOException;
import java.util.Arrays;

public class Secret {

    protected final char[] text;

    public Secret(char[] text) {
        this.text = text;
    }

    public char[] text(CryptoService service) {
        if (service == null) {
            return text;
        }
        return service.decrypt(text);
    }

    public String value() throws IOException {
        return new String(text);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Secret secret = (Secret) o;

        return Arrays.equals(text, secret.text);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(text);
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport;

import javax.net.ssl.SSLEngine;
import java.util.Locale;

public enum SSLClientAuth {

    NO() {
        public boolean enabled() {
            return false;
        }

        public void configure(SSLEngine engine) {
            // nothing to do here
            assert !engine.getWantClientAuth();
            assert !engine.getNeedClientAuth();
        }
    },
    OPTIONAL() {
        public boolean enabled() {
            return true;
        }

        public void configure(SSLEngine engine) {
            engine.setWantClientAuth(true);
        }
    },
    REQUIRED() {
        public boolean enabled() {
            return true;
        }

        public void configure(SSLEngine engine) {
            engine.setNeedClientAuth(true);
        }
    };

    public abstract boolean enabled();

    public abstract void configure(SSLEngine engine);

    public static SSLClientAuth parse(String value) {
        assert value != null;
        switch (value.toLowerCase(Locale.ROOT)) {
            case "no":
            case "false":
                return NO;

            case "optional":
                return OPTIONAL;

            case "required":
            case "true":
                return REQUIRED;
            default:
                throw new IllegalArgumentException("could not resolve ssl client auth auth. unknown ssl client auth value [" + value + "]");
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ssl.explain;

class TlsVersion {

    static final TlsVersion SSL2 = new TlsVersion((byte) 2, (byte) 0, "SSL2");
    static final TlsVersion SSL3 = new TlsVersion((byte) 3, (byte) 0, "SSL3");
    static final TlsVersion TLS1_0 = new TlsVersion((byte) 3, (byte) 1, "TLS1");
    static final TlsVersion TLS1_1 = new TlsVersion((byte) 3, (byte) 2, "TLS1.1");
    static final TlsVersion TLS1_2 = new TlsVersion((byte) 3, (byte) 3, "TLS1.2+");
    /**
     * TLS 1.3 uses the TLS1.2 version in the "legacy version" of a message, and then puts the 1.3 version into the extensions.
     * This means that we should never see a 1.3 version in a "legacy" version field.
     */
    static final TlsVersion TLS1_3 = new TlsVersion((byte) 3, (byte) 4, "TLS1.3");

    private final byte major;
    private final byte minor;
    private final String name;

    private TlsVersion(byte major, byte minor, String name) {
        this.major = major;
        this.minor = minor;
        this.name = name;
    }

    static TlsVersion forValue(byte major, byte minor) {
        switch (major) {
            case 2:
                return SSL2;
            case 3:
                switch (minor) {
                    case 0:
                        return SSL3;
                    case 1:
                        return TLS1_0;
                    case 2:
                        return TLS1_1;
                    case 3:
                        return TLS1_2;
                    case 4:
                        return TLS1_3;
                }
        }
        return new TlsVersion(major, minor, "SSL(" + major + "," + minor + ")");
    }

    public String toString() {
        return name;
    }
}

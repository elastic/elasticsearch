/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

public class VersionTests extends ESTestCase {

    public void testStringCtorOrderingSemver() {
        assertTrue(new Version("1").compareTo(new Version("1.0")) < 0);
        assertTrue(new Version("1.0").compareTo(new Version("1.0.0.0.0.0.0.0.0.1")) < 0);
        assertTrue(new Version("1.0.0").compareTo(new Version("1.0.0.0.0.0.0.0.0.1")) < 0);
        assertTrue(new Version("1.0.0").compareTo(new Version("2.0.0")) < 0);
        assertTrue(new Version("2.0.0").compareTo(new Version("11.0.0")) < 0);
        assertTrue(new Version("2.0.0").compareTo(new Version("2.1.0")) < 0);
        assertTrue(new Version("2.1.0").compareTo(new Version("2.1.1")) < 0);
        assertTrue(new Version("2.1.1").compareTo(new Version("2.1.1.0")) < 0);
        assertTrue(new Version("2.0.0").compareTo(new Version("11.0.0")) < 0);
        assertTrue(new Version("1.0.0").compareTo(new Version("2.0")) < 0);
        assertTrue(new Version("1.0.0-a").compareTo(new Version("1.0.0-b")) < 0);
        assertTrue(new Version("1.0.0-1.0.0").compareTo(new Version("1.0.0-2.0")) < 0);
        assertTrue(new Version("1.0.0-alpha").compareTo(new Version("1.0.0-alpha.1")) < 0);
        assertTrue(new Version("1.0.0-alpha.1").compareTo(new Version("1.0.0-alpha.beta")) < 0);
        assertTrue(new Version("1.0.0-alpha.beta").compareTo(new Version("1.0.0-beta")) < 0);
        assertTrue(new Version("1.0.0-beta").compareTo(new Version("1.0.0-beta.2")) < 0);
        assertTrue(new Version("1.0.0-beta.2").compareTo(new Version("1.0.0-beta.11")) < 0);
        assertTrue(new Version("1.0.0-beta11").compareTo(new Version("1.0.0-beta2")) < 0); // correct according to Semver specs
        assertTrue(new Version("1.0.0-beta.11").compareTo(new Version("1.0.0-rc.1")) < 0);
        assertTrue(new Version("1.0.0-rc.1").compareTo(new Version("1.0.0")) < 0);
        assertTrue(new Version("1.0.0").compareTo(new Version("2.0.0-pre127")) < 0);
        assertTrue(new Version("2.0.0-pre127").compareTo(new Version("2.0.0-pre128")) < 0);
        assertTrue(new Version("2.0.0-pre128").compareTo(new Version("2.0.0-pre128-somethingelse")) < 0);
        assertTrue(new Version("2.0.0-pre20201231z110026").compareTo(new Version("2.0.0-pre227")) < 0);
        // invalid versions sort after valid ones
        assertTrue(new Version("99999.99999.99999").compareTo(new Version("1.invalid")) < 0);
        assertTrue(new Version("").compareTo(new Version("a")) < 0);
    }

    public void testBytesRefCtorOrderingSemver() {
        assertTrue(new Version(encodeVersion("1")).compareTo(new Version(encodeVersion("1.0"))) < 0);
        assertTrue(new Version(encodeVersion("1.0")).compareTo(new Version(encodeVersion("1.0.0.0.0.0.0.0.0.1"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0")).compareTo(new Version(encodeVersion("1.0.0.0.0.0.0.0.0.1"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0")).compareTo(new Version(encodeVersion("2.0.0"))) < 0);
        assertTrue(new Version(encodeVersion("2.0.0")).compareTo(new Version(encodeVersion("11.0.0"))) < 0);
        assertTrue(new Version(encodeVersion("2.0.0")).compareTo(new Version(encodeVersion("2.1.0"))) < 0);
        assertTrue(new Version(encodeVersion("2.1.0")).compareTo(new Version(encodeVersion("2.1.1"))) < 0);
        assertTrue(new Version(encodeVersion("2.1.1")).compareTo(new Version(encodeVersion("2.1.1.0"))) < 0);
        assertTrue(new Version(encodeVersion("2.0.0")).compareTo(new Version(encodeVersion("11.0.0"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0")).compareTo(new Version(encodeVersion("2.0"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-a")).compareTo(new Version(encodeVersion("1.0.0-b"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-1.0.0")).compareTo(new Version(encodeVersion("1.0.0-2.0"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-alpha")).compareTo(new Version(encodeVersion("1.0.0-alpha.1"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-alpha.1")).compareTo(new Version(encodeVersion("1.0.0-alpha.beta"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-alpha.beta")).compareTo(new Version(encodeVersion("1.0.0-beta"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-beta")).compareTo(new Version(encodeVersion("1.0.0-beta.2"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-beta.2")).compareTo(new Version(encodeVersion("1.0.0-beta.11"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-beta11")).compareTo(new Version(encodeVersion("1.0.0-beta2"))) < 0); // correct
                                                                                                                         // according to
                                                                                                                         // Semver specs
        assertTrue(new Version(encodeVersion("1.0.0-beta.11")).compareTo(new Version(encodeVersion("1.0.0-rc.1"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0-rc.1")).compareTo(new Version(encodeVersion("1.0.0"))) < 0);
        assertTrue(new Version(encodeVersion("1.0.0")).compareTo(new Version(encodeVersion("2.0.0-pre127"))) < 0);
        assertTrue(new Version(encodeVersion("2.0.0-pre127")).compareTo(new Version(encodeVersion("2.0.0-pre128"))) < 0);
        assertTrue(new Version(encodeVersion("2.0.0-pre128")).compareTo(new Version(encodeVersion("2.0.0-pre128-somethingelse"))) < 0);
        assertTrue(new Version(encodeVersion("2.0.0-pre20201231z110026")).compareTo(new Version(encodeVersion("2.0.0-pre227"))) < 0);
        // invalid versions sort after valid ones
        assertTrue(new Version(encodeVersion("99999.99999.99999")).compareTo(new Version(encodeVersion("1.invalid"))) < 0);
        assertTrue(new Version(encodeVersion("")).compareTo(new Version(encodeVersion("a"))) < 0);
    }

    public void testConstructorsComparison() {
        assertTrue(new Version(encodeVersion("1")).compareTo(new Version("1")) == 0);
        assertTrue(new Version(encodeVersion("1.2.3")).compareTo(new Version("1.2.3")) == 0);
        assertTrue(new Version(encodeVersion("1.2.3-rc1")).compareTo(new Version("1.2.3-rc1")) == 0);
        assertTrue(new Version(encodeVersion("lkjlaskdjf")).compareTo(new Version("lkjlaskdjf")) == 0);
        assertTrue(new Version(encodeVersion("99999.99999.99999")).compareTo(new Version("99999.99999.99999")) == 0);
    }

    private static BytesRef encodeVersion(String version) {
        return VersionEncoder.encodeVersion(version).bytesRef;
    }

}

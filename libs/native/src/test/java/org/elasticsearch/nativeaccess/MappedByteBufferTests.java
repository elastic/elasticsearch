/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

public class MappedByteBufferTests extends ESTestCase {

    static NativeAccess nativeAccess;

    @BeforeClass
    public static void getZstd() {
        nativeAccess = NativeAccess.instance();
    }

    public void testBasic() {
        // nativeAccess.map() //TODO
    }

    private byte[] randomBytes(int size) {
        byte[] buffer = new byte[size];
        random().nextBytes(buffer);
        return buffer;
    }
}

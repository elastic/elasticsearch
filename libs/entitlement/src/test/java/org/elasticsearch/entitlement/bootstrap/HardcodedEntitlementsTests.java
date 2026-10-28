/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bootstrap;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTestCase.WithEntitlementsOnTestCode;

import java.io.ByteArrayInputStream;

import javax.imageio.stream.MemoryCacheImageInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

@WithEntitlementsOnTestCode
public class HardcodedEntitlementsTests extends ESTestCase {

    /**
     * The Tika library can do some things we don't ordinarily want to allow.
     * <p>
     * Note that {@link MemoryCacheImageInputStream} doesn't even use {@code Disposer} in JDK 26,
     * so it's an open question how much effort this deserves.
     */
    public void testTikaPDF() {
        new MemoryCacheImageInputStream(new ByteArrayInputStream("test test".getBytes(UTF_8)));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.Semaphore;

public class ModelImporterTests extends ESTestCase {

    public void testAcquire() throws InterruptedException {
        var s = new Semaphore(5);

        s.acquire();
        s.acquire();
        s.acquire();
        assertEquals(2, s.availablePermits());
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.support.BCrypt;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;

/**
 * Tests for the Bcrypt implementation specifically around modifications we have made
 */
public class BCryptTests extends ESTestCase {
    /*
     * This test checks that the BCrypt implementation can verify passwords correctly when being invoked from multiple
     * threads all the time. This attempts to simulate authentication of many clients at once (without a cache).
     *
     * This test can be used to reproduce the issue in https://github.com/elastic/x-plugins/issues/589, but it is not
     * 100% reliable unless memory parameters are changed such as lowering the heap size to something really small like
     * 16M and the test is really slow since the issue depends on garbage collection and object finalization.
     */
    @AwaitsFix(bugUrl = "need a better way to test this")
    public void testUnderLoad() throws Exception {
        final String password = randomAlphaOfLengthBetween(10, 32);
        final String bcrypt = BCrypt.hashpw(new SecureString(password), BCrypt.gensalt());

        ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(100);
        try {
            List<Callable<Boolean>> callables = new ArrayList<>(100);

            final AtomicBoolean failed = new AtomicBoolean(false);
            for (int i = 0; i < 100; i++) {
                callables.add(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        for (int i = 0; i < 10000 && !failed.get(); i++) {
                            if (BCrypt.checkpw(new SecureString(password), bcrypt) == false) {
                                failed.set(true);
                                return false;
                            }
                        }
                        return true;
                    }
                });
            }

            List<Future<Boolean>> futures = threadPoolExecutor.invokeAll(callables);
            for (Future<Boolean> future : futures) {
                assertThat(future.get(), is(true));
            }
        } finally {
            threadPoolExecutor.shutdownNow();
        }

    }
}

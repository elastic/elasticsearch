/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.secure_sm;

import junit.framework.TestCase;

import java.security.Permission;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/** Simple tests for SecureSM */
public class SecureSMTests extends TestCase {
    static {
        // install a mock security policy:
        // AllPermission to source code
        // ThreadPermission not granted anywhere else
        final ProtectionDomain sourceCode = SecureSM.class.getProtectionDomain();
        Policy.setPolicy(new Policy() {
            @Override
            public boolean implies(ProtectionDomain domain, Permission permission) {
                if (domain == sourceCode) {
                    return true;
                } else if (permission instanceof ThreadPermission) {
                    return false;
                }
                return true;
            }
        });
        System.setSecurityManager(SecureSM.createTestSecureSM());
    }

    @SuppressForbidden(reason = "testing that System#exit is blocked")
    public void testTryToExit() {
        try {
            System.exit(1);
            fail("did not hit expected exception");
        } catch (SecurityException expected) {}
    }

    public void testClassCanExit() {
        assertTrue(SecureSM.classCanExit("org.apache.maven.surefire.booter.CommandReader", SecureSM.TEST_RUNNER_PACKAGES));
        assertTrue(SecureSM.classCanExit("com.carrotsearch.ant.tasks.junit4.slave.JvmExit", SecureSM.TEST_RUNNER_PACKAGES));
        assertTrue(SecureSM.classCanExit("org.eclipse.jdt.internal.junit.runner.RemoteTestRunner", SecureSM.TEST_RUNNER_PACKAGES));
        assertTrue(SecureSM.classCanExit("com.intellij.rt.execution.junit.JUnitStarter", SecureSM.TEST_RUNNER_PACKAGES));
        assertTrue(SecureSM.classCanExit("org.elasticsearch.Foo", new String[] { "org.elasticsearch.Foo" }));
        assertFalse(SecureSM.classCanExit("org.elasticsearch.Foo", new String[] { "org.elasticsearch.Bar" }));
    }

    public void testCreateThread() throws Exception {
        Thread t = new Thread();
        t.start();
        t.join();
        // no exception
    }

    public void testCreateThreadGroup() throws Exception {
        Thread t = new Thread(new ThreadGroup("childgroup"), "child");
        t.start();
        t.join();
        // no exception
    }

    public void testModifyChild() throws Exception {
        final AtomicBoolean interrupted = new AtomicBoolean(false);
        Thread t = new Thread(new ThreadGroup("childgroup"), "child") {
            @Override
            public void run() {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException expected) {
                    interrupted.set(true);
                }
            }
        };
        t.start();
        t.interrupt();
        t.join();
        // no exception
        assertTrue(interrupted.get());
    }

    public void testNoModifySibling() throws Exception {
        final AtomicBoolean interrupted1 = new AtomicBoolean(false);
        final AtomicBoolean interrupted2 = new AtomicBoolean(false);

        final Thread t1 = new Thread(new ThreadGroup("childgroup"), "child") {
            @Override
            public void run() {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException expected) {
                    interrupted1.set(true);
                }
            }
        };
        t1.start();

        Thread t2 = new Thread(new ThreadGroup("anothergroup"), "another child") {
            @Override
            public void run() {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException expected) {
                    interrupted2.set(true);
                    try {
                        t1.interrupt(); // try to bogusly interrupt our sibling
                        fail("did not hit expected exception");
                    } catch (SecurityException expected2) {}
                }
            }
        };
        t2.start();
        t2.interrupt();
        t2.join();
        // sibling attempted to but was not able to muck with its other sibling
        assertTrue(interrupted2.get());
        assertFalse(interrupted1.get());
        // but we are the parent and can terminate
        t1.interrupt();
        t1.join();
        assertTrue(interrupted1.get());
    }

    public void testParallelStreamThreadGroup() throws Exception {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            list.add(i);
        }
        list.parallelStream().collect(Collectors.toSet());
    }
}

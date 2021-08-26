/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.secure_sm;

import java.security.AccessController;
import java.security.Permission;
import java.security.PrivilegedAction;
import java.util.Objects;

/**
 * Extension of SecurityManager that works around a few design flaws in Java Security.
 * <p>
 * There are a few major problems that require custom {@code SecurityManager} logic to fix:
 * <ul>
 *   <li>{@code exitVM} permission is implicitly granted to all code by the default
 *       Policy implementation. For a server app, this is not wanted. </li>
 *   <li>ThreadGroups are not enforced by default, instead only system threads are
 *       protected out of box by {@code modifyThread/modifyThreadGroup}. Applications
 *       are encouraged to override the logic here to implement a stricter policy.
 *   <li>System threads are not even really protected, because if the system uses
 *       ThreadPools, {@code modifyThread} is abused by its {@code shutdown} checks. This means
 *       a thread must have {@code modifyThread} to even terminate its own pool, leaving
 *       system threads unprotected.
 * </ul>
 * This class throws exception on {@code exitVM} calls, and provides a whitelist where calls
 * from exit are allowed.
 * <p>
 * Additionally it enforces threadgroup security with the following rules:
 * <ul>
 *   <li>{@code modifyThread} and {@code modifyThreadGroup} are required for any thread access
 *       checks: with these permissions, access is granted as long as the thread group is
 *       the same or an ancestor ({@code sourceGroup.parentOf(targetGroup) == true}).
 *   <li>code without these permissions can do very little, except to interrupt itself. It may
 *       not even create new threads.
 *   <li>very special cases (like test runners) that have {@link ThreadPermission} can violate
 *       threadgroup security rules.
 * </ul>
 * <p>
 * If java security debugging ({@code java.security.debug}) is enabled, and this SecurityManager
 * is installed, it will emit additional debugging information when threadgroup access checks fail.
 *
 * @see SecurityManager#checkAccess(Thread)
 * @see SecurityManager#checkAccess(ThreadGroup)
 * @see <a href="http://cs.oswego.edu/pipermail/concurrency-interest/2009-August/006508.html">
 *         http://cs.oswego.edu/pipermail/concurrency-interest/2009-August/006508.html</a>
 */
public class SecureSM extends SecurityManager {

    private final String[] classesThatCanExit;

    /**
     * Creates a new security manager where no packages can exit nor halt the virtual machine.
     */
    public SecureSM() {
        this(new String[0]);
    }

    /**
     * Creates a new security manager with the specified list of regular expressions as the those that class names will be tested against to
     * check whether or not a class can exit or halt the virtual machine.
     *
     * @param classesThatCanExit the list of classes that can exit or halt the virtual machine
     */
    public SecureSM(final String[] classesThatCanExit) {
        this.classesThatCanExit = classesThatCanExit;
    }

    /**
     * Creates a new security manager with a standard set of test packages being the only packages that can exit or halt the virtual
     * machine. The packages that can exit are:
     * <ul>
     *    <li><code>org.apache.maven.surefire.booter.</code></li>
     *    <li><code>com.carrotsearch.ant.tasks.junit4.</code></li>
     *    <li><code>org.eclipse.internal.junit.runner.</code></li>
     *    <li><code>com.intellij.rt.execution.junit.</code></li>
     * </ul>
     *
     * @return an instance of SecureSM where test packages can halt or exit the virtual machine
     */
    public static SecureSM createTestSecureSM() {
        return new SecureSM(TEST_RUNNER_PACKAGES);
    }

    static final String[] TEST_RUNNER_PACKAGES = new String[] {
        // surefire test runner
        "org\\.apache\\.maven\\.surefire\\.booter\\..*",
        // junit4 test runner
        "com\\.carrotsearch\\.ant\\.tasks\\.junit4\\.slave\\..*",
        // eclipse test runner
        "org\\.eclipse.jdt\\.internal\\.junit\\.runner\\..*",
        // intellij test runner (before IDEA version 2019.3)
        "com\\.intellij\\.rt\\.execution\\.junit\\..*",
        // intellij test runner (since IDEA version 2019.3)
        "com\\.intellij\\.rt\\.junit\\..*"
    };

    // java.security.debug support
    private static final boolean DEBUG = AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
        @Override
        public Boolean run() {
            try {
                String v = System.getProperty("java.security.debug");
                // simple check that they are trying to debug
                return v != null && v.length() > 0;
            } catch (SecurityException e) {
                return false;
            }
        }
    });

    @Override
    @SuppressForbidden(reason = "java.security.debug messages go to standard error")
    public void checkAccess(Thread t) {
        try {
            checkThreadAccess(t);
        } catch (SecurityException e) {
            if (DEBUG) {
                System.err.println("access: caller thread=" + Thread.currentThread());
                System.err.println("access: target thread=" + t);
                debugThreadGroups(Thread.currentThread().getThreadGroup(), t.getThreadGroup());
            }
            throw e;
        }
    }

    @Override
    @SuppressForbidden(reason = "java.security.debug messages go to standard error")
    public void checkAccess(ThreadGroup g) {
        try {
            checkThreadGroupAccess(g);
        } catch (SecurityException e) {
            if (DEBUG) {
                System.err.println("access: caller thread=" + Thread.currentThread());
                debugThreadGroups(Thread.currentThread().getThreadGroup(), g);
            }
            throw e;
        }
    }

    @SuppressForbidden(reason = "java.security.debug messages go to standard error")
    private void debugThreadGroups(final ThreadGroup caller, final ThreadGroup target) {
        System.err.println("access: caller group=" + caller);
        System.err.println("access: target group=" + target);
    }

    // thread permission logic

    private static final Permission MODIFY_THREAD_PERMISSION = new RuntimePermission("modifyThread");
    private static final Permission MODIFY_INNOCUOUS_THREAD_PERMISSION = new RuntimePermission("org.elasticsearch.modifyInnocuousThread");
    private static final Permission MODIFY_ARBITRARY_THREAD_PERMISSION = new ThreadPermission("modifyArbitraryThread");

    protected void checkThreadAccess(Thread t) {
        Objects.requireNonNull(t);

        // first, check if we can modify threads at all.
        checkPermission(MODIFY_THREAD_PERMISSION);

        // check the threadgroup, if its our thread group or an ancestor, its fine.
        final ThreadGroup source = Thread.currentThread().getThreadGroup();
        final ThreadGroup target = t.getThreadGroup();

        if (target == null) {
            return;    // its a dead thread, do nothing.
        } else if (source.parentOf(target) == false) {
            // The JDK doesn't consistently wrap this in a doPrivilege call, so we use a special permission instead
            if (t.getClass().getName().equals("jdk.internal.misc.InnocuousThread")) {
                checkPermission(MODIFY_INNOCUOUS_THREAD_PERMISSION);
            } else {
                checkPermission(MODIFY_ARBITRARY_THREAD_PERMISSION);
            }
        }
    }

    private static final Permission MODIFY_THREADGROUP_PERMISSION = new RuntimePermission("modifyThreadGroup");
    private static final Permission MODIFY_ARBITRARY_THREADGROUP_PERMISSION = new ThreadPermission("modifyArbitraryThreadGroup");

    protected void checkThreadGroupAccess(ThreadGroup g) {
        Objects.requireNonNull(g);

        // first, check if we can modify thread groups at all.
        checkPermission(MODIFY_THREADGROUP_PERMISSION);

        // check the threadgroup, if its our thread group or an ancestor, its fine.
        final ThreadGroup source = Thread.currentThread().getThreadGroup();
        final ThreadGroup target = g;

        if (source == null) {
            return; // we are a dead thread, do nothing
        } else if (source.parentOf(target) == false) {
            checkPermission(MODIFY_ARBITRARY_THREADGROUP_PERMISSION);
        }
    }

    // exit permission logic
    @Override
    public void checkExit(int status) {
        innerCheckExit(status);
    }

    /**
     * The "Uwe Schindler" algorithm.
     *
     * @param status the exit status
     */
    protected void innerCheckExit(final int status) {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                final String systemClassName = System.class.getName(),
                        runtimeClassName = Runtime.class.getName();
                String exitMethodHit = null;
                for (final StackTraceElement se : Thread.currentThread().getStackTrace()) {
                    final String className = se.getClassName(), methodName = se.getMethodName();
                    if (
                        ("exit".equals(methodName) || "halt".equals(methodName)) &&
                        (systemClassName.equals(className) || runtimeClassName.equals(className))
                    ) {
                        exitMethodHit = className + '#' + methodName + '(' + status + ')';
                        continue;
                    }

                    if (exitMethodHit != null) {
                        if (classesThatCanExit == null) {
                            break;
                        }
                        if (classCanExit(className, classesThatCanExit)) {
                            // this exit point is allowed, we return normally from closure:
                            return null;
                        }
                        // anything else in stack trace is not allowed, break and throw SecurityException below:
                        break;
                    }
                }

                if (exitMethodHit == null) {
                    // should never happen, only if JVM hides stack trace - replace by generic:
                    exitMethodHit = "JVM exit method";
                }
                throw new SecurityException(exitMethodHit + " calls are not allowed");
            }
        });

        // we passed the stack check, delegate to super, so default policy can still deny permission:
        super.checkExit(status);
    }

    static boolean classCanExit(final String className, final String[] classesThatCanExit) {
        for (final String classThatCanExit : classesThatCanExit) {
            if (className.matches(classThatCanExit)) {
                return true;
            }
        }
        return false;
    }

}

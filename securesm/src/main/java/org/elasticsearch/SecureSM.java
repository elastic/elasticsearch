package org.elasticsearch;

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
 * This class throws exception on {@code exitVM} calls, and provides a testing mode
 * where calls from test runners are allowed.
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
  private final boolean allowTestExit;
  
  /**
   * Create a new SecurityManager.
   */
  public SecureSM() {
    this(false);
  }
  
  /** 
   * Expert: for testing only.
   * @param allowTestExit {@code true} if test-runners should be allowed to exit the VM.
   */
  public SecureSM(boolean allowTestExit) {
    this.allowTestExit = allowTestExit;
  }
  
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
  public void checkAccess(Thread t) {
    try {
      checkThreadAccess(t);
    } catch (SecurityException e) {
      if (DEBUG) {
        System.out.println("access: caller thread=" + Thread.currentThread());
        System.out.println("access: target thread=" + t);
        debugThreadGroups(Thread.currentThread().getThreadGroup(), t.getThreadGroup());
      }
      throw e;
    }
  }
  
  @Override
  public void checkAccess(ThreadGroup g) {
    try {
      checkThreadGroupAccess(g);
    } catch (SecurityException e) {
      if (DEBUG) {
        System.out.println("access: caller thread=" + Thread.currentThread());
        debugThreadGroups(Thread.currentThread().getThreadGroup(), g);
      }
      throw e;
    }
  }
  
  private void debugThreadGroups(final ThreadGroup caller, final ThreadGroup target) {
    System.out.println("access: caller group=" + caller);
    System.out.println("access: target group=" + target);
  }
  
  // thread permission logic

  private static final Permission MODIFY_THREAD_PERMISSION = new RuntimePermission("modifyThread");
  private static final Permission MODIFY_ARBITRARY_THREAD_PERMISSION = new ThreadPermission("modifyArbitraryThread");

  protected void checkThreadAccess(Thread t) {
    Objects.requireNonNull(t);

    // first, check if we can modify threads at all.
    checkPermission(MODIFY_THREAD_PERMISSION);
    
    // check the threadgroup, if its our thread group or an ancestor, its fine.
    final ThreadGroup source = Thread.currentThread().getThreadGroup();
    final ThreadGroup target = t.getThreadGroup();
    
    if (target == null) {
      return;  // its a dead thread, do nothing.
    } else if (source.parentOf(target) == false) {
      checkPermission(MODIFY_ARBITRARY_THREAD_PERMISSION);
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
    if (allowTestExit) {
      checkTestExit(status);
    } else {
      throw new SecurityException("exit(" + status + ") not allowed by system policy");
    }
  }

  static final String TEST_RUNNER_PACKAGES[] = {
    // surefire test runner
    "org.apache.maven.surefire.booter.",
    // junit4 test runner
    "com.carrotsearch.ant.tasks.junit4.",
    // eclipse test runner
    "org.eclipse.jdt.internal.junit.runner.",
    // intellij test runner
    "com.intellij.rt.execution.junit."
  };
  
  /**
   * The "Uwe Schindler" algorithm.
   */
  protected void checkTestExit(final int status) {
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
            for (String testPackage : TEST_RUNNER_PACKAGES) {
              if (className.startsWith(testPackage)) {
                // this exit point is allowed, we return normally from closure:
                return null;
              }
            }
            // anything else in stack trace is not allowed, break and throw SecurityException below:
            break;
          }
        }
        
        if (exitMethodHit == null) {
          // should never happen, only if JVM hides stack trace - replace by generic:
          exitMethodHit = "JVM exit method";
        }
        throw new SecurityException(exitMethodHit + " calls are not allowed because they terminate the test runner's JVM.");
      }
    });
    
    // we passed the stack check, delegate to super, so default policy can still deny permission:
    super.checkExit(status);
  }
}

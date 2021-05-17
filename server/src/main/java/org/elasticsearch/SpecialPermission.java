/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import java.security.BasicPermission;

/**
 * Elasticsearch-specific permission to check before entering
 * {@code AccessController.doPrivileged()} blocks.
 * <p>
 * We try to avoid these blocks in our code and keep security simple,
 * but we need them for a few special places to contain hacks for third
 * party code, or dangerous things used by scripting engines.
 * <p>
 * All normal code has this permission, but checking this before truncating the stack
 * prevents unprivileged code (e.g. scripts), which do not have it, from gaining elevated
 * privileges.
 * <p>
 * In other words, don't do this:
 * <br>
 * <pre><code>
 *   // throw away all information about caller and run with our own privs
 *   AccessController.doPrivileged(
 *    ...
 *   );
 * </code></pre>
 * <br>
 * Instead do this;
 * <br>
 * <pre><code>
 *   // check caller first, to see if they should be allowed to do this
 *   SecurityManager sm = System.getSecurityManager();
 *   if (sm != null) {
 *     sm.checkPermission(new SpecialPermission());
 *   }
 *   // throw away all information about caller and run with our own privs
 *   AccessController.doPrivileged(
 *    ...
 *   );
 * </code></pre>
 */
public final class SpecialPermission extends BasicPermission {

    public static final SpecialPermission INSTANCE = new SpecialPermission();

    /**
     * Creates a new SpecialPermission object.
     */
    public SpecialPermission() {
        // TODO: if we really need we can break out name (e.g. "hack" or "scriptEngineService" or whatever).
        // but let's just keep it simple if we can.
        super("*");
    }

    /**
     * Creates a new SpecialPermission object.
     * This constructor exists for use by the {@code Policy} object to instantiate new Permission objects.
     *
     * @param name ignored
     * @param actions ignored
     */
    public SpecialPermission(String name, String actions) {
        this();
    }

    /**
     * Check that the current stack has {@link SpecialPermission} access according to the {@link SecurityManager}.
     */
    public static void check() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(INSTANCE);
        }
    }
}

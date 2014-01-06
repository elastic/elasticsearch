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

package org.elasticsearch.common.lease;

import java.util.Arrays;

/** Utility methods to work with {@link Releasable}s. */
public enum Releasables {
    ;

    private static void rethrow(Throwable t) {
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        }
        if (t instanceof Error) {
            throw (Error) t;
        }
        throw new RuntimeException(t);
    }

    private static void release(Iterable<Releasable> releasables, boolean ignoreException) {
        Throwable th = null;
        for (Releasable releasable : releasables) {
            if (releasable != null) {
                try {
                    releasable.release();
                } catch (Throwable t) {
                    if (th == null) {
                        th = t;
                    }
                }
            }
        }
        if (th != null && !ignoreException) {
            rethrow(th);
        }
    }

    /** Release the provided {@link Releasable}s. */
    public static void release(Iterable<Releasable> releasables) {
        release(releasables, false);
    }

    /** Release the provided {@link Releasable}s. */
    public static void release(Releasable... releasables) {
        release(Arrays.asList(releasables));
    }

    /** Release the provided {@link Releasable}s, ignoring exceptions. */
    public static void releaseWhileHandlingException(Iterable<Releasable> releasables) {
        release(releasables, true);
    }

    /** Release the provided {@link Releasable}s, ignoring exceptions. */
    public static void releaseWhileHandlingException(Releasable... releasables) {
        releaseWhileHandlingException(Arrays.asList(releasables));
    }

    /** Release the provided {@link Releasable}s, ignoring exceptions if <code>success</code> is <tt>false</tt>. */
    public static void release(boolean success, Iterable<Releasable> releasables) {
        if (success) {
            release(releasables);
        } else {
            releaseWhileHandlingException(releasables);
        }
    }

    /** Release the provided {@link Releasable}s, ignoring exceptions if <code>success</code> is <tt>false</tt>. */
    public static void release(boolean success, Releasable... releasables) {
        release(success, Arrays.asList(releasables));
    }
}

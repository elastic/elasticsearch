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

import org.elasticsearch.ElasticsearchException;

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

    private static void close(Iterable<? extends Releasable> releasables, boolean ignoreException) {
        Throwable th = null;
        for (Releasable releasable : releasables) {
            if (releasable != null) {
                try {
                    releasable.close();
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
    public static void close(Iterable<? extends Releasable> releasables) {
        close(releasables, false);
    }

    /** Release the provided {@link Releasable}s. */
    public static void close(Releasable... releasables) {
        close(Arrays.asList(releasables));
    }

    /** Release the provided {@link Releasable}s, ignoring exceptions. */
    public static void closeWhileHandlingException(Iterable<Releasable> releasables) {
        close(releasables, true);
    }

    /** Release the provided {@link Releasable}s, ignoring exceptions. */
    public static void closeWhileHandlingException(Releasable... releasables) {
        closeWhileHandlingException(Arrays.asList(releasables));
    }

    /** Release the provided {@link Releasable}s, ignoring exceptions if <code>success</code> is <tt>false</tt>. */
    public static void close(boolean success, Iterable<Releasable> releasables) {
        if (success) {
            close(releasables);
        } else {
            closeWhileHandlingException(releasables);
        }
    }

    /** Release the provided {@link Releasable}s, ignoring exceptions if <code>success</code> is <tt>false</tt>. */
    public static void close(boolean success, Releasable... releasables) {
        close(success, Arrays.asList(releasables));
    }

    /** Wrap several releasables into a single one. This is typically useful for use with try-with-resources: for example let's assume
     *  that you store in a list several resources that you would like to see released after execution of the try block:
     *
     *  <pre>
     *  List&lt;Releasable&gt; resources = ...;
     *  try (Releasable releasable = Releasables.wrap(resources)) {
     *      // do something
     *  }
     *  // the resources will be released when reaching here
     *  </pre>
     */
    public static Releasable wrap(final Iterable<Releasable> releasables) {
        return new Releasable() {

            @Override
            public void close() throws ElasticsearchException {
                Releasables.close(releasables);
            }

        };
    }

    /** @see #wrap(Iterable) */
    public static Releasable wrap(final Releasable... releasables) {
        return new Releasable() {

            @Override
            public void close() throws ElasticsearchException {
                Releasables.close(releasables);
            }

        };
    }
}

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

package org.elasticsearch.nio.utils;

import java.util.List;

// TODO: This should eventually be removed once ExceptionsHelper is moved to a core library jar
public class ExceptionsHelper {

    /**
     * Rethrows the first exception in the list and adds all remaining to the suppressed list.
     * If the given list is empty no exception is thrown
     *
     */
    public static <T extends Throwable> void rethrowAndSuppress(List<T> exceptions) throws T {
        T main = null;
        for (T ex : exceptions) {
            main = useOrSuppress(main, ex);
        }
        if (main != null) {
            throw main;
        }
    }

    private static <T extends Throwable> T useOrSuppress(T first, T second) {
        if (first == null) {
            return second;
        } else {
            first.addSuppressed(second);
        }
        return first;
    }
}

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

package org.elasticsearch.painless;

/**
 * The PainlessError class is used to throw internal errors caused by Painless scripts that cannot be
 * caught using a standard {@link Exception}.  This prevents the user from catching this specific error
 * (as Exceptions are available in the Painless API, but Errors are not,) and possibly continuing to do
 * something hazardous.  The alternative was extending {@link Throwable}, but that seemed worse than using
 * an {@link Error} in this case.
 */
@SuppressWarnings("serial")
public class PainlessError extends Error {

    /**
     * Constructor.
     * @param message The error message.
     */
    public PainlessError(final String message) {
       super(message);
    }
}

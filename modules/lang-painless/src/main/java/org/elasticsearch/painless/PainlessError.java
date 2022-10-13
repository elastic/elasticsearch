/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

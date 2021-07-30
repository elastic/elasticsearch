/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import java.nio.file.Path;

/**
 * Wrapper exception for checked exceptions thrown during the bootstrap process. Methods invoked
 * during bootstrap should explicitly declare the checked exceptions that they can throw, rather
 * than declaring the top-level checked exception {@link Exception}. This exception exists to wrap
 * these checked exceptions so that
 * {@link Bootstrap#init(boolean, Path, boolean, org.elasticsearch.env.Environment)}
 * does not have to declare all of these checked exceptions.
 */
class BootstrapException extends Exception {

    /**
     * Wraps an existing exception.
     *
     * @param cause the underlying cause of bootstrap failing
     */
    BootstrapException(final Exception cause) {
        super(cause);
    }

}

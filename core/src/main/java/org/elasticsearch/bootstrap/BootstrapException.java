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

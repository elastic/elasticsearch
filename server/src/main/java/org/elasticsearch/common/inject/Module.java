/*
 * Copyright (C) 2006 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.common.inject;

/**
 * A module contributes configuration information, typically interface
 * bindings, which will be used to create an {@link Injector}. A Guice-based
 * application is ultimately composed of little more than a set of
 * {@code Module}s and some bootstrapping code.
 * <p>
 * Your Module classes can use a more streamlined syntax by extending
 * {@link AbstractModule} rather than implementing this interface directly.
 * <p>
 * In addition to the bindings configured via {@link #configure}, bindings
 * will be created for all methods annotated with {@literal @}{@link Provides}.
 * Use scope and binding annotations on these methods to configure the
 * bindings.
 */
public interface Module {

    /**
     * Contributes bindings and other configurations for this module to {@code binder}.
     * <p>
     * <strong>Do not invoke this method directly</strong> to install submodules. Instead use
     * {@link Binder#install(Module)}, which ensures that {@link Provides provider methods} are
     * discovered.
     */
    void configure(Binder binder);
}

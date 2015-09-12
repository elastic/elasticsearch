/**
 * Copyright (C) 2008 Google Inc.
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

package org.elasticsearch.common.inject.spi;

import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Key;

import java.util.List;
import java.util.Set;

/**
 * A private collection of elements that are hidden from the enclosing injector or module by
 * default. See {@link org.elasticsearch.common.inject.PrivateModule PrivateModule} for details.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public interface PrivateElements extends Element {

    /**
     * Returns the configuration information in this private environment.
     */
    List<Element> getElements();

    /**
     * Returns the child injector that hosts these private elements, or null if the elements haven't
     * been used to create an injector.
     */
    Injector getInjector();

    /**
     * Returns the unique exposed keys for these private elements.
     */
    Set<Key<?>> getExposedKeys();

    /**
     * Returns an arbitrary object containing information about the "place" where this key was
     * exposed. Used by Guice in the production of descriptive error messages.
     * <p/>
     * <p>Tools might specially handle types they know about; {@code StackTraceElement} is a good
     * example. Tools should simply call {@code toString()} on the source object if the type is
     * unfamiliar.
     *
     * @param key one of the keys exposed by this module.
     */
    Object getExposedSource(Key<?> key);
}

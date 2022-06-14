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

import org.elasticsearch.common.inject.spi.InjectionPoint;

import java.lang.reflect.InvocationTargetException;

/**
 * Proxies calls to a {@link java.lang.reflect.Constructor} for a class
 * {@code T}.
 *
 * @author crazybob@google.com (Bob Lee)
 */
interface ConstructionProxy<T> {

    /**
     * Constructs an instance of {@code T} for the given arguments.
     */
    T newInstance(Object... arguments) throws InvocationTargetException;

    /**
     * Returns the injection point for this constructor.
     */
    InjectionPoint getInjectionPoint();

}

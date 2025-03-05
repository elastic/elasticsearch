/*
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

package org.elasticsearch.injection.guice.spi;

import org.elasticsearch.injection.guice.Binding;

import java.util.Set;

/**
 * A binding to a single instance. The same instance is returned for every injection.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public interface InstanceBinding<T> extends Binding<T> {

    /**
     * Returns the user-supplied instance.
     */
    T getInstance();

    /**
     * Returns the field and method injection points of the instance, injected at injector-creation
     * time only.
     *
     * @return a possibly empty set
     */
    Set<InjectionPoint> getInjectionPoints();

}

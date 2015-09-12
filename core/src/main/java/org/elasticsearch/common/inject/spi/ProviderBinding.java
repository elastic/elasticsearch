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

import org.elasticsearch.common.inject.Binding;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Provider;

/**
 * A binding to a {@link Provider} that delegates to the binding for the provided type. This binding
 * is used whenever a {@code Provider<T>} is injected (as opposed to injecting {@code T} directly).
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public interface ProviderBinding<T extends Provider<?>> extends Binding<T> {

    /**
     * Returns the key whose binding is used to {@link Provider#get provide instances}. That binding
     * can be retrieved from an injector using {@link org.elasticsearch.common.inject.Injector#getBinding(Key)
     * Injector.getBinding(providedKey)}
     */
    Key<?> getProvidedKey();
}
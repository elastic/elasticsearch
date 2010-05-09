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

package org.elasticsearch.util.inject.spi;

import org.elasticsearch.util.inject.Binding;
import org.elasticsearch.util.inject.Key;
import org.elasticsearch.util.inject.Provider;

/**
 * A binding to a provider key. To resolve injections, the provider key is first resolved, then that
 * provider's {@code get} method is invoked.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public interface ProviderKeyBinding<T> extends Binding<T> {

  /**
   * Returns the key used to resolve the provider's binding. That binding can be retrieved from an
   * injector using {@link org.elasticsearch.util.inject.Injector#getBinding(Key)
   * Injector.getBinding(providerKey)}
   */
  Key<? extends Provider<? extends T>> getProviderKey();

}
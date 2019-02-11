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

package org.elasticsearch.common.inject.spi;

import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Binding;

/**
 * A binding to a key exposed from an enclosed private environment.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public interface ExposedBinding<T> extends Binding<T>, HasDependencies {

    /**
     * Returns the enclosed environment that holds the original binding.
     */
    PrivateElements getPrivateElements();

    /**
     * Unsupported. Always throws {@link UnsupportedOperationException}.
     */
    @Override
    void applyTo(Binder binder);
}

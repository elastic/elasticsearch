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

import org.elasticsearch.common.inject.Binding;
import org.elasticsearch.common.inject.Key;

import java.util.Set;

/**
 * A binding created from converting a bound instance to a new type. The source binding has the same
 * binding annotation but a different type.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public interface ConvertedConstantBinding<T> extends Binding<T>, HasDependencies {

    /**
     * Returns the converted value.
     */
    T getValue();

    /**
     * Returns the key for the source binding.
     */
    Key<String> getSourceKey();

    /**
     * Returns a singleton set containing only the converted key.
     */
    @Override
    Set<Dependency<?>> getDependencies();
}
/*
 * Copyright (C) 2007 Google Inc.
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

package org.elasticsearch.common.inject.binder;

/**
 * Binds to a constant value.
 */
public interface ConstantBindingBuilder {

    /**
     * Binds constant to the given value.
     */
    void to(String value);

    /**
     * Binds constant to the given value.
     */
    void to(int value);

    /**
     * Binds constant to the given value.
     */
    void to(long value);

    /**
     * Binds constant to the given value.
     */
    void to(boolean value);

    /**
     * Binds constant to the given value.
     */
    void to(double value);

    /**
     * Binds constant to the given value.
     */
    void to(float value);

    /**
     * Binds constant to the given value.
     */
    void to(short value);

    /**
     * Binds constant to the given value.
     */
    void to(char value);

    /**
     * Binds constant to the given value.
     */
    void to(Class<?> value);

    /**
     * Binds constant to the given value.
     */
    <E extends Enum<E>> void to(E value);
}

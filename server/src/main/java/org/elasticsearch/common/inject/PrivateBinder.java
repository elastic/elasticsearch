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

package org.elasticsearch.common.inject;

/**
 * Returns a binder whose configuration information is hidden from its environment by default.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public interface PrivateBinder extends Binder {

    /**
     * Makes the binding for {@code key} available to the enclosing environment
     */
    void expose(Key<?> key);

    @Override
    PrivateBinder withSource(Object source);

    @Override
    PrivateBinder skipSources(Class<?>... classesToSkip);
}

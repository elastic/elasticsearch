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

package org.elasticsearch.injection.guice.spi;

import org.elasticsearch.injection.guice.TypeLiteral;

/**
 * Converts constant string values to a different type.
 *
 * @author crazybob@google.com (Bob Lee)
 * @since 2.0
 */
public interface TypeConverter {

    /**
     * Converts a string value. Throws an exception if a conversion error occurs.
     */
    Object convert(String value, TypeLiteral<?> toType);
}

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

package org.elasticsearch.common.inject.internal;

import org.elasticsearch.common.inject.BindingAnnotation;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author jessewilson@google.com (Jesse Wilson)
 */
public class UniqueAnnotations {
    private UniqueAnnotations() {
    }

    private static final AtomicInteger nextUniqueValue = new AtomicInteger(1);

    /**
     * Returns an annotation instance that is not equal to any other annotation
     * instances, for use in creating distinct {@link org.elasticsearch.common.inject.Key}s.
     */
    public static Annotation create() {
        return create(nextUniqueValue.getAndIncrement());
    }

    static Annotation create(final int value) {
        return new Internal() {
            @Override
            public int value() {
                return value;
            }

            @Override
            public Class<? extends Annotation> annotationType() {
                return Internal.class;
            }

            @Override
            public String toString() {
                return "@" + Internal.class.getName() + "(value=" + value + ")";
            }

            @Override
            public boolean equals(Object o) {
                return o instanceof Internal
                        && ((Internal) o).value() == value();
            }

            @Override
            public int hashCode() {
                return (127 * "value".hashCode()) ^ value;
            }
        };
    }

    @Retention(RUNTIME)
    @BindingAnnotation
    @interface Internal {
        int value();
    }
}

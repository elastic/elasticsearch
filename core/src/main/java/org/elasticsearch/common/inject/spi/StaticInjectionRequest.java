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

import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.ConfigurationException;

import java.util.Objects;
import java.util.Set;

/**
 * A request to inject the static fields and methods of a type. Requests are created
 * explicitly in a module using {@link org.elasticsearch.common.inject.Binder#requestStaticInjection(Class[])
 * requestStaticInjection()} statements:
 * <pre>
 *     requestStaticInjection(MyLegacyService.class);</pre>
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public final class StaticInjectionRequest implements Element {
    private final Object source;
    private final Class<?> type;

    StaticInjectionRequest(Object source, Class<?> type) {
        this.source = Objects.requireNonNull(source, "source");
        this.type = Objects.requireNonNull(type, "type");
    }

    @Override
    public Object getSource() {
        return source;
    }

    public Class<?> getType() {
        return type;
    }

    /**
     * Returns the static methods and fields of {@code type} that will be injected to fulfill this
     * request.
     *
     * @return a possibly empty set of injection points. The set has a specified iteration order. All
     *         fields are returned and then all methods. Within the fields, supertype fields are returned
     *         before subtype fields. Similarly, supertype methods are returned before subtype methods.
     * @throws ConfigurationException if there is a malformed injection point on {@code type}, such as
     *                                a field with multiple binding annotations. The exception's {@link
     *                                ConfigurationException#getPartialValue() partial value} is a {@code Set<InjectionPoint>}
     *                                of the valid injection points.
     */
    public Set<InjectionPoint> getInjectionPoints() throws ConfigurationException {
        return InjectionPoint.forStaticMethodsAndFields(type);
    }

    @Override
    public void applyTo(Binder binder) {
        binder.withSource(getSource()).requestStaticInjection(type);
    }

    @Override
    public <T> T acceptVisitor(ElementVisitor<T> visitor) {
        return visitor.visit(this);
    }
}

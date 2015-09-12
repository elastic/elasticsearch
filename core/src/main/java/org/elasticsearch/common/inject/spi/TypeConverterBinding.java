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
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.matcher.Matcher;

import java.util.Objects;

/**
 * Registration of type converters for matching target types. Instances are created
 * explicitly in a module using {@link org.elasticsearch.common.inject.Binder#convertToTypes(Matcher,
 * TypeConverter) convertToTypes()} statements:
 * <pre>
 *     convertToTypes(Matchers.only(DateTime.class), new DateTimeConverter());</pre>
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public final class TypeConverterBinding implements Element {
    private final Object source;
    private final Matcher<? super TypeLiteral<?>> typeMatcher;
    private final TypeConverter typeConverter;

    TypeConverterBinding(Object source, Matcher<? super TypeLiteral<?>> typeMatcher,
                         TypeConverter typeConverter) {
        this.source = Objects.requireNonNull(source, "source");
        this.typeMatcher = Objects.requireNonNull(typeMatcher, "typeMatcher");
        this.typeConverter = Objects.requireNonNull(typeConverter, "typeConverter");
    }

    @Override
    public Object getSource() {
        return source;
    }

    public Matcher<? super TypeLiteral<?>> getTypeMatcher() {
        return typeMatcher;
    }

    public TypeConverter getTypeConverter() {
        return typeConverter;
    }

    @Override
    public <T> T acceptVisitor(ElementVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public void applyTo(Binder binder) {
        binder.withSource(getSource()).convertToTypes(typeMatcher, typeConverter);
    }
}

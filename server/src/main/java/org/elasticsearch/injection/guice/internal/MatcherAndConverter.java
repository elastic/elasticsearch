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

package org.elasticsearch.injection.guice.internal;

import org.elasticsearch.injection.guice.TypeLiteral;
import org.elasticsearch.injection.guice.matcher.Matcher;
import org.elasticsearch.injection.guice.spi.TypeConverter;

import java.util.Objects;

/**
 * @author crazybob@google.com (Bob Lee)
 */
public final class MatcherAndConverter {

    private final Matcher<? super TypeLiteral<?>> typeMatcher;
    private final TypeConverter typeConverter;
    private final Object source;

    public MatcherAndConverter(Matcher<? super TypeLiteral<?>> typeMatcher, TypeConverter typeConverter, Object source) {
        this.typeMatcher = Objects.requireNonNull(typeMatcher, "type matcher");
        this.typeConverter = Objects.requireNonNull(typeConverter, "converter");
        this.source = source;
    }

    public TypeConverter getTypeConverter() {
        return typeConverter;
    }

    public Matcher<? super TypeLiteral<?>> getTypeMatcher() {
        return typeMatcher;
    }

    @Override
    public String toString() {
        return typeConverter + " which matches " + typeMatcher + " (bound at " + source + ")";
    }
}

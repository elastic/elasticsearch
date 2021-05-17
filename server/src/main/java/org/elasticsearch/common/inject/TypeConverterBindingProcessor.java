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

import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.MatcherAndConverter;
import org.elasticsearch.common.inject.internal.SourceProvider;
import org.elasticsearch.common.inject.internal.Strings;
import org.elasticsearch.common.inject.matcher.AbstractMatcher;
import org.elasticsearch.common.inject.matcher.Matcher;
import org.elasticsearch.common.inject.matcher.Matchers;
import org.elasticsearch.common.inject.spi.TypeConverter;
import org.elasticsearch.common.inject.spi.TypeConverterBinding;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

/**
 * Handles {@link Binder#convertToTypes} commands.
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 */
class TypeConverterBindingProcessor extends AbstractProcessor {

    TypeConverterBindingProcessor(Errors errors) {
        super(errors);
    }

    /**
     * Installs default converters for primitives, enums, and class literals.
     */
    public void prepareBuiltInConverters(InjectorImpl injector) {
        this.injector = injector;
        try {
            // Configure type converters.
            convertToPrimitiveType(int.class, Integer.class);
            convertToPrimitiveType(long.class, Long.class);
            convertToPrimitiveType(boolean.class, Boolean.class);
            convertToPrimitiveType(byte.class, Byte.class);
            convertToPrimitiveType(short.class, Short.class);
            convertToPrimitiveType(float.class, Float.class);
            convertToPrimitiveType(double.class, Double.class);

            convertToClass(Character.class, new TypeConverter() {
                @Override
                public Object convert(String value, TypeLiteral<?> toType) {
                    value = value.trim();
                    if (value.length() != 1) {
                        throw new RuntimeException("Length != 1.");
                    }
                    return value.charAt(0);
                }

                @Override
                public String toString() {
                    return "TypeConverter<Character>";
                }
            });

            convertToClasses(Matchers.subclassesOf(Enum.class), new TypeConverter() {
                @Override
                @SuppressWarnings("unchecked")
                public Object convert(String value, TypeLiteral<?> toType) {
                    return Enum.valueOf((Class) toType.getRawType(), value);
                }

                @Override
                public String toString() {
                    return "TypeConverter<E extends Enum<E>>";
                }
            });

            internalConvertToTypes(
                    new AbstractMatcher<TypeLiteral<?>>() {
                        @Override
                        public boolean matches(TypeLiteral<?> typeLiteral) {
                            return typeLiteral.getRawType() == Class.class;
                        }

                        @Override
                        public String toString() {
                            return "Class<?>";
                        }
                    },
                    new TypeConverter() {
                        @Override
                        public Object convert(String value, TypeLiteral<?> toType) {
                            try {
                                return Class.forName(value);
                            } catch (ClassNotFoundException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        public String toString() {
                            return "TypeConverter<Class<?>>";
                        }
                    }
            );
        } finally {
            this.injector = null;
        }
    }

    private <T> void convertToPrimitiveType(Class<T> primitiveType, final Class<T> wrapperType) {
        try {
            final Method parser = wrapperType.getMethod(
                    "parse" + Strings.capitalize(primitiveType.getName()), String.class);

            TypeConverter typeConverter = new TypeConverter() {
                @Override
                public Object convert(String value, TypeLiteral<?> toType) {
                    try {
                        return parser.invoke(null, value);
                    } catch (IllegalAccessException e) {
                        throw new AssertionError(e);
                    } catch (InvocationTargetException e) {
                        throw new RuntimeException(e.getTargetException());
                    }
                }

                @Override
                public String toString() {
                    return "TypeConverter<" + wrapperType.getSimpleName() + ">";
                }
            };

            convertToClass(wrapperType, typeConverter);
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    private <T> void convertToClass(Class<T> type, TypeConverter converter) {
        convertToClasses(Matchers.identicalTo(type), converter);
    }

    private void convertToClasses(final Matcher<? super Class<?>> typeMatcher,
                                  TypeConverter converter) {
        internalConvertToTypes(new AbstractMatcher<TypeLiteral<?>>() {
            @Override
            public boolean matches(TypeLiteral<?> typeLiteral) {
                Type type = typeLiteral.getType();
                if ((type instanceof Class) == false) {
                    return false;
                }
                Class<?> clazz = (Class<?>) type;
                return typeMatcher.matches(clazz);
            }

            @Override
            public String toString() {
                return typeMatcher.toString();
            }
        }, converter);
    }

    private void internalConvertToTypes(Matcher<? super TypeLiteral<?>> typeMatcher,
                                        TypeConverter converter) {
        injector.state.addConverter(
                new MatcherAndConverter(typeMatcher, converter, SourceProvider.UNKNOWN_SOURCE));
    }

    @Override
    public Boolean visit(TypeConverterBinding command) {
        injector.state.addConverter(new MatcherAndConverter(
                command.getTypeMatcher(), command.getTypeConverter(), command.getSource()));
        return true;
    }
}

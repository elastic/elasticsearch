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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.ConfigurationException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.internal.*;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;

import static org.elasticsearch.common.inject.internal.MoreTypes.getRawType;

/**
 * A constructor, field or method that can receive injections. Typically this is a member with the
 * {@literal @}{@link Inject} annotation. For non-private, no argument constructors, the member may
 * omit the annotation.
 *
 * @author crazybob@google.com (Bob Lee)
 * @since 2.0
 */
public final class InjectionPoint {

    private final boolean optional;
    private final Member member;
    private final ImmutableList<Dependency<?>> dependencies;

    private InjectionPoint(Member member,
                           ImmutableList<Dependency<?>> dependencies, boolean optional) {
        this.member = member;
        this.dependencies = dependencies;
        this.optional = optional;
    }

    InjectionPoint(TypeLiteral<?> type, Method method) {
        this.member = method;

        Inject inject = method.getAnnotation(Inject.class);
        this.optional = inject.optional();

        this.dependencies = forMember(method, type, method.getParameterAnnotations());
    }

    InjectionPoint(TypeLiteral<?> type, Constructor<?> constructor) {
        this.member = constructor;
        this.optional = false;
        this.dependencies = forMember(constructor, type, constructor.getParameterAnnotations());
    }

    InjectionPoint(TypeLiteral<?> type, Field field) {
        this.member = field;

        Inject inject = field.getAnnotation(Inject.class);
        this.optional = inject.optional();

        Annotation[] annotations = field.getAnnotations();

        Errors errors = new Errors(field);
        Key<?> key = null;
        try {
            key = Annotations.getKey(type.getFieldType(field), field, annotations, errors);
        } catch (ErrorsException e) {
            errors.merge(e.getErrors());
        }
        errors.throwConfigurationExceptionIfErrorsExist();

        this.dependencies = ImmutableList.<Dependency<?>>of(
                newDependency(key, Nullability.allowsNull(annotations), -1));
    }

    private ImmutableList<Dependency<?>> forMember(Member member, TypeLiteral<?> type,
                                                   Annotation[][] parameterAnnotations) {
        Errors errors = new Errors(member);
        Iterator<Annotation[]> annotationsIterator = Arrays.asList(parameterAnnotations).iterator();

        List<Dependency<?>> dependencies = Lists.newArrayList();
        int index = 0;

        for (TypeLiteral<?> parameterType : type.getParameterTypes(member)) {
            try {
                Annotation[] paramAnnotations = annotationsIterator.next();
                Key<?> key = Annotations.getKey(parameterType, member, paramAnnotations, errors);
                dependencies.add(newDependency(key, Nullability.allowsNull(paramAnnotations), index));
                index++;
            } catch (ErrorsException e) {
                errors.merge(e.getErrors());
            }
        }

        errors.throwConfigurationExceptionIfErrorsExist();
        return ImmutableList.copyOf(dependencies);
    }

    // This metohd is necessary to create a Dependency<T> with proper generic type information
    private <T> Dependency<T> newDependency(Key<T> key, boolean allowsNull, int parameterIndex) {
        return new Dependency<>(this, key, allowsNull, parameterIndex);
    }

    /**
     * Returns the injected constructor, field, or method.
     */
    public Member getMember() {
        return member;
    }

    /**
     * Returns the dependencies for this injection point. If the injection point is for a method or
     * constructor, the dependencies will correspond to that member's parameters. Field injection
     * points always have a single dependency for the field itself.
     *
     * @return a possibly-empty list
     */
    public List<Dependency<?>> getDependencies() {
        return dependencies;
    }

    /**
     * Returns true if this injection point shall be skipped if the injector cannot resolve bindings
     * for all required dependencies. Both explicit bindings (as specified in a module), and implicit
     * bindings ({@literal @}{@link org.elasticsearch.common.inject.ImplementedBy ImplementedBy}, default
     * constructors etc.) may be used to satisfy optional injection points.
     */
    public boolean isOptional() {
        return optional;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof InjectionPoint
                && member.equals(((InjectionPoint) o).member);
    }

    @Override
    public int hashCode() {
        return member.hashCode();
    }

    @Override
    public String toString() {
        return MoreTypes.toString(member);
    }

    /**
     * Returns a new injection point for the injectable constructor of {@code type}.
     *
     * @param type a concrete type with exactly one constructor annotated {@literal @}{@link Inject},
     *             or a no-arguments constructor that is not private.
     * @throws ConfigurationException if there is no injectable constructor, more than one injectable
     *                                constructor, or if parameters of the injectable constructor are malformed, such as a
     *                                parameter with multiple binding annotations.
     */
    public static InjectionPoint forConstructorOf(TypeLiteral<?> type) {
        Class<?> rawType = getRawType(type.getType());
        Errors errors = new Errors(rawType);

        Constructor<?> injectableConstructor = null;
        for (Constructor<?> constructor : rawType.getDeclaredConstructors()) {
            Inject inject = constructor.getAnnotation(Inject.class);
            if (inject != null) {
                if (inject.optional()) {
                    errors.optionalConstructor(constructor);
                }

                if (injectableConstructor != null) {
                    errors.tooManyConstructors(rawType);
                }

                injectableConstructor = constructor;
                checkForMisplacedBindingAnnotations(injectableConstructor, errors);
            }
        }

        errors.throwConfigurationExceptionIfErrorsExist();

        if (injectableConstructor != null) {
            return new InjectionPoint(type, injectableConstructor);
        }

        // If no annotated constructor is found, look for a no-arg constructor instead.
        try {
            Constructor<?> noArgConstructor = rawType.getDeclaredConstructor();

            // Disallow private constructors on non-private classes (unless they have @Inject)
            if (Modifier.isPrivate(noArgConstructor.getModifiers())
                    && !Modifier.isPrivate(rawType.getModifiers())) {
                errors.missingConstructor(rawType);
                throw new ConfigurationException(errors.getMessages());
            }

            checkForMisplacedBindingAnnotations(noArgConstructor, errors);
            return new InjectionPoint(type, noArgConstructor);
        } catch (NoSuchMethodException e) {
            errors.missingConstructor(rawType);
            throw new ConfigurationException(errors.getMessages());
        }
    }

    /**
     * Returns a new injection point for the injectable constructor of {@code type}.
     *
     * @param type a concrete type with exactly one constructor annotated {@literal @}{@link Inject},
     *             or a no-arguments constructor that is not private.
     * @throws ConfigurationException if there is no injectable constructor, more than one injectable
     *                                constructor, or if parameters of the injectable constructor are malformed, such as a
     *                                parameter with multiple binding annotations.
     */
    public static InjectionPoint forConstructorOf(Class<?> type) {
        return forConstructorOf(TypeLiteral.get(type));
    }

    /**
     * Returns all static method and field injection points on {@code type}.
     *
     * @return a possibly empty set of injection points. The set has a specified iteration order. All
     *         fields are returned and then all methods. Within the fields, supertype fields are returned
     *         before subtype fields. Similarly, supertype methods are returned before subtype methods.
     * @throws ConfigurationException if there is a malformed injection point on {@code type}, such as
     *                                a field with multiple binding annotations. The exception's {@link
     *                                ConfigurationException#getPartialValue() partial value} is a {@code Set<InjectionPoint>}
     *                                of the valid injection points.
     */
    public static Set<InjectionPoint> forStaticMethodsAndFields(TypeLiteral type) {
        List<InjectionPoint> sink = Lists.newArrayList();
        Errors errors = new Errors();

        addInjectionPoints(type, Factory.FIELDS, true, sink, errors);
        addInjectionPoints(type, Factory.METHODS, true, sink, errors);

        ImmutableSet<InjectionPoint> result = ImmutableSet.copyOf(sink);
        if (errors.hasErrors()) {
            throw new ConfigurationException(errors.getMessages()).withPartialValue(result);
        }
        return result;
    }

    /**
     * Returns all static method and field injection points on {@code type}.
     *
     * @return a possibly empty set of injection points. The set has a specified iteration order. All
     *         fields are returned and then all methods. Within the fields, supertype fields are returned
     *         before subtype fields. Similarly, supertype methods are returned before subtype methods.
     * @throws ConfigurationException if there is a malformed injection point on {@code type}, such as
     *                                a field with multiple binding annotations. The exception's {@link
     *                                ConfigurationException#getPartialValue() partial value} is a {@code Set<InjectionPoint>}
     *                                of the valid injection points.
     */
    public static Set<InjectionPoint> forStaticMethodsAndFields(Class<?> type) {
        return forStaticMethodsAndFields(TypeLiteral.get(type));
    }

    /**
     * Returns all instance method and field injection points on {@code type}.
     *
     * @return a possibly empty set of injection points. The set has a specified iteration order. All
     *         fields are returned and then all methods. Within the fields, supertype fields are returned
     *         before subtype fields. Similarly, supertype methods are returned before subtype methods.
     * @throws ConfigurationException if there is a malformed injection point on {@code type}, such as
     *                                a field with multiple binding annotations. The exception's {@link
     *                                ConfigurationException#getPartialValue() partial value} is a {@code Set<InjectionPoint>}
     *                                of the valid injection points.
     */
    public static Set<InjectionPoint> forInstanceMethodsAndFields(TypeLiteral<?> type) {
        List<InjectionPoint> sink = Lists.newArrayList();
        Errors errors = new Errors();

        // TODO (crazybob): Filter out overridden members.
        addInjectionPoints(type, Factory.FIELDS, false, sink, errors);
        addInjectionPoints(type, Factory.METHODS, false, sink, errors);

        ImmutableSet<InjectionPoint> result = ImmutableSet.copyOf(sink);
        if (errors.hasErrors()) {
            throw new ConfigurationException(errors.getMessages()).withPartialValue(result);
        }
        return result;
    }

    /**
     * Returns all instance method and field injection points on {@code type}.
     *
     * @return a possibly empty set of injection points. The set has a specified iteration order. All
     *         fields are returned and then all methods. Within the fields, supertype fields are returned
     *         before subtype fields. Similarly, supertype methods are returned before subtype methods.
     * @throws ConfigurationException if there is a malformed injection point on {@code type}, such as
     *                                a field with multiple binding annotations. The exception's {@link
     *                                ConfigurationException#getPartialValue() partial value} is a {@code Set<InjectionPoint>}
     *                                of the valid injection points.
     */
    public static Set<InjectionPoint> forInstanceMethodsAndFields(Class<?> type) {
        return forInstanceMethodsAndFields(TypeLiteral.get(type));
    }

    private static void checkForMisplacedBindingAnnotations(Member member, Errors errors) {
        Annotation misplacedBindingAnnotation = Annotations.findBindingAnnotation(
                errors, member, ((AnnotatedElement) member).getAnnotations());
        if (misplacedBindingAnnotation == null) {
            return;
        }

        // don't warn about misplaced binding annotations on methods when there's a field with the same
        // name. In Scala, fields always get accessor methods (that we need to ignore). See bug 242.
        if (member instanceof Method) {
            try {
                if (member.getDeclaringClass().getDeclaredField(member.getName()) != null) {
                    return;
                }
            } catch (NoSuchFieldException ignore) {
            }
        }

        errors.misplacedBindingAnnotation(member, misplacedBindingAnnotation);
    }

    private static <M extends Member & AnnotatedElement> void addInjectionPoints(TypeLiteral<?> type,
                                                                                 Factory<M> factory, boolean statics, Collection<InjectionPoint> injectionPoints,
                                                                                 Errors errors) {
        if (type.getType() == Object.class) {
            return;
        }

        // Add injectors for superclass first.
        TypeLiteral<?> superType = type.getSupertype(type.getRawType().getSuperclass());
        addInjectionPoints(superType, factory, statics, injectionPoints, errors);

        // Add injectors for all members next
        addInjectorsForMembers(type, factory, statics, injectionPoints, errors);
    }

    private static <M extends Member & AnnotatedElement> void addInjectorsForMembers(
            TypeLiteral<?> typeLiteral, Factory<M> factory, boolean statics,
            Collection<InjectionPoint> injectionPoints, Errors errors) {
        for (M member : factory.getMembers(getRawType(typeLiteral.getType()))) {
            if (isStatic(member) != statics) {
                continue;
            }

            Inject inject = member.getAnnotation(Inject.class);
            if (inject == null) {
                continue;
            }

            try {
                injectionPoints.add(factory.create(typeLiteral, member, errors));
            } catch (ConfigurationException ignorable) {
                if (!inject.optional()) {
                    errors.merge(ignorable.getErrorMessages());
                }
            }
        }
    }

    private static boolean isStatic(Member member) {
        return Modifier.isStatic(member.getModifiers());
    }

    private interface Factory<M extends Member & AnnotatedElement> {
        Factory<Field> FIELDS = new Factory<Field>() {
            @Override
            public Field[] getMembers(Class<?> type) {
                return type.getDeclaredFields();
            }

            @Override
            public InjectionPoint create(TypeLiteral<?> typeLiteral, Field member, Errors errors) {
                return new InjectionPoint(typeLiteral, member);
            }
        };

        Factory<Method> METHODS = new Factory<Method>() {
            @Override
            public Method[] getMembers(Class<?> type) {
                return type.getDeclaredMethods();
            }

            @Override
            public InjectionPoint create(TypeLiteral<?> typeLiteral, Method member, Errors errors) {
                checkForMisplacedBindingAnnotations(member, errors);
                return new InjectionPoint(typeLiteral, member);
            }
        };

        M[] getMembers(Class<?> type);

        InjectionPoint create(TypeLiteral<?> typeLiteral, M member, Errors errors);
    }
}

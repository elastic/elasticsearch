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

package org.elasticsearch.injection.guice.spi;

import org.elasticsearch.injection.guice.ConfigurationException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.injection.guice.Key;
import org.elasticsearch.injection.guice.TypeLiteral;
import org.elasticsearch.injection.guice.internal.Annotations;
import org.elasticsearch.injection.guice.internal.Errors;
import org.elasticsearch.injection.guice.internal.ErrorsException;
import org.elasticsearch.injection.guice.internal.MoreTypes;
import org.elasticsearch.injection.guice.internal.Nullability;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/**
 * A constructor, field or method that can receive injections. Typically this is a member with the
 * {@literal @}{@link Inject} annotation. For non-private, no argument constructors, the member may
 * omit the annotation.
 *
 * @author crazybob@google.com (Bob Lee)
 * @since 2.0
 */
@SuppressWarnings("rawtypes")
public final class InjectionPoint {

    private final boolean optional;
    private final Executable member;
    private final List<Dependency<?>> dependencies;

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

    private List<Dependency<?>> forMember(Member member, TypeLiteral<?> type, Annotation[][] parameterAnnotations) {
        Errors errors = new Errors(member);
        Iterator<Annotation[]> annotationsIterator = Arrays.asList(parameterAnnotations).iterator();

        List<Dependency<?>> dependencies = new ArrayList<>();
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
        return Collections.unmodifiableList(dependencies);
    }

    // This method is necessary to create a Dependency<T> with proper generic type information
    private <T> Dependency<T> newDependency(Key<T> key, boolean allowsNull, int parameterIndex) {
        return new Dependency<>(this, key, allowsNull, parameterIndex);
    }

    /**
     * Returns the injected constructor, field, or method.
     */
    public Executable getMember() {
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
     * bindings by default constructors etc.) may be used to satisfy optional injection points.
     */
    public boolean isOptional() {
        return optional;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof InjectionPoint && member.equals(((InjectionPoint) o).member);
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
        Class<?> rawType = MoreTypes.getRawType(type.getType());
        Errors errors = new Errors(rawType);

        Constructor<?> injectableConstructor = null;
        for (Constructor<?> constructor : rawType.getConstructors()) {
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
            Constructor<?> noArgConstructor = rawType.getConstructor();

            // Disallow private constructors on non-private classes (unless they have @Inject)
            if (Modifier.isPrivate(noArgConstructor.getModifiers()) && Modifier.isPrivate(rawType.getModifiers()) == false) {
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
    public static Set<InjectionPoint> forInstanceMethods(TypeLiteral<?> type) {
        Set<InjectionPoint> result = new HashSet<>();
        Errors errors = new Errors();

        // TODO (crazybob): Filter out overridden members.
        addInjectionPoints(type, false, result, errors);

        result = unmodifiableSet(result);
        if (errors.hasErrors()) {
            throw new ConfigurationException(errors.getMessages()).withPartialValue(result);
        }
        return result;
    }

    /**
     * Returns all instance method injection points on {@code type}.
     *
     * @return a possibly empty set of injection points. The set has a specified iteration order. All
     *         fields are returned and then all methods. Within the fields, supertype fields are returned
     *         before subtype fields. Similarly, supertype methods are returned before subtype methods.
     * @throws ConfigurationException if there is a malformed injection point on {@code type}, such as
     *                                a field with multiple binding annotations. The exception's {@link
     *                                ConfigurationException#getPartialValue() partial value} is a {@code Set<InjectionPoint>}
     *                                of the valid injection points.
     */
    public static Set<InjectionPoint> forInstanceMethods(Class<?> type) {
        return forInstanceMethods(TypeLiteral.get(type));
    }

    private static void checkForMisplacedBindingAnnotations(Member member, Errors errors) {
        Annotation misplacedBindingAnnotation = Annotations.findBindingAnnotation(
            errors,
            member,
            ((AnnotatedElement) member).getAnnotations()
        );
        if (misplacedBindingAnnotation == null) {
            return;
        }

        // don't warn about misplaced binding annotations on methods when there's a field with the same
        // name. In Scala, fields always get accessor methods (that we need to ignore). See bug 242.
        if (member instanceof Method) {
            try {
                member.getDeclaringClass().getField(member.getName());
                return;
            } catch (NoSuchFieldException ignore) {}
        }

        errors.misplacedBindingAnnotation(member, misplacedBindingAnnotation);
    }

    private static void addInjectionPoints(
        TypeLiteral<?> type,
        boolean statics,
        Collection<InjectionPoint> injectionPoints,
        Errors errors
    ) {
        if (type.getType() == Object.class) {
            return;
        }

        // Add injectors for superclass first.
        TypeLiteral<?> superType = type.getSupertype(type.getRawType().getSuperclass());
        addInjectionPoints(superType, statics, injectionPoints, errors);

        // Add injectors for all members next
        addInjectorsForMembers(type, statics, injectionPoints, errors);
    }

    private static void addInjectorsForMembers(
        TypeLiteral<?> typeLiteral,
        boolean statics,
        Collection<InjectionPoint> injectionPoints,
        Errors errors
    ) {
        for (Method member : MoreTypes.getRawType(typeLiteral.getType()).getMethods()) {
            if (isStatic(member) != statics) {
                continue;
            }

            Inject inject = member.getAnnotation(Inject.class);
            if (inject == null) {
                continue;
            }

            try {
                checkForMisplacedBindingAnnotations(member, errors);
                injectionPoints.add(new InjectionPoint(typeLiteral, member));
            } catch (ConfigurationException ignorable) {
                if (inject.optional() == false) {
                    errors.merge(ignorable.getErrorMessages());
                }
            }
        }
    }

    private static boolean isStatic(Member member) {
        return Modifier.isStatic(member.getModifiers());
    }

}

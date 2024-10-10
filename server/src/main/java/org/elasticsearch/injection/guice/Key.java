/*
 * Copyright (C) 2006 Google Inc.
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

package org.elasticsearch.injection.guice;

import org.elasticsearch.injection.guice.internal.Annotations;
import org.elasticsearch.injection.guice.internal.MoreTypes;
import org.elasticsearch.injection.guice.internal.ToStringBuilder;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Objects;

/**
 * Binding key consisting of an injection type and an optional annotation.
 * Matches the type and annotation at a point of injection.
 * <p>
 * For example, {@code Key.get(Service.class, Transactional.class)} will
 * match:
 * <pre>
 *   {@literal @}Inject
 *   public void setService({@literal @}Transactional Service service) {
 *     ...
 *   }
 * </pre>
 * <p>
 * {@code Key} supports generic types via subclassing just like {@link
 * TypeLiteral}.
 * <p>
 * Keys do not differentiate between primitive types (int, char, etc.) and
 * their corresponding wrapper types (Integer, Character, etc.). Primitive
 * types will be replaced with their wrapper types when keys are created.
 *
 * @author crazybob@google.com (Bob Lee)
 */
public class Key<T> {

    private final AnnotationStrategy annotationStrategy;

    private final TypeLiteral<T> typeLiteral;
    private final int hashCode;

    /**
     * Constructs a new key. Derives the type from this class's type parameter.
     * <p>
     * Clients create an empty anonymous subclass. Doing so embeds the type
     * parameter in the anonymous class's type hierarchy so we can reconstitute it
     * at runtime despite erasure.
     * <p>
     * Example usage for a binding of type {@code Foo}:
     * <p>
     * {@code new Key<Foo>() {}}.
     */
    @SuppressWarnings("unchecked")
    protected Key() {
        this.annotationStrategy = NullAnnotationStrategy.INSTANCE;
        this.typeLiteral = (TypeLiteral<T>) TypeLiteral.fromSuperclassTypeParameter(getClass());
        this.hashCode = computeHashCode();
    }

    /**
     * Unsafe. Constructs a key from a manually specified type.
     */
    @SuppressWarnings("unchecked")
    private Key(Type type, AnnotationStrategy annotationStrategy) {
        this.annotationStrategy = annotationStrategy;
        this.typeLiteral = MoreTypes.makeKeySafe((TypeLiteral<T>) TypeLiteral.get(type));
        this.hashCode = computeHashCode();
    }

    /**
     * Constructs a key from a manually specified type.
     */
    private Key(TypeLiteral<T> typeLiteral, AnnotationStrategy annotationStrategy) {
        this.annotationStrategy = annotationStrategy;
        this.typeLiteral = MoreTypes.makeKeySafe(typeLiteral);
        this.hashCode = computeHashCode();
    }

    private int computeHashCode() {
        return typeLiteral.hashCode() * 31 + annotationStrategy.hashCode();
    }

    /**
     * Gets the key type.
     */
    public final TypeLiteral<T> getTypeLiteral() {
        return typeLiteral;
    }

    /**
     * Gets the annotation type.
     */
    public final Class<? extends Annotation> getAnnotationType() {
        return annotationStrategy.getAnnotationType();
    }

    /**
     * Gets the annotation.
     */
    public final Annotation getAnnotation() {
        return annotationStrategy.getAnnotation();
    }

    boolean hasAnnotationType() {
        return annotationStrategy.getAnnotationType() != null;
    }

    Class<? super T> getRawType() {
        return typeLiteral.getRawType();
    }

    @Override
    public final boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if ((o instanceof Key<?>) == false) {
            return false;
        }
        Key<?> other = (Key<?>) o;
        return annotationStrategy.equals(other.annotationStrategy) && typeLiteral.equals(other.typeLiteral);
    }

    @Override
    public final int hashCode() {
        return this.hashCode;
    }

    @Override
    public final String toString() {
        return new ToStringBuilder(Key.class).add("type", typeLiteral).add("annotation", annotationStrategy).toString();
    }

    /**
     * Gets a key for an injection type.
     */
    public static <T> Key<T> get(Class<T> type) {
        return new Key<>(type, NullAnnotationStrategy.INSTANCE);
    }

    /**
     * Gets a key for an injection type.
     */
    public static <T> Key<T> get(TypeLiteral<T> typeLiteral) {
        return new Key<>(typeLiteral, NullAnnotationStrategy.INSTANCE);
    }

    /**
     * Gets a key for an injection type and an annotation.
     */
    public static <T> Key<T> get(TypeLiteral<T> typeLiteral, Annotation annotation) {
        return new Key<>(typeLiteral, strategyFor(annotation));
    }

    /**
     * Returns a new key of the {@link String} type with the same annotation as this
     * key.
     */
    Key<String> ofStringType() {
        return new Key<>(String.class, annotationStrategy);
    }

    /**
     * Returns a new key of the specified type with the same annotation as this
     * key.
     */
    Key<?> ofType(Type type) {
        return new Key<>(type, annotationStrategy);
    }

    /**
     * Returns true if this key has annotation attributes.
     */
    boolean hasAttributes() {
        return annotationStrategy.hasAttributes();
    }

    /**
     * Returns this key without annotation attributes, i.e. with only the
     * annotation type.
     */
    Key<T> withoutAttributes() {
        return new Key<>(typeLiteral, annotationStrategy.withoutAttributes());
    }

    interface AnnotationStrategy {
        Annotation getAnnotation();

        Class<? extends Annotation> getAnnotationType();

        boolean hasAttributes();

        AnnotationStrategy withoutAttributes();
    }

    /**
     * Gets the strategy for an annotation.
     */
    static AnnotationStrategy strategyFor(Annotation annotation) {
        Objects.requireNonNull(annotation, "annotation");
        Class<? extends Annotation> annotationType = annotation.annotationType();
        ensureRetainedAtRuntime(annotationType);
        ensureIsBindingAnnotation(annotationType);

        if (annotationType.getMethods().length == 0) {
            return new AnnotationTypeStrategy(annotationType, annotation);
        }

        return new AnnotationInstanceStrategy(annotation);
    }

    private static void ensureRetainedAtRuntime(Class<? extends Annotation> annotationType) {
        if (Annotations.isRetainedAtRuntime(annotationType) == false) {
            throw new IllegalArgumentException(
                annotationType.getName() + " is not retained at runtime. Please annotate it with @Retention(RUNTIME)."
            );
        }
    }

    private static void ensureIsBindingAnnotation(Class<? extends Annotation> annotationType) {
        if (isBindingAnnotation(annotationType) == false) {
            throw new IllegalArgumentException(
                annotationType.getName() + " is not a binding annotation. Please annotate it with @BindingAnnotation."
            );
        }
    }

    enum NullAnnotationStrategy implements AnnotationStrategy {
        INSTANCE;

        @Override
        public boolean hasAttributes() {
            return false;
        }

        @Override
        public AnnotationStrategy withoutAttributes() {
            throw new UnsupportedOperationException("Key already has no attributes.");
        }

        @Override
        public Annotation getAnnotation() {
            return null;
        }

        @Override
        public Class<? extends Annotation> getAnnotationType() {
            return null;
        }

        @Override
        public String toString() {
            return "[none]";
        }
    }

    // this class not test-covered
    static class AnnotationInstanceStrategy implements AnnotationStrategy {

        final Annotation annotation;

        AnnotationInstanceStrategy(Annotation annotation) {
            this.annotation = Objects.requireNonNull(annotation, "annotation");
        }

        @Override
        public boolean hasAttributes() {
            return true;
        }

        @Override
        public AnnotationStrategy withoutAttributes() {
            return new AnnotationTypeStrategy(getAnnotationType(), annotation);
        }

        @Override
        public Annotation getAnnotation() {
            return annotation;
        }

        @Override
        public Class<? extends Annotation> getAnnotationType() {
            return annotation.annotationType();
        }

        @Override
        public boolean equals(Object o) {
            if ((o instanceof AnnotationInstanceStrategy) == false) {
                return false;
            }

            AnnotationInstanceStrategy other = (AnnotationInstanceStrategy) o;
            return annotation.equals(other.annotation);
        }

        @Override
        public int hashCode() {
            return annotation.hashCode();
        }

        @Override
        public String toString() {
            return annotation.toString();
        }
    }

    static class AnnotationTypeStrategy implements AnnotationStrategy {

        final Class<? extends Annotation> annotationType;

        // Keep the instance around if we have it so the client can request it.
        final Annotation annotation;

        AnnotationTypeStrategy(Class<? extends Annotation> annotationType, Annotation annotation) {
            this.annotationType = Objects.requireNonNull(annotationType, "annotation type");
            this.annotation = annotation;
        }

        @Override
        public boolean hasAttributes() {
            return false;
        }

        @Override
        public AnnotationStrategy withoutAttributes() {
            throw new UnsupportedOperationException("Key already has no attributes.");
        }

        @Override
        public Annotation getAnnotation() {
            return annotation;
        }

        @Override
        public Class<? extends Annotation> getAnnotationType() {
            return annotationType;
        }

        @Override
        public boolean equals(Object o) {
            if ((o instanceof AnnotationTypeStrategy) == false) {
                return false;
            }

            AnnotationTypeStrategy other = (AnnotationTypeStrategy) o;
            return annotationType.equals(other.annotationType);
        }

        @Override
        public int hashCode() {
            return annotationType.hashCode();
        }

        @Override
        public String toString() {
            return "@" + annotationType.getName();
        }
    }

    static boolean isBindingAnnotation(Class<? extends Annotation> annotationType) {
        return annotationType.getAnnotation(BindingAnnotation.class) != null;
    }
}

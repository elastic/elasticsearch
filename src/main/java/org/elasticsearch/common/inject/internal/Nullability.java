package org.elasticsearch.common.inject.internal;

import java.lang.annotation.Annotation;

/**
 * Whether a member supports null values injected.
 * <p/>
 * <p>Support for {@code Nullable} annotations in Guice is loose.
 * Any annotation type whose simplename is "Nullable" is sufficient to indicate
 * support for null values injected.
 * <p/>
 * <p>This allows support for JSR-305's
 * <a href="http://groups.google.com/group/jsr-305/web/proposed-annotations">
 * javax.annotation.meta.Nullable</a> annotation and IntelliJ IDEA's
 * <a href="http://www.jetbrains.com/idea/documentation/howto.html">
 * org.jetbrains.annotations.Nullable</a>.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
public class Nullability {
    private Nullability() {
    }

    public static boolean allowsNull(Annotation[] annotations) {
        for (Annotation a : annotations) {
            if ("Nullable".equals(a.annotationType().getSimpleName())) {
                return true;
            }
        }
        return false;
    }
}

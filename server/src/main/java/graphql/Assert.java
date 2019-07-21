package graphql;

import java.util.Collection;

import static java.lang.String.format;

@SuppressWarnings("TypeParameterUnusedInFormals")
@Internal
public class Assert {

    public static <T> T assertNotNull(T object, String format, Object... args) {
        if (object != null) {
            return object;
        }
        throw new AssertException(format(format, args));
    }

    public static <T> T assertNotNullWithNPE(T object, String format, Object... args) {
        if (object != null) {
            return object;
        }
        throw new NullPointerException(format(format, args));
    }

    public static <T> T assertNotNull(T object) {
        if (object != null) {
            return object;
        }
        throw new AssertException("Object required to be not null");
    }

    public static <T> void assertNull(T object, String format, Object... args) {
        if (object == null) {
            return;
        }
        throw new AssertException(format(format, args));
    }

    public static <T> void assertNull(T object) {
        if (object == null) {
            return;
        }
        throw new AssertException("Object required to be null");
    }

    public static <T> T assertNeverCalled() {
        throw new AssertException("Should never been called");
    }

    public static <T> T assertShouldNeverHappen(String format, Object... args) {
        throw new AssertException("Internal error: should never happen: " + format(format, args));
    }

    public static <T> T assertShouldNeverHappen() {
        throw new AssertException("Internal error: should never happen");
    }

    public static <T> Collection<T> assertNotEmpty(Collection<T> collection) {
        if (collection == null || collection.isEmpty()) {
            throw new AssertException("collection must be not null and not empty");
        }
        return collection;
    }

    public static <T> Collection<T> assertNotEmpty(Collection<T> collection, String format, Object... args) {
        if (collection == null || collection.isEmpty()) {
            throw new AssertException(format(format, args));
        }
        return collection;
    }

    public static void assertTrue(boolean condition, String format, Object... args) {
        if (condition) {
            return;
        }
        throw new AssertException(format(format, args));
    }

    public static void assertTrue(boolean condition) {
        if (condition) {
            return;
        }
        throw new AssertException("condition expected to be true");
    }

    public static void assertFalse(boolean condition, String format, Object... args) {
        if (!condition) {
            return;
        }
        throw new AssertException(format(format, args));
    }

    private static final String invalidNameErrorMessage = "Name must be non-null, non-empty and match [_A-Za-z][_0-9A-Za-z]* - was '%s'";

    /**
     * Validates that the Lexical token name matches the current spec.
     * currently non null, non empty,
     *
     * @param name - the name to be validated.
     *
     * @return the name if valid, or AssertException if invalid.
     */
    public static String assertValidName(String name) {
        if (name != null && !name.isEmpty() && name.matches("[_A-Za-z][_0-9A-Za-z]*")) {
            return name;
        }
        throw new AssertException(String.format(invalidNameErrorMessage, name));
    }

}

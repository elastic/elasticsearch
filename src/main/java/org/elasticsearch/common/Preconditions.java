/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ElasticsearchNullPointerException;

import java.util.NoSuchElementException;

/**
 * Simple static methods to be called at the start of your own methods to verify
 * correct arguments and state. This allows constructs such as
 * <pre>
 *     if (count <= 0) {
 *       throw new ElasticsearchIllegalArgumentException("must be positive: " + count);
 *     }</pre>
 *
 * to be replaced with the more compact
 * <pre>
 *     checkArgument(count > 0, "must be positive: %s", count);</pre>
 *
 * Note that the sense of the expression is inverted; with {@code Preconditions}
 * you declare what you expect to be <i>true</i>, just as you do with an
 * <a href="http://java.sun.com/j2se/1.5.0/docs/guide/language/assert.html">
 * {@code assert}</a> or a JUnit {@code assertTrue()} call.
 *
 * <p>Take care not to confuse precondition checking with other similar types
 * of checks! Precondition exceptions -- including those provided here, but also
 * {@link IndexOutOfBoundsException}, {@link NoSuchElementException}, {@link
 * UnsupportedOperationException} and others -- are used to signal that the
 * <i>calling method</i> has made an error. This tells the caller that it should
 * not have invoked the method when it did, with the arguments it did, or
 * perhaps <i>ever</i>. Postcondition or other invariant failures should not
 * throw these types of exceptions.
 *
 * <p><b>Note:</b> The methods of the {@code Preconditions} class are highly
 * unusual in one way: they are <i>supposed to</i> throw exceptions, and promise
 * in their specifications to do so even when given perfectly valid input. That
 * is, {@code null} is a valid parameter to the method {@link
 * #checkNotNull(Object)} -- and technically this parameter could be even marked
 * as Nullable -- yet the method will still throw an exception anyway,
 * because that's what its contract says to do.
 *
 *
 */
public final class Preconditions {
    private Preconditions() {
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the
     * calling method.
     *
     * @param expression a boolean expression
     * @throws org.elasticsearch.ElasticsearchIllegalArgumentException
     *          if {@code expression} is false
     */
    public static void checkArgument(boolean expression) {
        if (!expression) {
            throw new ElasticsearchIllegalArgumentException();
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the
     * calling method.
     *
     * @param expression   a boolean expression
     * @param errorMessage the exception message to use if the check fails; will
     *                     be converted to a string using {@link String#valueOf(Object)}
     * @throws org.elasticsearch.ElasticsearchIllegalArgumentException
     *          if {@code expression} is false
     */
    public static void checkArgument(boolean expression, Object errorMessage) {
        if (!expression) {
            throw new ElasticsearchIllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    /**
     * Ensures the truth of an expression involving one or more parameters to the
     * calling method.
     *
     * @param expression           a boolean expression
     * @param errorMessageTemplate a template for the exception message should the
     *                             check fail. The message is formed by replacing each {@code %s}
     *                             placeholder in the template with an argument. These are matched by
     *                             position - the first {@code %s} gets {@code errorMessageArgs[0]}, etc.
     *                             Unmatched arguments will be appended to the formatted message in square
     *                             braces. Unmatched placeholders will be left as-is.
     * @param errorMessageArgs     the arguments to be substituted into the message
     *                             template. Arguments are converted to strings using
     *                             {@link String#valueOf(Object)}.
     * @throws org.elasticsearch.ElasticsearchIllegalArgumentException
     *          if {@code expression} is false
     * @throws org.elasticsearch.ElasticsearchNullPointerException
     *          if the check fails and either {@code
     *          errorMessageTemplate} or {@code errorMessageArgs} is null (don't let
     *          this happen)
     */
    public static void checkArgument(boolean expression,
                                     String errorMessageTemplate, Object... errorMessageArgs) {
        if (!expression) {
            throw new ElasticsearchIllegalArgumentException(
                    format(errorMessageTemplate, errorMessageArgs));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling
     * instance, but not involving any parameters to the calling method.
     *
     * @param expression a boolean expression
     * @throws org.elasticsearch.ElasticsearchIllegalStateException
     *          if {@code expression} is false
     */
    public static void checkState(boolean expression) {
        if (!expression) {
            throw new ElasticsearchIllegalStateException();
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling
     * instance, but not involving any parameters to the calling method.
     *
     * @param expression   a boolean expression
     * @param errorMessage the exception message to use if the check fails; will
     *                     be converted to a string using {@link String#valueOf(Object)}
     * @throws org.elasticsearch.ElasticsearchIllegalStateException
     *          if {@code expression} is false
     */
    public static void checkState(boolean expression, Object errorMessage) {
        if (!expression) {
            throw new ElasticsearchIllegalStateException(String.valueOf(errorMessage));
        }
    }

    /**
     * Ensures the truth of an expression involving the state of the calling
     * instance, but not involving any parameters to the calling method.
     *
     * @param expression           a boolean expression
     * @param errorMessageTemplate a template for the exception message should the
     *                             check fail. The message is formed by replacing each {@code %s}
     *                             placeholder in the template with an argument. These are matched by
     *                             position - the first {@code %s} gets {@code errorMessageArgs[0]}, etc.
     *                             Unmatched arguments will be appended to the formatted message in square
     *                             braces. Unmatched placeholders will be left as-is.
     * @param errorMessageArgs     the arguments to be substituted into the message
     *                             template. Arguments are converted to strings using
     *                             {@link String#valueOf(Object)}.
     * @throws org.elasticsearch.ElasticsearchIllegalStateException
     *          if {@code expression} is false
     * @throws org.elasticsearch.ElasticsearchNullPointerException
     *          if the check fails and either {@code
     *          errorMessageTemplate} or {@code errorMessageArgs} is null (don't let
     *          this happen)
     */
    public static void checkState(boolean expression,
                                  String errorMessageTemplate, Object... errorMessageArgs) {
        if (!expression) {
            throw new ElasticsearchIllegalStateException(
                    format(errorMessageTemplate, errorMessageArgs));
        }
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling
     * method is not null.
     *
     * @param reference an object reference
     * @return the non-null reference that was validated
     * @throws org.elasticsearch.ElasticsearchNullPointerException
     *          if {@code reference} is null
     */
    public static <T> T checkNotNull(T reference) {
        if (reference == null) {
            throw new ElasticsearchNullPointerException();
        }
        return reference;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling
     * method is not null.
     *
     * @param reference    an object reference
     * @param errorMessage the exception message to use if the check fails; will
     *                     be converted to a string using {@link String#valueOf(Object)}
     * @return the non-null reference that was validated
     * @throws org.elasticsearch.ElasticsearchNullPointerException
     *          if {@code reference} is null
     */
    public static <T> T checkNotNull(T reference, Object errorMessage) {
        if (reference == null) {
            throw new ElasticsearchNullPointerException(String.valueOf(errorMessage));
        }
        return reference;
    }

    /**
     * Ensures that an object reference passed as a parameter to the calling
     * method is not null.
     *
     * @param reference            an object reference
     * @param errorMessageTemplate a template for the exception message should the
     *                             check fail. The message is formed by replacing each {@code %s}
     *                             placeholder in the template with an argument. These are matched by
     *                             position - the first {@code %s} gets {@code errorMessageArgs[0]}, etc.
     *                             Unmatched arguments will be appended to the formatted message in square
     *                             braces. Unmatched placeholders will be left as-is.
     * @param errorMessageArgs     the arguments to be substituted into the message
     *                             template. Arguments are converted to strings using
     *                             {@link String#valueOf(Object)}.
     * @return the non-null reference that was validated
     * @throws org.elasticsearch.ElasticsearchNullPointerException
     *          if {@code reference} is null
     */
    public static <T> T checkNotNull(T reference, String errorMessageTemplate,
                                     Object... errorMessageArgs) {
        if (reference == null) {
            // If either of these parameters is null, the right thing happens anyway
            throw new ElasticsearchNullPointerException(
                    format(errorMessageTemplate, errorMessageArgs));
        }
        return reference;
    }

    /**
     * Substitutes each {@code %s} in {@code template} with an argument. These
     * are matched by position - the first {@code %s} gets {@code args[0]}, etc.
     * If there are more arguments than placeholders, the unmatched arguments will
     * be appended to the end of the formatted message in square braces.
     *
     * @param template a non-null string containing 0 or more {@code %s}
     *                 placeholders.
     * @param args     the arguments to be substituted into the message
     *                 template. Arguments are converted to strings using
     *                 {@link String#valueOf(Object)}. Arguments can be null.
     */
    // VisibleForTesting
    static String format(String template, Object... args) {
        // start substituting the arguments into the '%s' placeholders
        StringBuilder builder = new StringBuilder(
                template.length() + 16 * args.length);
        int templateStart = 0;
        int i = 0;
        while (i < args.length) {
            int placeholderStart = template.indexOf("%s", templateStart);
            if (placeholderStart == -1) {
                break;
            }
            builder.append(template.substring(templateStart, placeholderStart));
            builder.append(args[i++]);
            templateStart = placeholderStart + 2;
        }
        builder.append(template.substring(templateStart));

        // if we run out of placeholders, append the extra args in square braces
        if (i < args.length) {
            builder.append(" [");
            builder.append(args[i++]);
            while (i < args.length) {
                builder.append(", ");
                builder.append(args[i++]);
            }
            builder.append("]");
        }

        return builder.toString();
    }
}

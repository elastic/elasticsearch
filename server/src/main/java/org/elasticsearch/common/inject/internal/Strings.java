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

package org.elasticsearch.common.inject.internal;

/**
 * String utilities.
 *
 * @author crazybob@google.com (Bob Lee)
 */
public class Strings {
    private Strings() {
    }

    /**
     * Returns a string that is equivalent to the specified string with its
     * first character converted to uppercase as by {@link String#toUpperCase}.
     * The returned string will have the same value as the specified string if
     * its first character is non-alphabetic, if its first character is already
     * uppercase, or if the specified string is of length 0.
     * <p>
     * For example:
     * <pre>
     *    capitalize("foo bar").equals("Foo bar");
     *    capitalize("2b or not 2b").equals("2b or not 2b")
     *    capitalize("Foo bar").equals("Foo bar");
     *    capitalize("").equals("");
     * </pre>
     *
     * @param s the string whose first character is to be uppercased
     * @return a string equivalent to <tt>s</tt> with its first character
     *         converted to uppercase
     * @throws NullPointerException if <tt>s</tt> is null
     */
    public static String capitalize(String s) {
        if (s.length() == 0) {
            return s;
        }
        char first = s.charAt(0);
        char capitalized = Character.toUpperCase(first);
        return (first == capitalized)
                ? s
                : capitalized + s.substring(1);
    }
}

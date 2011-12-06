/**
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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Provides access to the calling line of code.
 *
 * @author crazybob@google.com (Bob Lee)
 */
public class SourceProvider {

    /**
     * Indicates that the source is unknown.
     */
    public static final Object UNKNOWN_SOURCE = "[unknown source]";

    private final ImmutableSet<String> classNamesToSkip;

    public SourceProvider() {
        this.classNamesToSkip = ImmutableSet.of(SourceProvider.class.getName());
    }

    public static final SourceProvider DEFAULT_INSTANCE
            = new SourceProvider(ImmutableSet.of(SourceProvider.class.getName()));

    private SourceProvider(Iterable<String> classesToSkip) {
        this.classNamesToSkip = ImmutableSet.copyOf(classesToSkip);
    }

    /**
     * Returns a new instance that also skips {@code moreClassesToSkip}.
     */
    public SourceProvider plusSkippedClasses(Class... moreClassesToSkip) {
        return new SourceProvider(Iterables.concat(classNamesToSkip, asStrings(moreClassesToSkip)));
    }

    /**
     * Returns the class names as Strings
     */
    private static List<String> asStrings(Class... classes) {
        List<String> strings = Lists.newArrayList();
        for (Class c : classes) {
            strings.add(c.getName());
        }
        return strings;
    }

    /**
     * Returns the calling line of code. The selected line is the nearest to the top of the stack that
     * is not skipped.
     */
    public StackTraceElement get() {
        for (final StackTraceElement element : new Throwable().getStackTrace()) {
            String className = element.getClassName();
            if (!classNamesToSkip.contains(className)) {
                return element;
            }
        }
        throw new AssertionError();
    }
}

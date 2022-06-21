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

import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableSet;

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

    private final Set<String> classNamesToSkip;

    public SourceProvider() {
        this.classNamesToSkip = singleton(SourceProvider.class.getName());
    }

    @SuppressWarnings("rawtypes")
    private SourceProvider(SourceProvider copy, Class[] moreClassesToSkip) {
        Set<String> classNamesToSkip = new HashSet<>(copy.classNamesToSkip);
        for (Class toSkip : moreClassesToSkip) {
            classNamesToSkip.add(toSkip.getName());
        }
        this.classNamesToSkip = unmodifiableSet(classNamesToSkip);
    }

    /**
     * Returns a new instance that also skips {@code moreClassesToSkip}.
     */
    @SuppressWarnings("rawtypes")
    public SourceProvider plusSkippedClasses(Class... moreClassesToSkip) {
        return new SourceProvider(this, moreClassesToSkip);
    }

    /**
     * Returns the calling line of code. The selected line is the nearest to the top of the stack that
     * is not skipped.
     */
    public StackTraceElement get() {
        for (final StackTraceElement element : new Throwable().getStackTrace()) {
            String className = element.getClassName();
            if (classNamesToSkip.contains(className) == false) {
                return element;
            }
        }
        throw new AssertionError();
    }
}

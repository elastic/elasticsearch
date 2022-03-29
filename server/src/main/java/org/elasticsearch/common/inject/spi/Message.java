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

package org.elasticsearch.common.inject.spi;

import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.SourceProvider;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An error message and the context in which it occurred. Messages are usually created internally by
 * Guice and its extensions. Messages can be created explicitly in a module using {@link
 * org.elasticsearch.common.inject.Binder#addError(Throwable) addError()} statements:
 * <pre>
 *     try {
 *       bindPropertiesFromFile();
 *     } catch (IOException e) {
 *       addError(e);
 *     }</pre>
 *
 * @author crazybob@google.com (Bob Lee)
 */
public final class Message implements Element {
    private final String message;
    private final Throwable cause;
    private final List<Object> sources;

    /**
     * @since 2.0
     */
    public Message(List<Object> sources, String message, Throwable cause) {
        this.sources = Collections.unmodifiableList(sources);
        this.message = Objects.requireNonNull(message, "message");
        this.cause = cause;
    }

    public Message(Object source, String message) {
        this(Collections.singletonList(source), message, null);
    }

    public Message(Object source, Throwable cause) {
        this(Collections.singletonList(source), null, cause);
    }

    public Message(String message) {
        this(Collections.emptyList(), message, null);
    }

    @Override
    public String getSource() {
        return sources.isEmpty() ? SourceProvider.UNKNOWN_SOURCE.toString() : Errors.convert(sources.get(sources.size() - 1)).toString();
    }

    /**
     * @since 2.0
     */
    public List<Object> getSources() {
        return sources;
    }

    /**
     * Gets the error message text.
     */
    public String getMessage() {
        return message;
    }

    /**
     * @since 2.0
     */
    @Override
    public <T> T acceptVisitor(ElementVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /**
     * Returns the throwable that caused this message, or {@code null} if this
     * message was not caused by a throwable.
     *
     * @since 2.0
     */
    public Throwable getCause() {
        return cause;
    }

    @Override
    public String toString() {
        return message;
    }

    @Override
    public int hashCode() {
        return message.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if ((o instanceof Message) == false) {
            return false;
        }
        Message e = (Message) o;
        return message.equals(e.message) && Objects.equals(cause, e.cause) && sources.equals(e.sources);
    }

}

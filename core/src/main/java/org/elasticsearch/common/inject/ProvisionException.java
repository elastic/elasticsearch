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

package org.elasticsearch.common.inject;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.spi.Message;

import java.util.Collection;
import java.util.Collections;

/**
 * Indicates that there was a runtime failure while providing an instance.
 *
 * @author kevinb@google.com (Kevin Bourrillion)
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public final class ProvisionException extends RuntimeException {

    private final ImmutableSet<Message> messages;

    /**
     * Creates a ConfigurationException containing {@code messages}.
     */
    public ProvisionException(Iterable<Message> messages) {
        this.messages = ImmutableSet.copyOf(messages);
        if (this.messages.isEmpty()) {
            throw new IllegalArgumentException();
        }
        initCause(Errors.getOnlyCause(this.messages));
    }

    public ProvisionException(String message, Throwable cause) {
        super(cause);
        this.messages = ImmutableSet.of(new Message(Collections.emptyList(), message, cause));
    }

    public ProvisionException(String message) {
        this.messages = ImmutableSet.of(new Message(message));
    }

    /**
     * Returns messages for the errors that caused this exception.
     */
    public Collection<Message> getErrorMessages() {
        return messages;
    }

    @Override
    public String getMessage() {
        return Errors.format("Guice provision errors", messages);
    }

    private static final long serialVersionUID = 0;
}

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

package org.elasticsearch.common.inject;

import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.spi.Message;

import java.util.Collection;

/**
 * Thrown when errors occur while creating a {@link Injector}. Includes a list of encountered
 * errors. Clients should catch this exception, log it, and stop execution.
 *
 * @author crazybob@google.com (Bob Lee)
 */
public class CreationException extends RuntimeException {
    private final Collection<Message> messages;

    /**
     * Creates a CreationException containing {@code messages}.
     */
    public CreationException(Collection<Message> messages) {
        this.messages = messages;
        if (this.messages.isEmpty()) {
            throw new IllegalArgumentException();
        }
        initCause(Errors.getOnlyCause(this.messages));
    }

    /**
     * Returns messages for the errors that caused this exception.
     */
    public Collection<Message> getErrorMessages() {
        return messages;
    }

    @Override
    public String getMessage() {
        return Errors.format("Guice creation errors", messages);
    }
}

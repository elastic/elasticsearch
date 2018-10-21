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

import org.elasticsearch.common.inject.spi.Message;

/**
 * Handles errors in the Injector.
 *
 * @author crazybob@google.com (Bob Lee)
 */
public interface ErrorHandler {

    /**
     * Handles an error.
     */
    void handle(Object source, Errors errors);

    /**
     * Handles a user-reported error.
     */
    void handle(Message message);
}

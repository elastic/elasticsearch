/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.util.yaml.snakeyaml.parser;

import org.elasticsearch.util.yaml.snakeyaml.events.Event;

/**
 * This interface represents an input stream of {@link Event Events}.
 * <p>
 * The parser and the scanner form together the 'Parse' step in the loading
 * process (see chapter 3.1 of the <a href="http://yaml.org/spec/1.1/">YAML
 * Specification</a>).
 * </p>
 *
 * @see org.elasticsearch.util.yaml.snakeyaml.events.Event
 */
public interface Parser {

    /**
     * Check if the next event is one of the given type.
     *
     * @param choice Event ID.
     * @return <code>true</code> if the next event can be assigned to a variable
     *         of the given type. Returns <code>false</code> if no more events
     *         are available.
     * @throws ParserException Thrown in case of malformed input.
     */
    public boolean checkEvent(Event.ID choice);

    /**
     * Return the next event, but do not delete it from the stream.
     *
     * @return The event that will be returned on the next call to
     *         {@link #getEvent}
     * @throws ParserException Thrown in case of malformed input.
     */
    public Event peekEvent();

    /**
     * Returns the next event.
     * <p>
     * The event will be removed from the stream.
     * </p>
     *
     * @throws ParserException Thrown in case of malformed input.
     */
    public Event getEvent();
}

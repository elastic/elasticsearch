/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;

import java.util.Objects;

/**
 * Request to delete a Machine Learning Calendar
 */
public class DeleteCalendarRequest extends ActionRequest {

    private final String calendarId;

    /**
     * The constructor requires a single calendar id.
     * @param calendarId The calendar to delete. Must be {@code non-null}
     */
    public DeleteCalendarRequest(String calendarId) {
        this.calendarId = Objects.requireNonNull(calendarId, "[calendar_id] must not be null");
    }

    public String getCalendarId() {
        return calendarId;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(calendarId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DeleteCalendarRequest other = (DeleteCalendarRequest) obj;
        return Objects.equals(calendarId, other.calendarId);
    }
}

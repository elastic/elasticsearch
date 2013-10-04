/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.unit.TimeValue;

/**
 * Interface that allows to mark action requests that support acknowledgements.
 * Facilitates consistency across different api.
 */
public interface AcknowledgedRequest<T extends ActionRequest<T>> {

    /**
     * Allows to set the timeout
     * @param timeout timeout as a string (e.g. 1s)
     * @return the request itself
     */
    T timeout(String timeout);

    /**
     * Allows to set the timeout
     * @param timeout timeout as a {@link TimeValue}
     * @return the request itself
     */
    T timeout(TimeValue timeout);

    /**
     * Returns the current timeout
     * @return the current timeout as a {@link TimeValue}
     */
    TimeValue timeout();
}

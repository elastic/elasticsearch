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

package org.elasticsearch.util.timer;

/**
 * A handle associated with a {@link TimerTask} that is returned by a
 * {@link Timer}.
 *
 * @author kimchy (Shay Banon)
 */
public interface Timeout {

    /**
     * Returns the {@link Timer} that created this handle.
     */
    Timer getTimer();

    /**
     * Returns the {@link TimerTask} which is associated with this handle.
     */
    TimerTask getTask();

    /**
     * Returns {@code true} if and only if the {@link TimerTask} associated
     * with this handle has been expired.
     */
    boolean isExpired();

    /**
     * Returns {@code true} if and only if the {@link TimerTask} associated
     * with this handle has been cancelled.
     */
    boolean isCancelled();

    /**
     * Cancels the {@link TimerTask} associated with this handle.  It the
     * task has been executed or cancelled already, it will return with no
     * side effect.
     */
    void cancel();
}

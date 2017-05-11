/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Objects;

/**
 * A scroll enables scrolling of search request. It holds a {@link #keepAlive()} time that
 * will control how long to keep the scrolling resources open.
 *
 *
 */
public final class Scroll implements Writeable {

    private final TimeValue keepAlive;

    public Scroll(StreamInput in) throws IOException {
        this.keepAlive = new TimeValue(in);
    }

    /**
     * Constructs a new scroll of the provided keep alive.
     */
    public Scroll(TimeValue keepAlive) {
        this.keepAlive = Objects.requireNonNull(keepAlive, "keepAlive must not be null");
    }

    /**
     * How long the resources will be kept open to support the scroll request.
     */
    public TimeValue keepAlive() {
        return keepAlive;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        keepAlive.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Scroll scroll = (Scroll) o;
        return Objects.equals(keepAlive, scroll.keepAlive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keepAlive);
    }

    @Override
    public String toString() {
        return "Scroll{keepAlive=" + keepAlive + '}';
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;

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
        this.keepAlive = in.readTimeValue();
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
        out.writeTimeValue(keepAlive);
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

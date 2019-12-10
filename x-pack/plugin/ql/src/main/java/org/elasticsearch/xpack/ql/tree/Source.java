/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.tree;

import java.util.Objects;

public final class Source {

    public static final Source EMPTY = new Source(Location.EMPTY, "");

    private final Location location;
    private final String text;

    public Source(int line, int charPositionInLine, String text) {
        this(new Location(line, charPositionInLine), text);
    }

    public Source(Location location, String text) {
        this.location = location;
        this.text = text;
    }

    public Location source() {
        return location;
    }

    public String text() {
        return text;
    }

    @Override
    public int hashCode() {
        return Objects.hash(location, text);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Source other = (Source) obj;
        return Objects.equals(location, other.location) && Objects.equals(text, other.text);
    }

    @Override
    public String toString() {
        return text + location;
    }
}

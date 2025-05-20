/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.geometry;

import java.util.List;

/**
 * Represents a MultiLine geometry object on the earth's surface.
 */
public class MultiLine extends GeometryCollection<Line> {
    public static final MultiLine EMPTY = new MultiLine();

    private MultiLine() {}

    public MultiLine(List<Line> lines) {
        super(lines);
    }

    @Override
    public ShapeType type() {
        return ShapeType.MULTILINESTRING;
    }

    @Override
    public <T, E extends Exception> T visit(GeometryVisitor<T, E> visitor) throws E {
        return visitor.visit(this);
    }
}

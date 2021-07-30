/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.Location;

/**
 * The superclass for all E* (expression) and P* (postfix) nodes.
 */
public abstract class AExpression extends ANode {

    /**
     * Standard constructor with location used for error tracking.
     */
    AExpression(int identifier, Location location) {
        super(identifier, location);
    }
}

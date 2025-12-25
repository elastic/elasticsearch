/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.core.Releasable;

interface Queue extends Accountable, Releasable {
    int size();

    int topCount();

    Row add(Row spare);

    Row top();

    boolean lessThan(Row top, Row spare);

    Row updateTop(Row spare);

    Row pop();
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.api;

import java.util.Iterator;

/**
 * An {@link Iterator} that can return primitive values
 */
public interface ValueIterator<T> extends Iterator<T> {
    boolean nextBoolean();

    byte nextByte();

    short nextShort();

    char nextChar();

    int nextInt();

    long nextLong();

    float nextFloat();

    double nextDouble();
}

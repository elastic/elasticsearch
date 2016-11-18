/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.utils;

import java.io.Closeable;
import java.util.Iterator;

/**
 * An interface for iterators that can have resources that will be automatically cleaned up
 * if iterator is created in a try-with-resources block.
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {

}

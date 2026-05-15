/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;

import java.io.IOException;

/**
 * Resolve a single @link IvfSegmentConfig} for the input segment (query time).
 */
@FunctionalInterface
public interface IvfQueryConfigResolver {

    IvfSegmentConfig resolve(FieldInfo fieldInfo, LeafReader leafReader) throws IOException;

}

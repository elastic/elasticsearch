/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.lucene.index;

import org.apache.lucene.store.Directory;

import java.io.IOException;

/**
 * This class only exists temporarily while we're creating a different approach to allow parsing SegmentInfos of old Lucene versions
 */
public class SegmentInfosHelper {

    public static final SegmentInfos readLucene7x(Directory directory) throws IOException {
        return SegmentInfos.readLatestCommit(directory, 7);
    }
}

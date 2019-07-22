/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.repositories.RepositoryData;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Date;
import java.util.Set;

public interface Repository {
    Tuple<Long, Date> getLatestIndexIdAndTimestamp() throws IOException;

    InputStream getBlobInputStream(String blobName);

    Set<String> getAllIndexDirectoryNames();

    Date getIndexTimestamp(String indexDirectoryName);

    Tuple<Integer, Long> deleteIndex(String indexDirectoryName);

    void cleanup() throws IOException;
}

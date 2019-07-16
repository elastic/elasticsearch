/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.repositories.RepositoryData;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Set;

public interface Repository {
    Tuple<Long, Date> getLatestIndexIdAndTimestamp() throws IOException;

    RepositoryData getRepositoryData(long indexFileGeneration) throws IOException;

    Collection<SnapshotId> getIncompatibleSnapshots() throws IOException;

    Set<String> getAllIndexDirectoryNames();

    Date getIndexTimestamp(String indexDirectoryName);

    Tuple<Integer, Long> deleteIndex(String indexDirectoryName);

    void cleanup() throws IOException;
}

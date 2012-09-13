/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.cache.id;

import org.elasticsearch.common.bytes.HashedBytesArray;

/**
 *
 */
public interface IdReaderTypeCache {

    /**
     * @param docId The Lucene docId of the child document to return the parent _uid for.
     * @return The parent _uid for the specified docId (which is a child document)
     */
    HashedBytesArray parentIdByDoc(int docId);

    /**
     * @param uid The uid of the document to return the lucene docId for
     * @return The lucene docId for the specified uid
     */
    int docById(HashedBytesArray uid);

    /**
     * @param docId The lucene docId of the document to return _uid for
     * @return The _uid of the specified docId
     */
    HashedBytesArray idByDoc(int docId);

    /**
     * @return The size in bytes for this particular instance
     */
    long sizeInBytes();
}

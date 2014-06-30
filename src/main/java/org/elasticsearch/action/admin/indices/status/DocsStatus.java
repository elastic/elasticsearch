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

package org.elasticsearch.action.admin.indices.status;

/**
 * This class will be removed in future versions
 * Use the recovery API instead
 */
@Deprecated
public class DocsStatus {

    long numDocs = 0;
    long maxDoc = 0;
    long deletedDocs = 0;

    /**
     * The number of docs.
     */
    public long getNumDocs() {
        return numDocs;
    }

    /**
     * The max doc.
     */
    public long getMaxDoc() {
        return maxDoc;
    }

    /**
     * The number of deleted docs in the index.
     */
    public long getDeletedDocs() {
        return deletedDocs;
    }
}

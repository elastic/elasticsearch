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

package org.elasticsearch.common.lucene.uid;

public final class Versions {

    /** used to indicate the write operation should succeed regardless of current version **/
    public static final long MATCH_ANY = -3L;

    /** indicates that the current document was not found in lucene and in the version map */
    public static final long NOT_FOUND = -1L;

    // -2 was used for docs that can be found in the index but do not have a version

    /**
     * used to indicate that the write operation should be executed if the document is currently deleted
     * i.e., not found in the index and/or found as deleted (with version) in the version map
     */
    public static final long MATCH_DELETED = -4L;
}

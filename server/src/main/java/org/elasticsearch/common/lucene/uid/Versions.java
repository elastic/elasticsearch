/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

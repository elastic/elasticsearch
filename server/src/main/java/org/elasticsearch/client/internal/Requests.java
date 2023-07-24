/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.internal;

import org.elasticsearch.xcontent.XContentType;

/**
 * @deprecated for removal
 */
@Deprecated
public class Requests {

    /**
     * The default content type to use to generate source documents when indexing.
     * TODO: remove this, we shouldn't have mutable public static fields that we use in prod code
     */
    public static XContentType INDEX_CONTENT_TYPE = XContentType.JSON;

}

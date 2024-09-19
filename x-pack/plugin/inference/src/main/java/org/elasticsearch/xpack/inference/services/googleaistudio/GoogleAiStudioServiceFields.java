/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio;

public class GoogleAiStudioServiceFields {

    /**
     * Didn't find any documentation on this, but provoked it through a large enough request, which returned:
     *
     * <pre>
     *     <code>
     *         {
     *             "error": {
     *                  "code": 400,
    *                      "message": "* BatchEmbedContentsRequest.requests: at most 100 requests can be in one batch\n",
     *                   "status": "INVALID_ARGUMENT"
     *              }
     *          }
     *     </code>
     * </pre>
     */
    static final int EMBEDDING_MAX_BATCH_SIZE = 100;

}

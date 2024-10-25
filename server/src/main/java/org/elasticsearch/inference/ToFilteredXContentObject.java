/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Provides a contract writing the full contents of an object as well as a mechanism for
 * filtering some fields from the response.
 */
public interface ToFilteredXContentObject extends ToXContentObject {

    /**
     * Returns a {@link XContentBuilder} which has some fields removed from the response content.
     */
    XContentBuilder toFilteredXContent(XContentBuilder builder, ToXContent.Params params) throws IOException;
}

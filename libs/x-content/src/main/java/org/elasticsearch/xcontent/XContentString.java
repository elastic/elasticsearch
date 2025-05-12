/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent;

import java.nio.ByteBuffer;

public interface XContentString {
    /**
     * Returns a {@link String} view of the data.
     */
    String string();

    /**
     * Returns a UTF8-encoded {@link ByteBuffer} view of the data.
     */
    ByteBuffer bytes();

    /**
     * Returns the number of characters in the represented string.
     */
    int stringLength();
}

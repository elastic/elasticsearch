/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Objects;

public class RetentionLeaseAlreadyExistsException extends ResourceAlreadyExistsException {

    public RetentionLeaseAlreadyExistsException(final String id) {
        super("retention lease with ID [" + Objects.requireNonNull(id) + "] already exists");
    }

    public RetentionLeaseAlreadyExistsException(final StreamInput in) throws IOException {
        super(in);
    }

}

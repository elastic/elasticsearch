/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Objects;

public class RetentionLeaseNotFoundException extends ResourceNotFoundException {

    public RetentionLeaseNotFoundException(final String id) {
        super("retention lease with ID [" + Objects.requireNonNull(id) + "] not found");
    }

    public RetentionLeaseNotFoundException(final StreamInput in) throws IOException {
        super(in);
    }

}

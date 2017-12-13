/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.util;

import java.io.DataOutputStream;
import java.io.OutputStream;

public class AccessibleDataOutputStream extends DataOutputStream {

    public AccessibleDataOutputStream(OutputStream out) {
        super(out);
    }

    public OutputStream wrappedStream() {
        return out;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql;

import org.elasticsearch.Version;

public class QlVersionMismatchException extends QlServerException {
    private final Version inputVersion;

    public QlVersionMismatchException(String objectName, Version inputVersion, Version expectedVersion) {
        super("Unsupported {} version [{}], expected [{}]", objectName, inputVersion, expectedVersion);
        this.inputVersion = inputVersion;
    }

    public Version getInputVersion() {
        return inputVersion;
    }
}

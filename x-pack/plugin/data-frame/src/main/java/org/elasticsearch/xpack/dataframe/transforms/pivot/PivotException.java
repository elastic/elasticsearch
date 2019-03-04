/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms.pivot;

/**
 * Class for Exceptions thrown specifically by Pivot
 */
public class PivotException extends RuntimeException {

    public PivotException() {
        super();
    }

    public PivotException(String message) {
        super(message);
    }

    public PivotException(String message, Throwable cause) {
        super(message, cause);
    }
}

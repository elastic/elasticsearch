/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

/**
 * Context for log fields generation - includes options that can influence how the fields are generated.
 */
public class ActionLoggingFieldsContext {
    /**
     * Do we want any user authentication context?
     */
    private volatile boolean includeUserInformation;

    public ActionLoggingFieldsContext() {
        this(false);
    }

    public ActionLoggingFieldsContext(boolean includeUserInformation) {
        this.includeUserInformation = includeUserInformation;
    }

    public boolean includeUserInformation() {
        return includeUserInformation;
    }

    public void setIncludeUserInformation(boolean includeUserInformation) {
        this.includeUserInformation = includeUserInformation;
    }
}

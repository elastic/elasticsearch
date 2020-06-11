/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.index;

import java.util.Map;

public class QlFieldCapabilities {
    private final String[] indices;
    private Map<String, Map<String, QlFieldCapability>> fields;
    
    public QlFieldCapabilities(String[] indices) {
        this.indices = indices;
    }

    public Map<String, Map<String, QlFieldCapability>> getFields() {
        return fields;
    }

    public void setFields(Map<String, Map<String, QlFieldCapability>> fields) {
        this.fields = fields;
    }

    public String[] getIndices() {
        return indices;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase;

import java.util.List;

/**
 * The context needed to retrieve fields.
 */
public class FetchFieldsContext {
    private final List<FieldAndFormat> fields;

    public FetchFieldsContext(List<FieldAndFormat> fields) {
        this.fields = fields;
    }

    public List<FieldAndFormat> fields() {
        return fields;
    }
}

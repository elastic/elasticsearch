/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.FormattedDocValues;

import java.util.Map;

/**
 * Interface for retrieving field values by reading field data
 */
public interface FieldValueFetcher {

    /**
     * Return the field data leaf reader for one or more fields.
     * @return a map with the field name as a key and field data leaf reader as value
     */
    Map<String, FormattedDocValues> getLeaves(LeafReaderContext context);
}

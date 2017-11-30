/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import java.util.Map;


public class PreloadedCatalog implements Catalog {

    private final Map<String, GetIndexResult> map;

    public PreloadedCatalog(Map<String, GetIndexResult> map) {
        this.map = map;
    }

    @Override
    public GetIndexResult getIndex(String index) {
        return map.getOrDefault(index, GetIndexResult.notFound(index));
    }
}

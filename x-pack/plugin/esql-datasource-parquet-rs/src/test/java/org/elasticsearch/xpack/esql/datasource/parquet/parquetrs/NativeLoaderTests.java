/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

import org.elasticsearch.test.ESTestCase;

public class NativeLoaderTests extends ESTestCase {

    public void testLoad() {
        NativeLibLoader.ensureLoaded();
    }
}

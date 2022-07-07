/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.aggs.mapreduce.MapReduceValueSource.Field;
import org.elasticsearch.xpack.ml.aggs.mapreduce.MapReduceValueSource.ValueFormatter;

public class MapReduceValueSourceTests extends ESTestCase {

    // helper method to create fields for testing without opening namespace
    public static Field createKeywordFieldForTesting(String name, int id) {
        return new Field(name, id, DocValueFormat.RAW, ValueFormatter.BYTES_REF);
    }
}

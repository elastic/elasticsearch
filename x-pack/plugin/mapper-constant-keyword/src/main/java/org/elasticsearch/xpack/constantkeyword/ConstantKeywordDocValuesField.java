/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.constantkeyword;

import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.field.BaseKeywordDocValuesField;

public class ConstantKeywordDocValuesField extends BaseKeywordDocValuesField {
    public ConstantKeywordDocValuesField(SortedBinaryDocValues input, String name) {
        super(input, name);
    }
}

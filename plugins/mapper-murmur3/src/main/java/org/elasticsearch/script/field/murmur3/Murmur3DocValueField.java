/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field.murmur3;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.script.field.AbstractLongDocValuesField;

public class Murmur3DocValueField extends AbstractLongDocValuesField {

    public Murmur3DocValueField(SortedNumericDocValues input, String name) {
        super(input, name);
    }
}

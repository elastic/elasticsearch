/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.analysis.lucene;

import org.apache.lucene.analysis.charfilter.MappingCharFilter;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;

import java.io.Reader;

public class ReplaceCharToNumber extends MappingCharFilter {

    public ReplaceCharToNumber(Reader in, String oldChar, int newNumber) {
        super(charMap(oldChar, newNumber), in);
    }

    private static NormalizeCharMap charMap(String oldChar, int newNumber) {
        NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();
        builder.add(oldChar, String.valueOf(newNumber));
        return builder.build();
    }
}

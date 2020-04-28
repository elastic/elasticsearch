/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.type;

import java.util.Collections;

import static org.elasticsearch.xpack.ql.type.DataTypes.CONSTANT_KEYWORD;

/**
 * SQL-related information about an index field with a constant_keyword type
 */
public class ConstantKeywordEsField extends KeywordEsField {

    public ConstantKeywordEsField(String name) {
        super(name, CONSTANT_KEYWORD, Collections.emptyMap(), true, Short.MAX_VALUE, false, false);
    }

}

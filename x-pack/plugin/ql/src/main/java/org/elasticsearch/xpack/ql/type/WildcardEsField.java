/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.type;

import java.util.Collections;

import static org.elasticsearch.xpack.ql.type.DataTypes.WILDCARD;

public class WildcardEsField extends KeywordEsField {

    public WildcardEsField(String name) {
        super(name, WILDCARD, Collections.emptyMap(), true, Short.MAX_VALUE, false, false);
    }
}

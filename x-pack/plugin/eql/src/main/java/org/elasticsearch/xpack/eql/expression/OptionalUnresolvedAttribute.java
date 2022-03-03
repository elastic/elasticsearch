/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.expression;

import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * Dedicated attribute for optional field declaration in queries - ?field
 * Separated from its superclass to maintain its optional property which
 * should not result in failure in case of incomplete resolution.
 */
public class OptionalUnresolvedAttribute extends UnresolvedAttribute {

    public OptionalUnresolvedAttribute(Source source, String name) {
        super(source, name);
    }
}

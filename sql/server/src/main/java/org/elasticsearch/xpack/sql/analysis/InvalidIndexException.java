/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis;

import org.elasticsearch.xpack.sql.tree.Node;

public class InvalidIndexException extends AnalysisException {

    public InvalidIndexException(String index, Node<?> source) {
        super(source, "Invalid index %s; contains more than one type", index);
    }
}

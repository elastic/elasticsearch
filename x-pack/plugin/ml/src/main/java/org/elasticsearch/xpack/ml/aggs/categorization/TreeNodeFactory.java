/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.BytesRef;

interface TreeNodeFactory {
    TreeNode newNode(long docCount, int tokenPos, BytesRef[] logTokens);

    LogGroup newGroup(long docCount, BytesRef[] logTokens);
}

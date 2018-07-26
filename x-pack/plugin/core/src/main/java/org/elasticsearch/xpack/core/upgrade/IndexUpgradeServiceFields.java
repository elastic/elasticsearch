/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.upgrade;

import org.elasticsearch.action.support.IndicesOptions;

public final class IndexUpgradeServiceFields {

    public static final IndicesOptions UPGRADE_INDEX_OPTIONS = IndicesOptions.strictSingleIndexNoExpandForbidClosed();

    private IndexUpgradeServiceFields() {}
}

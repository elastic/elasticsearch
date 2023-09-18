/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.common.VersionId;

/**
 * Represents a version number o f a subsidiary component to be reported in node info
 */
public interface ComponentVersionNumber {
    String componentId();

    VersionId<?> versionNumber();
}

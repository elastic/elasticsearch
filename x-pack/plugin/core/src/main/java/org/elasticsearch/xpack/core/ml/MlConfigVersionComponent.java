/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.action.admin.cluster.node.info.ComponentVersionNumber;
import org.elasticsearch.common.VersionId;

public class MlConfigVersionComponent implements ComponentVersionNumber {
    @Override
    public String componentId() {
        return "ml_config_version";
    }

    @Override
    public VersionId<?> versionNumber() {
        return MlConfigVersion.CURRENT;
    }
}

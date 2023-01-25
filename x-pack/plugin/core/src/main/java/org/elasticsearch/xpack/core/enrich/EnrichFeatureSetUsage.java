/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.enrich;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;

public class EnrichFeatureSetUsage extends XPackFeatureSet.Usage {

    public EnrichFeatureSetUsage() {
        super(XPackField.ENRICH, true, true);
    }

    public EnrichFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_5_0;
    }
}

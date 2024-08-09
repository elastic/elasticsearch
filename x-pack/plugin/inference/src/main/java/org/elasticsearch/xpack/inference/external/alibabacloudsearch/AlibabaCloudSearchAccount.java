/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.alibabacloudsearch;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;

import java.net.URI;
import java.util.Objects;

public record AlibabaCloudSearchAccount(@Nullable URI url, SecureString apiKey) {

    public AlibabaCloudSearchAccount {
        Objects.requireNonNull(apiKey);
    }
}

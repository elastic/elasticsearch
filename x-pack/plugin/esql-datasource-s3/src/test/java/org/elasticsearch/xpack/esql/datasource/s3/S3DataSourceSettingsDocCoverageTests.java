/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.xpack.esql.datasources.spi.AbstractDataSourceSettingsDocCoverageTests;

import java.util.Set;

public class S3DataSourceSettingsDocCoverageTests extends AbstractDataSourceSettingsDocCoverageTests {

    @Override
    protected Set<String> settingNames() {
        return S3Configuration.dataSourceFieldNames();
    }

    @Override
    protected String docSectionTitle() {
        return "Amazon S3";
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.security;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.qa.sql.jdbc.ShowTablesTestCase;

import java.util.Properties;

@AwaitsFix(bugUrl="https://github.com/elastic/x-pack-elasticsearch/issues/2074")
public class JdbcShowTablesIT extends ShowTablesTestCase {
    // NOCOMMIT filter out tables starting with .
    @Override
    protected Settings restClientSettings() {
        return RestSqlIT.securitySettings();
    }

    @Override
    protected Properties connectionProperties() {
        return JdbcConnectionIT.securityProperties();
    }
}

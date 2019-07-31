/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.startsWith;

public class SqlDisabledIT extends AbstractSqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(XPackPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(XPackSettings.SQL_ENABLED.getKey(), false)
                .build();
    }

    public void testSqlAction() {
        Throwable throwable = expectThrows(Throwable.class,
                () -> new SqlQueryRequestBuilder(client(), SqlQueryAction.INSTANCE).query("SHOW tables").get());
        assertThat(throwable.getMessage(),
                either(startsWith("no proxy found for action"))   // disabled on client
                        .or(startsWith("failed to find action"))  // disabled on proxy client
                        .or(startsWith("No handler for action [indices:data/read/sql]"))); // disabled on server
    }
}


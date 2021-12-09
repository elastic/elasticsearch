/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.Level;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ScriptCompilationSettingsIT extends AbstractUpgradeTestCase {
    private static final String WARNING =
        "[script.max_compilations_rate] setting was deprecated in Elasticsearch and will be removed in a future release! " +
            "See the breaking changes documentation for the next major version.";
    public void testMaxCompilationRate() throws IOException {
        assumeTrue("default changed in v7.16", UPGRADE_FROM_VERSION.onOrAfter(Version.V_7_15_0) && UPGRADE_FROM_VERSION.before(Version.V_7_16_0));
        if (CLUSTER_TYPE.equals(ClusterType.OLD)) {
            Request request = new Request("PUT", "_cluster/settings");
            request.setJsonEntity("{\"persistent\" : { \"script.context.template.max_compilations_rate\": \"5000/5m\"" +
                // ", \"script.max_compilations_rate\": \"use-context\" " +
                "} }");
            request.setOptions(expectWarnings(WARNING));
            Response response = client().performRequest(request);
            assertEquals("{\"acknowledged\":true," +
                    "\"persistent\":{" +
                    "\"script\":{" +
                        "\"context\":{\"template\":{\"max_compilations_rate\":\"5000/5m\"}}" +
                        //",\"max_compilations_rate\":\"use-context\"" +
                    "}}," +
                    "\"transient\":{}}",
                EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));

        } else if (CLUSTER_TYPE.equals(ClusterType.MIXED)) {
            Request request = new Request("GET", "_cluster/settings");
            Response response = client().performRequest(request);
            assertEquals("{\"persistent\":{\"script\":{" +
                    "\"context\":{\"template\":{\"max_compilations_rate\":\"5000/5m\"}}" +
                    //",\"max_compilations_rate\":\"use-context\"" +
                    "}},\"transient\":{}}",
                EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
        }
    }

    @Override
    protected boolean preserveClusterSettings() {
        return true;
    }
}

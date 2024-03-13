/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.List;

public class TimeSeriesIT extends AbstractEsqlIntegTestCase {

    @Override
    protected EsqlQueryResponse run(EsqlQueryRequest request) {
        assertTrue("timseries requires pragmas", canUseQueryPragmas());
        var settings = Settings.builder().put(request.pragmas().getSettings()).put(QueryPragmas.TIME_SERIES_MODE.getKey(), "true").build();
        request.pragmas(new QueryPragmas(settings));
        return super.run(request);
    }

    public void testEmpty() {
        Settings settings = Settings.builder().put("mode", "time_series").putList("routing_path", List.of("pod")).build();
        client().admin()
            .indices()
            .prepareCreate("pods")
            .setSettings(settings)
            .setMapping(
                "@timestamp",
                "type=date",
                "pod",
                "type=keyword,time_series_dimension=true",
                "cpu",
                "type=long,time_series_metric=gauge"
            )
            .get();
        run("FROM pods | LIMIT 1").close();
    }
}

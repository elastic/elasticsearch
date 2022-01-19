/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.vectortile.rest;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.CCSVersionCheckHelper;
import org.elasticsearch.search.FailBeforeVersionQueryBuilder;
import org.elasticsearch.search.NewlyReleasedQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.NamedXContentRegistry.Entry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RestVectorTileActionTests extends RestActionTestCase {

    private static RestVectorTileAction action = new RestVectorTileAction();

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        List<Entry> namedXContents = searchModule.getNamedXContents();
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                QueryBuilder.class,
                new ParseField(FailBeforeVersionQueryBuilder.NAME),
                FailBeforeVersionQueryBuilder::fromXContent,
                RestApiVersion.onOrAfter(RestApiVersion.current())
            )
        );
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                QueryBuilder.class,
                new ParseField(NewlyReleasedQueryBuilder.NAME),
                NewlyReleasedQueryBuilder::fromXContent,
                RestApiVersion.onOrAfter(RestApiVersion.current())
            )
        );
        return new NamedXContentRegistry(namedXContents);
    }

    public void testCCSCheckCompatibilityFlag() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put(VectorTileRequest.INDEX_PARAM, "my-index");
        params.put(VectorTileRequest.FIELD_PARAM, "my-geo-field");
        params.put(VectorTileRequest.Z_PARAM, "15");
        params.put(VectorTileRequest.X_PARAM, "5271");
        params.put(VectorTileRequest.Y_PARAM, "12710");
        params.put(CCSVersionCheckHelper.CCS_VERSION_CHECK_FLAG, "true");

        String query = """
            { "query" : { "fail_before_current_version" : { }}}
            """;

        {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("my-index/_mvt/my-geo-field/15/5271/12710")
                .withParams(params)
                .withContent(new BytesArray(query), XContentType.JSON)
                .build();

            Exception ex = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, verifyingClient));
            assertEquals(
                "parts of request [POST my-index/_mvt/my-geo-field/15/5271/12710] are not compatible with version 8.0.0 and the "
                    + "'check_ccs_compatibility' is enabled.",
                ex.getMessage()
            );
            assertEquals("This query isn't serializable to nodes on or before 8.0.0", ex.getCause().getMessage());
        }

        String newQueryBuilderInside = """
            { "query" : { "new_released_query" : { }}}
            """;

        {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("my-index/_mvt/my-geo-field/15/5271/12710")
                .withParams(params)
                .withContent(new BytesArray(newQueryBuilderInside), XContentType.JSON)
                .build();

            Exception ex = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, verifyingClient));
            assertEquals(
                "parts of request [POST my-index/_mvt/my-geo-field/15/5271/12710] are not compatible with version 8.0.0 and the "
                    + "'check_ccs_compatibility' is enabled.",
                ex.getMessage()
            );
            assertEquals(
                "NamedWritable [org.elasticsearch.search.NewlyReleasedQueryBuilder] was released in "
                    + "version 8.1.0 and was not supported in version 8.0.0",
                ex.getCause().getMessage()
            );
        }

        // this shouldn't fail without the flag enabled
        params.remove(CCSVersionCheckHelper.CCS_VERSION_CHECK_FLAG);
        if (randomBoolean()) {
            params.put(CCSVersionCheckHelper.CCS_VERSION_CHECK_FLAG, "false");
        }
        {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("my-index/_mvt/my-geo-field/15/5271/12710")
                .withParams(params)
                .withContent(new BytesArray(query), XContentType.JSON)
                .build();
            action.prepareRequest(request, verifyingClient);
        }
    }

}

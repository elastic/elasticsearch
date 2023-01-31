/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class IngestGeoIpClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    public IngestGeoIpClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @Before
    public void waitForDatabases() throws Exception {
        putGeoipPipeline();
        assertBusy(() -> {
            Request request = new Request("GET", "/_ingest/geoip/stats");
            Map<String, Object> response = entityAsMap(client().performRequest(request));

            Map<?, ?> downloadStats = (Map<?, ?>) response.get("stats");
            assertThat(downloadStats.get("databases_count"), equalTo(3));

            Map<?, ?> nodes = (Map<?, ?>) response.get("nodes");
            assertThat(nodes.size(), equalTo(1));
            Map<?, ?> node = (Map<?, ?>) nodes.values().iterator().next();
            List<String> databases = ((List<?>) node.get("databases")).stream()
                .map(o -> (String) ((Map<?, ?>) o).get("name"))
                .collect(Collectors.toList());
            assertThat(databases, containsInAnyOrder("GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "GeoLite2-ASN.mmdb"));
        });
    }

    /**
     * This creates a pipeline with a geoip processor so that the GeoipDownloader will download its databases.
     * @throws IOException
     */
    private void putGeoipPipeline() throws IOException {
        final BytesReference bytes;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startArray("processors");
                {
                    builder.startObject();
                    {
                        builder.startObject("geoip");
                        {
                            builder.field("field", "ip");
                            builder.field("target_field", "ip-city");
                            builder.field("database_file", "GeoLite2-City.mmdb");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();
            bytes = BytesReference.bytes(builder);
        }
        Request putPipelineRequest = new Request("PUT", "/_ingest/pipeline/pipeline-with-geoip");
        putPipelineRequest.setEntity(new ByteArrayEntity(bytes.array(), ContentType.APPLICATION_JSON));
        client().performRequest(putPipelineRequest);
    }

}

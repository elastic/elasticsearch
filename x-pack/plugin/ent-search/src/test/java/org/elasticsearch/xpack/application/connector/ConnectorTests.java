/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.CoreMatchers.equalTo;

public class ConnectorTests extends ESTestCase {

    public void testToXContent() throws IOException {
        String connectorId = "test-connector";
        String content = XContentHelper.stripWhitespace("""
            {
               "api_key_id":"test-aki",
               "api_key_secret_id":"test-aksi",
               "custom_scheduling":{
                  "schedule-key":{
                     "configuration_overrides":{
                        "domain_allowlist":[
                           "https://example.com"
                        ],
                        "max_crawl_depth":1,
                        "seed_urls":[
                           "https://example.com/blog",
                           "https://example.com/info"
                        ],
                        "sitemap_discovery_disabled":true,
                        "sitemap_urls":[
                           "https://example.com/sitemap.xml"
                        ]
                     },
                     "enabled":true,
                     "interval":"0 0 12 * * ?",
                     "last_synced":null,
                     "name":"My Schedule"
                  }
               },
               "configuration":{
                  "some_field":{
                     "default_value":null,
                     "depends_on":[
                        {
                           "field":"some_field",
                           "value":true
                        }
                     ],
                     "display":"textbox",
                     "label":"Very important field",
                     "options":[],
                     "order":4,
                     "required":true,
                     "sensitive":false,
                     "tooltip":"Wow, this tooltip is useful.",
                     "type":"str",
                     "ui_restrictions":[],
                     "validations":[
                        {
                           "constraint":0,
                           "type":"greater_than"
                        }
                     ],
                     "value":""
                  },
                   "field_with_null_tooltip":{
                     "default_value":null,
                     "depends_on":[
                        {
                           "field":"some_field",
                           "value":true
                        }
                     ],
                     "display":"textbox",
                     "label":"Very important field",
                     "options":[],
                     "order":4,
                     "required":true,
                     "sensitive":false,
                     "tooltip":null,
                     "type":"str",
                     "ui_restrictions":[],
                     "validations":[
                        {
                           "constraint":0,
                           "type":"greater_than"
                        }
                     ],
                     "value":""
                  }
               },
               "description":"test-connector",
               "features":{
                  "document_level_security":{
                     "enabled":true
                  },
                  "sync_rules":{
                     "advanced":{
                        "enabled":false
                     },
                     "basic":{
                        "enabled":true
                     }
                  },
                  "native_connector_api_keys": {
                     "enabled": true
                  }
               },
               "filtering":[
                  {
                     "active":{
                        "advanced_snippet":{
                           "created_at":"2023-11-09T15:13:08.231Z",
                           "updated_at":"2023-11-09T15:13:08.231Z",
                           "value":[
                             {
                                 "tables": [
                                     "some_table"
                                 ],
                                 "query": "SELECT id, st_geohash(coordinates) FROM my_db.some_table;"
                             }
                           ]
                        },
                        "rules":[
                           {
                              "created_at":"2023-11-09T15:13:08.231Z",
                              "field":"_",
                              "id":"DEFAULT",
                              "order":0,
                              "policy":"include",
                              "rule":"regex",
                              "updated_at":"2023-11-09T15:13:08.231Z",
                              "value":".*"
                           }
                        ],
                        "validation":{
                           "errors":[],
                           "state":"valid"
                        }
                     },
                     "domain":"DEFAULT",
                     "draft":{
                        "advanced_snippet":{
                           "created_at":"2023-11-09T15:13:08.231Z",
                           "updated_at":"2023-11-09T15:13:08.231Z",
                           "value":[
                             {
                                 "tables": [
                                     "some_table"
                                 ],
                                 "query": "SELECT id, st_geohash(coordinates) FROM my_db.some_table;"
                             }
                           ]
                        },
                        "rules":[
                           {
                              "created_at":"2023-11-09T15:13:08.231Z",
                              "field":"_",
                              "id":"DEFAULT",
                              "order":0,
                              "policy":"include",
                              "rule":"regex",
                              "updated_at":"2023-11-09T15:13:08.231Z",
                              "value":".*"
                           }
                        ],
                        "validation":{
                           "errors":[],
                           "state":"valid"
                        }
                     }
                  }
               ],
               "index_name":"search-test",
               "is_native":true,
               "language":"polish",
               "last_access_control_sync_error":"some error",
               "last_access_control_sync_scheduled_at":"2023-11-09T15:13:08.231Z",
               "last_access_control_sync_status":"pending",
               "last_deleted_document_count":42,
               "last_incremental_sync_scheduled_at":"2023-11-09T15:13:08.231Z",
               "last_indexed_document_count":42,
               "last_seen":"2023-11-09T15:13:08.231Z",
               "last_sync_error":"some error",
               "last_sync_scheduled_at":"2024-11-09T15:13:08.231Z",
               "last_sync_status":"completed",
               "last_synced":"2024-11-09T15:13:08.231Z",
               "name":"test-name",
               "pipeline":{
                  "extract_binary_content":true,
                  "name":"search-default-ingestion",
                  "reduce_whitespace":true,
                  "run_ml_inference":false
               },
               "scheduling":{
                  "access_control":{
                     "enabled":false,
                     "interval":"0 0 0 * * ?"
                  },
                  "full":{
                     "enabled":false,
                     "interval":"0 0 0 * * ?"
                  },
                  "incremental":{
                     "enabled":false,
                     "interval":"0 0 0 * * ?"
                  }
               },
               "service_type":"google_drive",
               "status":"needs_configuration",
               "sync_now":false
            }""");

        Connector connector = Connector.fromXContentBytes(new BytesArray(content), connectorId, XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(connector, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        Connector parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = Connector.fromXContent(parser, connectorId);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testToContent_WithNullValues() throws IOException {
        String connectorId = "test-connector";
        String content = XContentHelper.stripWhitespace("""
            {
               "api_key_id": null,
               "api_key_secret_id": null,
               "custom_scheduling":{},
               "configuration":{},
               "description": null,
               "features": null,
               "filtering":[],
               "index_name": "search-test",
               "is_native": false,
               "language": null,
               "last_access_control_sync_error": null,
               "last_access_control_sync_scheduled_at": null,
               "last_access_control_sync_status": null,
               "last_deleted_document_count":null,
               "last_incremental_sync_scheduled_at": null,
               "last_indexed_document_count":null,
               "last_seen": null,
               "last_sync_error": null,
               "last_sync_scheduled_at": null,
               "last_sync_status": null,
               "last_synced": null,
               "name": null,
               "pipeline":{
                  "extract_binary_content":true,
                  "name":"search-default-ingestion",
                  "reduce_whitespace":true,
                  "run_ml_inference":false
               },
               "scheduling":{
                  "access_control":{
                     "enabled":false,
                     "interval":"0 0 0 * * ?"
                  },
                  "full":{
                     "enabled":false,
                     "interval":"0 0 0 * * ?"
                  },
                  "incremental":{
                     "enabled":false,
                     "interval":"0 0 0 * * ?"
                  }
               },
               "service_type": null,
               "status": "needs_configuration",
               "sync_now":false
            }""");

        Connector connector = Connector.fromXContentBytes(new BytesArray(content), connectorId, XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(connector, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        Connector parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = Connector.fromXContent(parser, connectorId);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
    }

    public void testToXContent_withOptionalFieldsMissing() throws IOException {
        // This test is to ensure the doc can serialize without fields that have been added since 8.12.
        // This is to avoid breaking serverless, which has a regular BC built
        // that can be broken if we haven't made migrations yet.
        String connectorId = "test-connector";

        // Missing from doc:
        // api_key_secret_id
        String content = XContentHelper.stripWhitespace("""
            {
               "api_key_id": null,
               "custom_scheduling":{},
               "configuration":{},
               "description": null,
               "features": null,
               "filtering":[],
               "index_name": "search-test",
               "is_native": false,
               "language": null,
               "last_access_control_sync_error": null,
               "last_access_control_sync_scheduled_at": null,
               "last_access_control_sync_status": null,
               "last_incremental_sync_scheduled_at": null,
               "last_seen": null,
               "last_sync_error": null,
               "last_sync_scheduled_at": null,
               "last_sync_status": null,
               "last_synced": null,
               "name": null,
               "pipeline":{
                  "extract_binary_content":true,
                  "name":"search-default-ingestion",
                  "reduce_whitespace":true,
                  "run_ml_inference":false
               },
               "scheduling":{
                  "access_control":{
                     "enabled":false,
                     "interval":"0 0 0 * * ?"
                  },
                  "full":{
                     "enabled":false,
                     "interval":"0 0 0 * * ?"
                  },
                  "incremental":{
                     "enabled":false,
                     "interval":"0 0 0 * * ?"
                  }
               },
               "service_type": null,
               "status": "needs_configuration",
               "sync_now":false
            }""");

        Connector connector = Connector.fromXContentBytes(new BytesArray(content), connectorId, XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(connector, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        Connector parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = Connector.fromXContent(parser, connectorId);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);
        assertThat(parsed.getApiKeySecretId(), equalTo(null));
    }
}

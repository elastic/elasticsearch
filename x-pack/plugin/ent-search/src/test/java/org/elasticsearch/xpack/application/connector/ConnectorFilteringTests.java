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
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class ConnectorFilteringTests extends ESTestCase {

    public void testToXContent() throws IOException {
        String content = XContentHelper.stripWhitespace("""
                {
                    "active": {
                        "advanced_snippet": {
                            "created_at": "2023-11-09T15:13:08.231Z",
                            "updated_at": "2023-11-09T15:13:08.231Z",
                            "value": {}
                        },
                        "rules": [
                            {
                                "created_at": "2023-11-09T15:13:08.231Z",
                                "field": "_",
                                "id": "DEFAULT",
                                "order": 0,
                                "policy": "include",
                                "rule": "regex",
                                "updated_at": "2023-11-09T15:13:08.231Z",
                                "value": ".*"
                            }
                        ],
                        "validation": {
                            "errors": [],
                            "state": "valid"
                        }
                    },
                    "domain": "DEFAULT",
                    "draft": {
                        "advanced_snippet": {
                            "created_at": "2023-11-09T15:13:08.231Z",
                            "updated_at": "2023-11-09T15:13:08.231Z",
                            "value": {}
                        },
                        "rules": [
                            {
                                "created_at": "2023-11-09T15:13:08.231Z",
                                "field": "_",
                                "id": "DEFAULT",
                                "order": 0,
                                "policy": "include",
                                "rule": "regex",
                                "updated_at": "2023-11-09T15:13:08.231Z",
                                "value": ".*"
                            }
                        ],
                        "validation": {
                            "errors": [],
                            "state": "valid"
                        }
                    }
                }
            """);

        ConnectorFiltering filtering = ConnectorFiltering.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(filtering, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        ConnectorFiltering parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = ConnectorFiltering.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);

    }

    public void testToXContent_WithAdvancedSnippetPopulatedWithAValueArray() throws IOException {
        String content = XContentHelper.stripWhitespace("""
                {
                    "active": {
                        "advanced_snippet": {
                            "created_at": "2023-11-09T15:13:08.231Z",
                            "updated_at": "2023-11-09T15:13:08.231Z",
                            "value": [
                               {"service": "Incident", "query": "user_nameSTARTSWITHa"},
                               {"service": "Incident", "query": "user_nameSTARTSWITHj"}
                            ]
                        },
                        "rules": [
                            {
                                "created_at": "2023-11-09T15:13:08.231Z",
                                "field": "_",
                                "id": "DEFAULT",
                                "order": 0,
                                "policy": "include",
                                "rule": "regex",
                                "updated_at": "2023-11-09T15:13:08.231Z",
                                "value": ".*"
                            }
                        ],
                        "validation": {
                            "errors": [],
                            "state": "valid"
                        }
                    },
                    "domain": "DEFAULT",
                    "draft": {
                        "advanced_snippet": {
                            "created_at": "2023-11-09T15:13:08.231Z",
                            "updated_at": "2023-11-09T15:13:08.231Z",
                            "value": {}
                        },
                        "rules": [
                            {
                                "created_at": "2023-11-09T15:13:08.231Z",
                                "field": "_",
                                "id": "DEFAULT",
                                "order": 0,
                                "policy": "include",
                                "rule": "regex",
                                "updated_at": "2023-11-09T15:13:08.231Z",
                                "value": ".*"
                            }
                        ],
                        "validation": {
                            "errors": [],
                            "state": "valid"
                        }
                    }
                }
            """);

        ConnectorFiltering filtering = ConnectorFiltering.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(filtering, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        ConnectorFiltering parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = ConnectorFiltering.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);

    }

    public void testToXContent_WithAdvancedSnippetPopulatedWithAValueObject() throws IOException {
        String content = XContentHelper.stripWhitespace("""
                {
                    "active": {
                        "advanced_snippet": {
                            "created_at": "2023-11-09T15:13:08.231Z",
                            "updated_at": "2023-11-09T15:13:08.231Z",
                            "value": {
                                    "service": "Incident",
                                    "query": "user_nameSTARTSWITHa"
                            }
                        },
                        "rules": [
                            {
                                "created_at": "2023-11-09T15:13:08.231Z",
                                "field": "_",
                                "id": "DEFAULT",
                                "order": 0,
                                "policy": "include",
                                "rule": "regex",
                                "updated_at": "2023-11-09T15:13:08.231Z",
                                "value": ".*"
                            }
                        ],
                        "validation": {
                            "errors": [{"ids": ["1"], "messages": ["some messages"]}],
                            "state": "invalid"
                        }
                    },
                    "domain": "DEFAULT",
                    "draft": {
                        "advanced_snippet": {
                            "created_at": "2023-11-09T15:13:08.231Z",
                            "updated_at": "2023-11-09T15:13:08.231Z",
                            "value": {}
                        },
                        "rules": [
                            {
                                "created_at": "2023-11-09T15:13:08.231Z",
                                "field": "_",
                                "id": "DEFAULT",
                                "order": 0,
                                "policy": "include",
                                "rule": "regex",
                                "updated_at": "2023-11-09T15:13:08.231Z",
                                "value": ".*"
                            }
                        ],
                        "validation": {
                            "errors": [],
                            "state": "valid"
                        }
                    }
                }
            """);

        ConnectorFiltering filtering = ConnectorFiltering.fromXContentBytes(new BytesArray(content), XContentType.JSON);
        boolean humanReadable = true;
        BytesReference originalBytes = toShuffledXContent(filtering, XContentType.JSON, ToXContent.EMPTY_PARAMS, humanReadable);
        ConnectorFiltering parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), originalBytes)) {
            parsed = ConnectorFiltering.fromXContent(parser);
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, XContentType.JSON, humanReadable), XContentType.JSON);

    }

    public void testToXContent_WithAdvancedSnippetPopulatedWithAValueLiteral_ExpectParseException() throws IOException {
        String content = XContentHelper.stripWhitespace("""
                {
                    "active": {
                        "advanced_snippet": {
                            "created_at": "2023-11-09T15:13:08.231Z",
                            "updated_at": "2023-11-09T15:13:08.231Z",
                            "value": "string literal"
                        },
                        "rules": [
                            {
                                "created_at": "2023-11-09T15:13:08.231Z",
                                "field": "_",
                                "id": "DEFAULT",
                                "order": 0,
                                "policy": "include",
                                "rule": "regex",
                                "updated_at": "2023-11-09T15:13:08.231Z",
                                "value": ".*"
                            }
                        ],
                        "validation": {
                            "errors": [],
                            "state": "valid"
                        }
                    },
                    "domain": "DEFAULT",
                    "draft": {
                        "advanced_snippet": {
                            "created_at": "2023-11-09T15:13:08.231Z",
                            "updated_at": "2023-11-09T15:13:08.231Z",
                            "value": {}
                        },
                        "rules": [
                            {
                                "created_at": "2023-11-09T15:13:08.231Z",
                                "field": "_",
                                "id": "DEFAULT",
                                "order": 0,
                                "policy": "include",
                                "rule": "regex",
                                "updated_at": "2023-11-09T15:13:08.231Z",
                                "value": ".*"
                            }
                        ],
                        "validation": {
                            "errors": [],
                            "state": "valid"
                        }
                    }
                }
            """);

        assertThrows(XContentParseException.class, () -> ConnectorFiltering.fromXContentBytes(new BytesArray(content), XContentType.JSON));
    }
}

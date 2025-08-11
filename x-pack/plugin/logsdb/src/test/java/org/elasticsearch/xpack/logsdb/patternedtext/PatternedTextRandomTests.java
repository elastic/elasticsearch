/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class PatternedTextRandomTests extends ESIntegTestCase {



    public void test() throws IOException {
        var settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName());
        String index = "test_index";
        var mappings = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .endObject()
            .startObject("field_patterned_text")
            .field("type", "patterned_text")
            .endObject()
            .startObject("field_match_only_text")
            .field("type", "match_only_text")
            .endObject()
            .endObject()
            .endObject();

        var createRequest = new CreateIndexRequest(index)
            .settings(settings)
            .mapping(mappings);

        var createResponse = safeGet(admin().indices().create(createRequest));
        assertTrue(createResponse.isAcknowledged());

        BulkRequest bulkRequest = new BulkRequest();
        int numDocs = randomIntBetween(1, 300);
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < numDocs; i++) {
            timestamp += TimeUnit.SECONDS.toMillis(1);
            String logMessage = randomMessage();
            var indexRequest = new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE)
                .source(
                    JsonXContent.contentBuilder()
                        .startObject()
                        .field("@timestamp", timestamp)
                        .field("field_patterned_text", logMessage)
                        .field("field_match_only_text", logMessage);
                        .endObject()
                );
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();

            client().index(

            )
        );
        safeGet(
            indicesAdmin().refresh(
                new RefreshRequest(".ds-" + dataStreamName + "*").indicesOptions(IndicesOptions.lenientExpandOpenHidden())
            )
        );

        int numDocs = randomIntBetween(100, 1000);


        new BulkRequest()
            .

        for (int i = 0; i < numDocs; i++) {

        }

    }



    public static String randomMessage() {
        if (rarely()) {
            return randomRealisticUnicodeOfCodepointLength(randomIntBetween(1, 100));
        }

        StringBuilder message = new StringBuilder();
        int numTokens = randomIntBetween(1, 30);

        if (randomBoolean()) {
            message.append("[").append(randomTimestamp()).append("]");
        }
        for (int i = 0; i < numTokens; i++) {
            message.append(randomSeparator());

            if (randomBoolean()) {
                message.append(randomSentence());
            } else {
                var token = randomFrom(
                    random(),
                    () -> randomRealisticUnicodeOfCodepointLength(randomIntBetween(1, 20)),
                    () -> UUID.randomUUID().toString(),
                    () -> randomIp(randomBoolean()),
                    PatternedTextRandomTests::randomTimestamp,
                    ESTestCase::randomInt,
                    ESTestCase::randomDouble
                );

                if (randomBoolean()) {
                    message.append("[").append(token).append("]");
                } else {
                    message.append(token);
                }
            }
        }
        return message.toString();
    }

    private static StringBuilder randomSentence() {
        int words = randomIntBetween(1, 10);
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < words; i++) {
            if (i > 0) {
                text.append(" ");
            }
            text.append(randomAlphaOfLength(randomIntBetween(1, 10)));
        }
        return text;
    }

    private static String randomSeparator() {
        if (randomBoolean()) {
            // Return spaces frequently since current token splitting is on spaces.
            return " ".repeat(randomIntBetween(1, 10));
        } else {
            return randomFrom("\t\n;:.',".split(""));
        }
    }

    private static String randomTimestamp() {
        long millis = randomMillisUpToYear9999();
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), randomZone());
        DateFormatter formatter = DateFormatter.forPattern(randomDateFormatterPattern()).withLocale(randomLocale(random()));
        return formatter.format(zonedDateTime);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.elasticsearch.common.Strings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@code NESTED_EXPAND} and {@code NESTED_ANY} over Parquet collections — a {@code LIST} of structures and a {@code MAP} —
 * running the queries of the collection representation specification over files written here, each against the table
 * the specification prints beneath it.
 */
public class ExternalParquetCollectionsIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    private static final MessageType DNS = MessageTypeParser.parseMessageType("""
        message dns {
          required binary question (UTF8);
          optional group answers (LIST) {
            repeated group list {
              optional group element {
                optional binary name (UTF8);
                optional binary type (UTF8);
                optional int64 ttl;
                optional binary data (UTF8);
              }
            }
          }
        }
        """);

    private static final MessageType SPANS = MessageTypeParser.parseMessageType("""
        message spans {
          required binary span_id (UTF8);
          optional group attributes (MAP) {
            repeated group key_value {
              required binary key (UTF8);
              optional binary value (UTF8);
            }
          }
          optional group metrics (MAP) {
            repeated group key_value {
              required binary key (UTF8);
              optional int64 value;
            }
          }
          optional group headers (MAP) {
            repeated group key_value {
              required binary key (UTF8);
              optional group value (LIST) {
                repeated group list {
                  optional binary element (UTF8);
                }
              }
            }
          }
        }
        """);

    private static final MessageType EMAIL = MessageTypeParser.parseMessageType("""
        message email {
          optional group email {
            optional binary subject (UTF8);
          }
          optional group attachments (LIST) {
            repeated group list {
              optional group element {
                optional group file {
                  optional binary name (UTF8);
                  optional int64 size;
                  optional binary mime_type (UTF8);
                }
              }
            }
          }
        }
        """);

    public void testDnsAnswers() throws Exception {
        String dns = registerDataset("dns", StoragePath.fileUri(write("dns", DNS, List.of(g -> {
            g.add("question", "www.example.com");
            Group answers = g.addGroup("answers");
            answer(answers, "www.example.com", "CNAME", 30L, "lb.example.net");
            answer(answers, "lb.example.net", "A", 300L, "93.184.1.1");
        }, g -> {
            g.add("question", "api.example.com");
            answer(g.addGroup("answers"), "api.example.com", "A", 60L, "93.184.1.2");
        }, g -> {
            g.add("question", "nx.example.com");
            g.addGroup("answers");
        }, g -> {
            g.add("question", "cdn.example.com");
            Group answers = g.addGroup("answers");
            answer(answers, null, "A", 10L, null);
            answer(answers, null, "CNAME", 300L, null);
        }))), Map.of());

        // Which queries got a CNAME with a short TTL?
        assertRows(
            "FROM " + dns + " | WHERE NESTED_ANY(answers, answers.type == \"CNAME\" AND answers.ttl < 60) | KEEP question",
            row("www.example.com")
        );

        // Show me the answers.
        assertRows(
            "FROM " + dns + " | NESTED_EXPAND answers | KEEP question, answers.name, answers.type, answers.ttl",
            row("www.example.com", "www.example.com", "CNAME", 30L),
            row("www.example.com", "lb.example.net", "A", 300L),
            row("api.example.com", "api.example.com", "A", 60L),
            row("nx.example.com", null, null, null),
            row("cdn.example.com", null, "A", 10L),
            row("cdn.example.com", null, "CNAME", 300L)
        );

        // What record types does this file return? The response with no answers keeps one row of nulls, so it is a group too.
        assertRows(
            "FROM " + dns + " | NESTED_EXPAND answers | STATS n = COUNT() BY answers.type",
            row(3L, "A"),
            row(2L, "CNAME"),
            row(1L, null)
        );

        // What is the shortest TTL in each response?
        assertRows(
            "FROM " + dns + " | NESTED_EXPAND answers | STATS min_ttl = MIN(answers.ttl) BY question",
            row(30L, "www.example.com"),
            row(60L, "api.example.com"),
            row(10L, "cdn.example.com"),
            row(null, "nx.example.com")
        );

        // A query naming no collection operator reads the dataset as it did before.
        assertRows("FROM " + dns + " | KEEP question | SORT question | LIMIT 1", row("api.example.com"));
    }

    public void testEmailAttachments() throws Exception {
        String email = registerDataset("email", StoragePath.fileUri(write("email", EMAIL, List.of(g -> {
            g.addGroup("email").add("subject", "quarterly report");
            Group attachments = g.addGroup("attachments");
            attachment(attachments, "report.pdf", 20_000_000L, "application/pdf");
            attachment(attachments, "logo.png", 30_000L, "image/png");
        }, g -> {
            g.addGroup("email").add("subject", "small pdf");
            attachment(g.addGroup("attachments"), "note.pdf", 1_000L, "application/pdf");
        }, g -> {
            g.addGroup("email").add("subject", "large image and a small pdf");
            Group attachments = g.addGroup("attachments");
            attachment(attachments, "photo.png", 50_000_000L, "image/png");
            attachment(attachments, "memo.pdf", 2_000L, "application/pdf");
        }))), Map.of());

        // Which emails carry a large PDF? The element's structure gives further columns of the same relation.
        assertRows(Strings.format("""
            FROM %s
            | WHERE NESTED_ANY(attachments, attachments.file.mime_type == "application/pdf"
                                        AND attachments.file.size > 10000000)
            | KEEP email.subject""", email), row("quarterly report"));
    }

    public void testOtelSpans() throws Exception {
        String spans = registerDataset("otel_spans", StoragePath.fileUri(write("otel_spans", SPANS, List.of(g -> {
            g.add("span_id", "s1");
            Group attributes = g.addGroup("attributes");
            entry(attributes, "service.name", "checkout");
            entry(attributes, "http.request.method", "POST");
            entry(attributes, "k8s.pod.label.app", "checkout");
            entry(attributes, "k8s.pod.label.tier", "frontend");
            Group metrics = g.addGroup("metrics");
            metrics.addGroup("key_value").append("key", "http.response.status_code").append("value", 500L);
            metrics.addGroup("key_value").append("key", "duration_ms").append("value", 1840L);
            Group headers = g.addGroup("headers");
            header(headers, "accept", "text/html", "*/*");
            header(headers, "user-agent", "curl/8.4");
        }, g -> {
            g.add("span_id", "s2");
            Group attributes = g.addGroup("attributes");
            entry(attributes, "service.name", "cart");
            entry(attributes, "http.request.method", "GET");
            entry(attributes, "k8s.pod.label.app", "cart");
            Group metrics = g.addGroup("metrics");
            metrics.addGroup("key_value").append("key", "http.response.status_code").append("value", 200L);
            metrics.addGroup("key_value").append("key", "duration_ms").append("value", 12L);
            header(g.addGroup("headers"), "accept", "application/json");
        }, g -> {
            g.add("span_id", "s3");
            Group attributes = g.addGroup("attributes");
            entry(attributes, "service.name", "checkout");
            entry(attributes, "http.request.method", "GET");
            Group metrics = g.addGroup("metrics");
            metrics.addGroup("key_value").append("key", "http.response.status_code").append("value", 200L);
            metrics.addGroup("key_value").append("key", "duration_ms").append("value", 31L);
            g.addGroup("headers");
        }))), Map.of());

        // Which checkout spans failed? The value comparison is numeric because that value column is int64 in the footer.
        assertRows(Strings.format("""
            FROM %s
            | WHERE NESTED_ANY(attributes, attributes.key == "service.name" AND attributes.value == "checkout")
                AND NESTED_ANY(metrics, metrics.key == "http.response.status_code" AND metrics.value >= 500)
            | KEEP span_id""", spans), row("s1"));

        // Which pods carry both of these labels?
        assertRows(Strings.format("""
            FROM %s
            | WHERE NESTED_ANY(attributes, attributes.key == "k8s.pod.label.app" AND attributes.value == "checkout")
                AND NESTED_ANY(attributes, attributes.key == "k8s.pod.label.tier" AND attributes.value == "frontend")
            | KEEP span_id""", spans), row("s1"));

        // Which spans carry any pod label at all?
        assertRows(
            "FROM " + spans + " | WHERE NESTED_ANY(attributes, attributes.key LIKE \"k8s.pod.label.*\") | KEEP span_id",
            row("s1"),
            row("s2")
        );

        // What attribute keys does this file actually use?
        assertRows(
            "FROM " + spans + " | NESTED_EXPAND attributes | STATS spans = COUNT() BY attributes.key",
            row(3L, "service.name"),
            row(3L, "http.request.method"),
            row(2L, "k8s.pod.label.app"),
            row(1L, "k8s.pod.label.tier")
        );

        // Which requests would accept HTML? A list-valued map, so two levels: the entries, then the values of the entry.
        assertRows(Strings.format("""
            FROM %s | NESTED_EXPAND headers
            | WHERE headers.key == "accept" AND MV_CONTAINS(headers.value, "text/html")
            | KEEP span_id""", spans), row("s1"));
    }

    public void testCollectionInsideElementIsRefused() throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType("""
            message nested {
              required binary id (UTF8);
              optional group by_host (MAP) {
                repeated group key_value {
                  required binary key (UTF8);
                  optional group value (MAP) {
                    repeated group key_value {
                      required binary key (UTF8);
                      optional int64 value;
                    }
                  }
                }
              }
            }
            """);
        String nested = registerDataset("by_host", StoragePath.fileUri(write("by_host", schema, List.of(g -> {
            g.add("id", "a");
            Group inner = g.addGroup("by_host").addGroup("key_value").append("key", "web01").addGroup("value");
            inner.addGroup("key_value").append("key", "cpu").append("value", 93L);
        }))), Map.of());
        Exception e = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("FROM " + nested + " | NESTED_EXPAND by_host")).close());
        assertThat(e.getMessage(), containsString("is a collection with more than one field inside each element of [by_host]"));
    }

    private void assertRows(String query, List<?>... expected) {
        try (var response = run(syncEsqlQueryRequest(query))) {
            List<List<Object>> rows = getValuesList(response);
            List<List<Object>> normalized = new ArrayList<>();
            for (List<Object> r : rows) {
                List<Object> n = new ArrayList<>();
                for (Object value : r) {
                    n.add(value instanceof Integer i ? (Object) i.longValue() : value);
                }
                normalized.add(n);
            }
            assertThat(query, normalized, containsInAnyOrder(expected));
            assertThat(query, normalized.size(), equalTo(expected.length));
        }
    }

    private static List<?> row(Object... values) {
        return Arrays.asList(values);
    }

    private static void answer(Group answers, String name, String type, Long ttl, String data) {
        Group element = answers.addGroup("list").addGroup("element");
        if (name != null) {
            element.append("name", name);
        }
        if (type != null) {
            element.append("type", type);
        }
        if (ttl != null) {
            element.append("ttl", ttl);
        }
        if (data != null) {
            element.append("data", data);
        }
    }

    private static void attachment(Group attachments, String name, long size, String mimeType) {
        attachments.addGroup("list")
            .addGroup("element")
            .addGroup("file")
            .append("name", name)
            .append("size", size)
            .append("mime_type", mimeType);
    }

    private static void entry(Group map, String key, String value) {
        map.addGroup("key_value").append("key", key).append("value", value);
    }

    private static void header(Group headers, String key, String... values) {
        Group list = headers.addGroup("key_value").append("key", key).addGroup("value");
        for (String value : values) {
            list.addGroup("list").append("element", value);
        }
    }

    private Path write(String name, MessageType schema, List<Consumer<Group>> rows) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (Consumer<Group> row : rows) {
                Group g = factory.newGroup();
                row.accept(g);
                writer.write(g);
            }
        }
        Path file = createTempDir().resolve(name + ".parquet");
        Files.write(file, baos.toByteArray());
        return file;
    }
}

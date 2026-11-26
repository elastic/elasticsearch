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
 * {@code NESTED_EXPAND} and {@code NESTED_ANY} over Parquet collections, each a {@code LIST} of structures,
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

    /** A {@code MAP} is not a collection: neither operator reads it. */
    public void testMapIsNotACollection() throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType("""
            message spans {
              required binary span_id (UTF8);
              optional group attributes (MAP) {
                repeated group key_value {
                  required binary key (UTF8);
                  optional binary value (UTF8);
                }
              }
            }
            """);
        String spans = registerDataset("spans", StoragePath.fileUri(write("spans", schema, List.of(g -> {
            g.add("span_id", "s1");
            g.addGroup("attributes").addGroup("key_value").append("key", "service.name").append("value", "checkout");
        }))), Map.of());
        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM " + spans + " | NESTED_EXPAND attributes | KEEP span_id")).close()
        );
        assertThat(e.getMessage(), containsString("NESTED_EXPAND requires a mapped nested field, not [attributes]"));
    }

    /** A collection operator with no KEEP after it returns the dataset's own columns, the collection unreadable as before. */
    public void testNestedAnyWithoutKeep() throws Exception {
        String dns = registerDataset("dns_nokeep", StoragePath.fileUri(write("dns", DNS, List.of(g -> {
            g.add("question", "www.example.com");
            answer(g.addGroup("answers"), "www.example.com", "CNAME", 30L, "lb.example.net");
        }, g -> {
            g.add("question", "api.example.com");
            answer(g.addGroup("answers"), "api.example.com", "A", 60L, "93.184.1.2");
        }))), Map.of());
        assertRows("FROM " + dns + " | WHERE NESTED_ANY(answers, answers.type == \"CNAME\")", row("www.example.com", null));
        assertRows(
            "FROM " + dns + " | WHERE NESTED_ANY(answers, answers.type == \"CNAME\") | KEEP question, answers",
            row("www.example.com", null)
        );
        assertRows(
            "FROM " + dns + " | NESTED_EXPAND answers | KEEP question, answers.type | SORT question",
            row("api.example.com", "A"),
            row("www.example.com", "CNAME")
        );
    }

    /** A sort and limit after the operator, over a dataset wide enough that the scan may defer the columns the sort does not read. */
    public void testCollectionBeforeTopN() throws Exception {
        MessageType wide = MessageTypeParser.parseMessageType("""
            message wide {
              required binary question (UTF8);
              optional binary c1 (UTF8);
              optional binary c2 (UTF8);
              optional binary c3 (UTF8);
              optional group answers (LIST) {
                repeated group list {
                  optional group element {
                    optional binary type (UTF8);
                    optional int64 ttl;
                  }
                }
              }
            }
            """);
        List<Consumer<Group>> rows = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            int n = i;
            rows.add(g -> {
                g.add("question", "q" + (char) ('a' + n));
                g.add("c1", "x");
                g.add("c2", "y");
                g.add("c3", "z");
                Group element = g.addGroup("answers").addGroup("list").addGroup("element");
                element.append("type", n % 2 == 0 ? "A" : "CNAME").append("ttl", (long) n);
            });
        }
        String dataset = registerDataset("wide", StoragePath.fileUri(write("wide", wide, rows)), Map.of());
        assertRows(
            "FROM " + dataset + " | NESTED_EXPAND answers | SORT question | LIMIT 3 | KEEP question, c1, answers.type, answers.ttl",
            row("qa", "x", "A", 0L),
            row("qb", "x", "CNAME", 1L),
            row("qc", "x", "A", 2L)
        );
        assertRows(
            "FROM " + dataset + " | WHERE NESTED_ANY(answers, answers.type == \"CNAME\") | SORT question | LIMIT 2 | KEEP question, c1",
            row("qb", "x"),
            row("qd", "x")
        );
    }

    /** A later file whose element field has another type is refused rather than read as the first file's type. */
    public void testElementTypeDiffersBetweenFiles() throws Exception {
        String template = """
            message spans {
              required binary span_id (UTF8);
              optional group measurements (LIST) {
                repeated group list {
                  optional group element {
                    optional binary name (UTF8);
                    optional %s value;
                  }
                }
              }
            }
            """;
        Path dir = createTempDir();
        write(dir, "a", MessageTypeParser.parseMessageType(Strings.format(template, "double")), List.of(g -> {
            g.add("span_id", "s1");
            g.addGroup("measurements").addGroup("list").addGroup("element").append("name", "latency").append("value", 1.5);
        }));
        write(dir, "b", MessageTypeParser.parseMessageType(Strings.format(template, "int64")), List.of(g -> {
            g.add("span_id", "s2");
            g.addGroup("measurements").addGroup("list").addGroup("element").append("name", "latency").append("value", 500L);
        }));
        String dataset = registerDataset("mixed", StoragePath.fileUri(dir) + "/*.parquet", Map.of());
        Exception e = expectThrows(
            Exception.class,
            () -> run(
                syncEsqlQueryRequest("FROM " + dataset + " | WHERE NESTED_ANY(measurements, measurements.value >= 500) | KEEP span_id")
            ).close()
        );
        assertThat(e.getMessage(), containsString("[measurements.value]"));
    }

    /**
     * Under {@code union_by_name} the element's fields are the union of every file's, so a field only a later file holds is
     * readable, and is null on the elements of the files that lack it.
     */
    public void testUnionByNameUnitesElementFields() throws Exception {
        Path dir = writeMeasurementFiles();
        String dataset = registerDataset("united", StoragePath.fileUri(dir) + "/*.parquet", Map.of("schema_resolution", "union_by_name"));
        assertRows("FROM " + dataset + " | WHERE NESTED_ANY(measurements, measurements.unit == \"ms\") | KEEP span_id", row("s2"));
        assertRows(
            "FROM " + dataset + " | NESTED_EXPAND measurements | KEEP span_id, measurements.name, measurements.unit",
            row("s1", "latency", null),
            row("s2", "latency", "ms")
        );
    }

    /** Under {@code strict} every file must describe the same element fields. */
    public void testStrictRefusesDifferentElementFields() throws Exception {
        Path dir = writeMeasurementFiles();
        String dataset = registerDataset("strict", StoragePath.fileUri(dir) + "/*.parquet", Map.of("schema_resolution", "strict"));
        Exception e = expectThrows(
            Exception.class,
            () -> run(syncEsqlQueryRequest("FROM " + dataset + " | NESTED_EXPAND measurements | KEEP span_id")).close()
        );
        assertThat(e.getMessage(), containsString("has nested columns"));
    }

    /** Two files whose measurements differ in one field: the second also records the unit. */
    private Path writeMeasurementFiles() throws IOException {
        String template = """
            message spans {
              required binary span_id (UTF8);
              optional group measurements (LIST) {
                repeated group list {
                  optional group element {
                    optional binary name (UTF8);
                    %s
                  }
                }
              }
            }
            """;
        Path dir = createTempDir();
        write(dir, "a", MessageTypeParser.parseMessageType(Strings.format(template, "")), List.of(g -> {
            g.add("span_id", "s1");
            g.addGroup("measurements").addGroup("list").addGroup("element").append("name", "latency");
        }));
        write(dir, "b", MessageTypeParser.parseMessageType(Strings.format(template, "optional binary unit (UTF8);")), List.of(g -> {
            g.add("span_id", "s2");
            g.addGroup("measurements").addGroup("list").addGroup("element").append("name", "latency").append("unit", "ms");
        }));
        return dir;
    }

    public void testCollectionInsideElementIsRefused() throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType("""
            message traces {
              required binary id (UTF8);
              optional group hops (LIST) {
                repeated group list {
                  optional group element {
                    optional binary host (UTF8);
                    optional group links (LIST) {
                      repeated group list {
                        optional group element {
                          optional binary to (UTF8);
                          optional int64 ms;
                        }
                      }
                    }
                  }
                }
              }
            }
            """);
        String traces = registerDataset("traces", StoragePath.fileUri(write("traces", schema, List.of(g -> {
            g.add("id", "a");
            Group hop = g.addGroup("hops").addGroup("list").addGroup("element").append("host", "web01");
            hop.addGroup("links").addGroup("list").addGroup("element").append("to", "db01").append("ms", 3L);
        }))), Map.of());
        Exception e = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("FROM " + traces + " | NESTED_EXPAND hops")).close());
        assertThat(e.getMessage(), containsString("is a collection with more than one field inside each element of [hops]"));
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

    private Path write(String name, MessageType schema, List<Consumer<Group>> rows) throws IOException {
        return write(createTempDir(), name, schema, rows);
    }

    private Path write(Path dir, String name, MessageType schema, List<Consumer<Group>> rows) throws IOException {
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
        Path file = dir.resolve(name + ".parquet");
        Files.write(file, baos.toByteArray());
        return file;
    }
}

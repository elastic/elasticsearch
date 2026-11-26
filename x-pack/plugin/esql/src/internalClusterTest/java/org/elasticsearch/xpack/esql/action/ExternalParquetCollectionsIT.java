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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * {@code NESTED_EXPAND} over Parquet collections, each a {@code LIST} of structures,
 * running the queries of the collection representation specification over files written here, each against the table
 * the specification prints beneath it.
 */
public class ExternalParquetCollectionsIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Before
    public void requireNestedExpandOverDatasets() {
        assumeTrue("NESTED_EXPAND over datasets is snapshot-only", EsqlCapabilities.Cap.NESTED_EXPAND_DATASET.isEnabled());
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

        // Which queries got a CNAME with a short TTL? Both conditions hold on the same answer, which is one row once expanded.
        assertRows(
            "FROM " + dns + " | NESTED_EXPAND answers | WHERE answers.type == \"CNAME\" AND answers.ttl < 60 | KEEP question",
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
            | NESTED_EXPAND attachments
            | WHERE attachments.file.mime_type == "application/pdf" AND attachments.file.size > 10000000
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
        // A map stays a column, unsupported and null, as an unsupported type does on an index
        assertRows("FROM " + spans + " | KEEP span_id, attributes", row("s1", null));
    }

    /**
     * A dataset's list of structures is not one of its columns, as an index's {@code nested} field is not: it is reached only
     * through {@code NESTED_EXPAND}. {@code NESTED_ANY} does not read it.
     */
    public void testNestedColumnIsNotAColumn() throws Exception {
        String dns = registerDataset("dns_hidden", StoragePath.fileUri(write("dns", DNS, List.of(g -> {
            g.add("question", "www.example.com");
            answer(g.addGroup("answers"), "www.example.com", "CNAME", 30L, "lb.example.net");
        }, g -> {
            g.add("question", "api.example.com");
            answer(g.addGroup("answers"), "api.example.com", "A", 60L, "93.184.1.2");
        }))), Map.of());
        assertRows("FROM " + dns, row("www.example.com"), row("api.example.com"));
        assertRows("FROM " + dns + " | KEEP *", row("www.example.com"), row("api.example.com"));
        assertQueryFails("FROM " + dns + " | KEEP question, answers", "Unknown column [answers]");
        assertQueryFails("FROM " + dns + " | DROP answers", "Unknown column [answers]");
        assertQueryFails("FROM " + dns + " | WHERE NESTED_ANY(answers, answers.type == \"CNAME\")", "Unknown column [answers.type]");
        assertRows(
            "FROM " + dns + " | NESTED_EXPAND answers | KEEP question, answers.type | SORT question",
            row("api.example.com", "A"),
            row("www.example.com", "CNAME")
        );
        // A plan that expands gives the dataset its nested scope; NESTED_ANY elsewhere in it must not bind to that scope
        assertQueryFails(
            "FROM " + dns + " | WHERE NESTED_ANY(answers, answers.type == \"CNAME\") | NESTED_EXPAND answers | KEEP question",
            "NESTED_ANY cannot read [answers] from a dataset"
        );
        assertQueryFails(
            "FROM " + dns + " | EVAL m = NESTED_ANY(answers, answers.ttl < 60) | NESTED_EXPAND answers",
            "NESTED_ANY cannot read [answers] from a dataset"
        );
    }

    /**
     * A Hive partition key that shares a list's name shadows the file's column, as it shadows any column: the partition value
     * reads in its place, and the list is not expanded.
     */
    public void testPartitionShadowsNestedColumn() throws Exception {
        Path root = createTempDir().resolve("shadow");
        write(root.resolve("answers=2024"), "f", DNS, List.of(g -> {
            g.add("question", "www.example.com");
            answer(g.addGroup("answers"), "www.example.com", "CNAME", 30L, "lb.example.net");
        }));
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.parquet";
        String dataset = registerDataset("shadow", glob, Map.of("partition_detection", "hive"));
        assertRows("FROM " + dataset + " | KEEP question, answers", row("www.example.com", 2024L));
        assertQueryFails(
            "FROM " + dataset + " | NESTED_EXPAND answers",
            "NESTED_EXPAND cannot use [answers]: a column of that name replaces it here"
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
    }

    /**
     * An element field's type is inferred, as a column's is: a later file that stores a type widening to the first
     * file's converts each value, as a top-level column does, and one that stores a type it does not widen to reads as
     * null there, with a warning, under every error policy.
     */
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
        String widening = registerDataset(
            "mixed",
            StoragePath.fileUri(dir) + "/*.parquet",
            Map.of("schema_resolution", "first_file_wins", "file_sort_by", "name")
        );
        assertRows(
            "FROM " + widening + " | NESTED_EXPAND measurements | KEEP span_id, measurements.value",
            row("s1", 1.5),
            row("s2", 500.0)
        );

        Path narrowing = createTempDir();
        write(narrowing, "a", MessageTypeParser.parseMessageType(Strings.format(template, "int64")), List.of(g -> {
            g.add("span_id", "s1");
            g.addGroup("measurements").addGroup("list").addGroup("element").append("name", "latency").append("value", 500L);
        }));
        write(narrowing, "b", MessageTypeParser.parseMessageType(Strings.format(template, "double")), List.of(g -> {
            g.add("span_id", "s2");
            g.addGroup("measurements").addGroup("list").addGroup("element").append("name", "latency").append("value", 1.5);
        }));
        String narrowed = registerDataset(
            "narrowed",
            StoragePath.fileUri(narrowing) + "/*.parquet",
            Map.of("schema_resolution", "first_file_wins", "file_sort_by", "name")
        );
        Result result = runWithWarnings("FROM " + narrowed + " | NESTED_EXPAND measurements | KEEP span_id, measurements.value");
        assertThat(result.rows(), containsInAnyOrder(row("s1", 500L), row("s2", null)));
        assertThat(result.warnings(), hasItem(containsString("[measurements.value]")));
    }

    /**
     * Under {@code union_by_name} the element's fields are the union of every file's, so a field only a later file holds is
     * readable, and is null on the elements of the files that lack it.
     */
    public void testUnionByNameUnitesElementFields() throws Exception {
        Path dir = writeMeasurementFiles();
        String dataset = registerDataset("united", StoragePath.fileUri(dir) + "/*.parquet", Map.of("schema_resolution", "union_by_name"));
        assertRows("FROM " + dataset + " | NESTED_EXPAND measurements | WHERE measurements.unit == \"ms\" | KEEP span_id", row("s2"));
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

    private static final MessageType ORDERS = MessageTypeParser.parseMessageType("""
        message orders {
          required int64 id;
          required int64 price;
          optional binary msg (UTF8);
          optional group tags (LIST) {
            repeated group list {
              optional binary element (UTF8);
            }
          }
          optional group items (LIST) {
            repeated group list {
              optional group element {
                optional binary sku (UTF8);
                optional int64 qty;
              }
            }
          }
          optional group parts (LIST) {
            repeated group list {
              optional group element {
                optional binary name (UTF8);
              }
            }
          }
        }
        """);

    /** Parents of the orders fixture. */
    private static final int ORDERS_PARENTS = 50;
    /** The first parent's elements, more than a LIMIT below asks for, so a limit counting elements as parents would show. */
    private static final int FIRST_ORDER_ITEMS = 40;

    /**
     * The orders fixture: parent {@code id} 1 to 50, {@code price} 10 up to id 40 and 200 after, {@code msg} "order-id".
     * Parent 1 has 40 items, parent 3 an empty list, parent 4 none, every other parent two, of quantity {@code id} and
     * {@code id + 100}; every parent has the tags {@code t1} and {@code t2} and one part. Row groups are small, so the
     * file has several.
     */
    private String orders(String name) throws IOException {
        List<Consumer<Group>> rows = new ArrayList<>();
        for (long id = 1; id <= ORDERS_PARENTS; id++) {
            long n = id;
            rows.add(g -> {
                g.add("id", n);
                g.add("price", n <= 40 ? 10L : 200L);
                g.add("msg", "order-" + n);
                Group tags = g.addGroup("tags");
                tags.addGroup("list").append("element", "t1");
                tags.addGroup("list").append("element", "t2");
                g.addGroup("parts").addGroup("list").addGroup("element").append("name", "p" + n);
                if (n == 4) {
                    return;
                }
                Group items = g.addGroup("items");
                for (long q : orderQuantities(n)) {
                    items.addGroup("list").addGroup("element").append("sku", "s" + n + "-" + q).append("qty", q);
                }
            });
        }
        return registerDataset(name, StoragePath.fileUri(write(createTempDir(), name, ORDERS, rows, 512)), Map.of());
    }

    /** The item quantities of order {@code id}, in list order; empty for orders 3 (an empty list) and 4 (no list). */
    private static List<Long> orderQuantities(long id) {
        if (id == 3 || id == 4) {
            return List.of();
        }
        if (id == 1) {
            List<Long> quantities = new ArrayList<>();
            for (long q = 1; q <= FIRST_ORDER_ITEMS; q++) {
                quantities.add(q);
            }
            return quantities;
        }
        return List.of(id, id + 100);
    }

    /** The expanded rows of the orders fixture: one per item, or one null row for an order without items. */
    private static long orderRows() {
        long rows = 0;
        for (long id = 1; id <= ORDERS_PARENTS; id++) {
            rows += Math.max(1, orderQuantities(id).size());
        }
        return rows;
    }

    /** A parent without elements keeps one row, whose element columns are null: an empty list and no list alike. */
    public void testEmptyListKeepsOneNullRow() throws Exception {
        String orders = orders("orders_empty");
        assertRows(
            "FROM " + orders + " | NESTED_EXPAND items | WHERE id == 3 OR id == 4 | KEEP id, items.sku, items.qty",
            row(3L, null, null),
            row(4L, null, null)
        );
    }

    /**
     * A {@code LIMIT} after the expansion counts rows, and its copy below the expansion counts parents, so a first parent
     * with more elements than the limit does not end the read: exactly the limit comes back, and all rows when it is above
     * them.
     */
    public void testLimitAfterExpansionCountsRows() throws Exception {
        String orders = orders("orders_limit");
        for (int limit : new int[] { 5, FIRST_ORDER_ITEMS + 10, 1000 }) {
            assertRows(
                "FROM " + orders + " | NESTED_EXPAND items | LIMIT " + limit + " | STATS c = COUNT(*)",
                row(Math.min(limit, orderRows()))
            );
        }
    }

    /** A {@code LIMIT} before the expansion runs on the coordinator, which cannot expand, as on an index. */
    public void testLimitBeforeExpansionIsRefused() throws Exception {
        String orders = orders("orders_limit_before");
        assertQueryFails("FROM " + orders + " | LIMIT 2 | NESTED_EXPAND items", "NESTED_EXPAND must run on the data nodes");
    }

    /**
     * N1: the limit under the expansion is not applied to the parents when a filter the reader cannot answer sits below
     * it. The first 40 parents fail the filter; a parent limit of 3 read before the filter would leave none.
     */
    public void testFilterOnParentBeforeExpansion() throws Exception {
        String orders = orders("orders_filter_parent");
        try (
            var response = run(syncEsqlQueryRequest("FROM " + orders + " | WHERE price % 7 == 4 | NESTED_EXPAND items | LIMIT 3 | KEEP id"))
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(3));
            for (List<Object> r : rows) {
                assertThat(((Number) r.getFirst()).longValue(), greaterThan(40L));
            }
        }
        // The same filter as the reader can answer it
        assertRows("FROM " + orders + " | WHERE price > 100 | NESTED_EXPAND items | STATS c = COUNT(*)", row(2L * (ORDERS_PARENTS - 40)));
    }

    /** N5: a predicate on an element column is a filter on the expanded rows, never a filter on the file's columns. */
    public void testFilterOnElementAfterExpansion() throws Exception {
        String orders = orders("orders_filter_element");
        long expected = 0;
        for (long id = 1; id <= ORDERS_PARENTS; id++) {
            expected += orderQuantities(id).stream().filter(q -> q > 30).count();
        }
        assertRows("FROM " + orders + " | NESTED_EXPAND items | WHERE items.qty > 30 | STATS c = COUNT(*)", row(expected));
        List<List<?>> firstOrders = new ArrayList<>();
        for (long id = 1; id < 5; id++) {
            for (long q : orderQuantities(id)) {
                if (q > 30) {
                    firstOrders.add(row(id, q));
                }
            }
        }
        assertRows(
            "FROM " + orders + " | NESTED_EXPAND items | WHERE items.qty > 30 AND id < 5 | KEEP id, items.qty",
            firstOrders.toArray(List<?>[]::new)
        );
    }

    public void testSortAndLimitAfterExpansion() throws Exception {
        String orders = orders("orders_topn");
        assertRows(
            "FROM " + orders + " | NESTED_EXPAND items | SORT items.qty DESC NULLS LAST | LIMIT 3 | KEEP id, items.qty",
            row(50L, 150L),
            row(49L, 149L),
            row(48L, 148L)
        );
    }

    /**
     * A count or sum after the expansion is over the expanded rows, which the file's footer does not describe: it holds
     * 50 rows, and a price per row, not per element.
     */
    public void testStatsAfterExpansionAreNotTheFooter() throws Exception {
        String orders = orders("orders_count");
        long sum = 0;
        for (long id = 1; id <= ORDERS_PARENTS; id++) {
            sum += (id <= 40 ? 10L : 200L) * Math.max(1, orderQuantities(id).size());
        }
        assertRows(
            "FROM " + orders + " | NESTED_EXPAND items | STATS c = COUNT(*), n = COUNT(price), s = SUM(price)",
            row(orderRows(), orderRows(), sum)
        );
        assertRows("FROM " + orders + " | STATS c = COUNT(*)", row((long) ORDERS_PARENTS));
    }

    /**
     * A maximum over the elements of some parents: the footer's statistics of the quantity column describe the elements
     * of every parent, whose largest is 150, so an answer read from them would be wrong for each filtered query here.
     */
    public void testMaxOfElementIsNotTheFooter() throws Exception {
        String orders = orders("orders_max");
        assertRows("FROM " + orders + " | NESTED_EXPAND items | STATS m = MAX(items.qty)", row(150L));
        assertRows("FROM " + orders + " | WHERE id <= 40 | NESTED_EXPAND items | STATS m = MAX(items.qty)", row(140L));
        assertRows("FROM " + orders + " | WHERE id == 1 | NESTED_EXPAND items | STATS m = MAX(items.qty), c = COUNT(*)", row(40L, 40L));
    }

    /** Per-row commands before the expansion run on the expanded rows, each a copy of its parent's. */
    public void testPerRowCommandsBeforeExpansion() throws Exception {
        String orders = orders("orders_dissect");
        assertRows(
            Strings.format("""
                FROM %s
                | DISSECT msg "%%{kind}-%%{n}"
                | GROK msg "%%{WORD:word}-%%{INT:num:int}"
                | EVAL k = id * 10
                | WHERE id == 2
                | NESTED_EXPAND items
                | KEEP kind, n, word, num, k, items.sku""", orders),
            row("order", "2", "order", 2L, 20L, "s2-2"),
            row("order", "2", "order", 2L, 20L, "s2-102")
        );
    }

    public void testMvExpandBeforeExpansion() throws Exception {
        String orders = orders("orders_mv_expand");
        assertRows(
            "FROM " + orders + " | WHERE id == 2 | MV_EXPAND tags | NESTED_EXPAND items | KEEP id, tags, items.qty",
            row(2L, "t1", 2L),
            row(2L, "t1", 102L),
            row(2L, "t2", 2L),
            row(2L, "t2", 102L)
        );
    }

    /** Commands that read more than their own row cannot sit below an expansion that the scan does. */
    public void testCommandsTheExpansionCannotFollow() throws Exception {
        String orders = orders("orders_refused");
        assertQueryFails(
            "FROM " + orders + " | SAMPLE 0.5 | NESTED_EXPAND items",
            "NESTED_EXPAND over a dataset cannot follow [SAMPLE 0.5]"
        );
        // CHANGE_POINT reads its neighbours, so it is planned on the coordinator, which no expansion follows, as on an index
        assertQueryFails(
            "FROM " + orders + " | CHANGE_POINT price ON id | NESTED_EXPAND items",
            "NESTED_EXPAND must run on the data nodes, so it cannot follow [CHANGE_POINT price ON id]"
        );
        assertQueryFails(
            "FROM " + orders + " | NESTED_EXPAND items | NESTED_EXPAND parts",
            "NESTED_EXPAND over a dataset reads one collection per query"
        );
    }

    /** A partitioned dataset adds its partition column beside the parents' columns, and the element columns follow them. */
    public void testPartitionedDatasetExpands() throws Exception {
        Path root = createTempDir().resolve("partitioned");
        write(root.resolve("region=eu"), "f", DNS, List.of(g -> {
            g.add("question", "www.example.com");
            Group answers = g.addGroup("answers");
            answer(answers, "www.example.com", "CNAME", 30L, "lb.example.net");
            answer(answers, "lb.example.net", "A", 300L, "93.184.1.1");
        }));
        write(root.resolve("region=us"), "f", DNS, List.of(g -> {
            g.add("question", "api.example.com");
            answer(g.addGroup("answers"), "api.example.com", "A", 60L, "93.184.1.2");
        }));
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.parquet";
        String dataset = registerDataset("partitioned", glob, Map.of("partition_detection", "hive"));
        assertRows(
            "FROM " + dataset + " | NESTED_EXPAND answers | KEEP region, question, answers.type, answers.ttl",
            row("eu", "www.example.com", "CNAME", 30L),
            row("eu", "www.example.com", "A", 300L),
            row("us", "api.example.com", "A", 60L)
        );
        assertRows("FROM " + dataset + " | NESTED_EXPAND answers | WHERE region == \"eu\" | STATS c = COUNT(*)", row(2L));
    }

    /**
     * W2 and the wrapper chain under {@code skip_row}: a parent value that a {@code union_by_name} cast cannot represent
     * drops the parent, every row of it, and the error budget counts that as one error, as it does for the file's row
     * without the expansion. Parent {@code bad} has three elements and a timestamp past what nanoseconds hold, so
     * charging per element would spend a budget of two.
     */
    public void testSkipRowDropsParentsAndChargesOncePerParent() throws Exception {
        String template = """
            message events {
              required binary id (UTF8);
              optional int64 ts (TIMESTAMP(%s,true));
              optional group items (LIST) {
                repeated group list {
                  optional group element {
                    optional binary sku (UTF8);
                  }
                }
              }
            }
            """;
        Path dir = createTempDir();
        // 2300-01-01T00:00:00Z in milliseconds, which has no date_nanos value
        long beyondNanos = 10_413_792_000_000L;
        write(dir, "a", MessageTypeParser.parseMessageType(Strings.format(template, "MILLIS")), List.of(g -> {
            g.add("id", "bad");
            g.add("ts", beyondNanos);
            Group items = g.addGroup("items");
            for (String sku : List.of("x", "y", "z")) {
                items.addGroup("list").addGroup("element").append("sku", sku);
            }
        }, g -> {
            g.add("id", "good");
            g.add("ts", 1_000L);
            Group items = g.addGroup("items");
            items.addGroup("list").addGroup("element").append("sku", "g1");
            items.addGroup("list").addGroup("element").append("sku", "g2");
        }));
        write(dir, "b", MessageTypeParser.parseMessageType(Strings.format(template, "NANOS")), List.of(g -> {
            g.add("id", "nanos");
            g.add("ts", 5L);
            g.addGroup("items").addGroup("list").addGroup("element").append("sku", "n1");
        }));
        String dataset = registerDataset(
            "skip_row_budget",
            StoragePath.fileUri(dir) + "/*.parquet",
            Map.of("schema_resolution", "union_by_name", "error_mode", "skip_row", "max_errors", 2)
        );
        assertRows(
            "FROM " + dataset + " | NESTED_EXPAND items | EVAL timed = ts IS NOT NULL | KEEP id, timed, items.sku",
            row("good", true, "g1"),
            row("good", true, "g2"),
            row("nanos", true, "n1")
        );
        String strict = registerDataset(
            "skip_row_no_budget",
            StoragePath.fileUri(dir) + "/*.parquet",
            Map.of("schema_resolution", "union_by_name", "error_mode", "skip_row", "max_errors", 0)
        );
        assertQueryFails(
            "FROM " + strict + " | NESTED_EXPAND items | EVAL timed = ts IS NOT NULL | KEEP id, timed, items.sku",
            "dropped rows"
        );
    }

    /** A plan that runs on several data nodes reads the expansion in each node's scan. */
    public void testMultiNode() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        Path dir = createTempDir();
        for (int f = 0; f < 4; f++) {
            int file = f;
            write(dir, "f" + f, DNS, List.of(g -> {
                g.add("question", "q" + file);
                Group answers = g.addGroup("answers");
                answer(answers, "a" + file, "A", 10L * file, null);
                answer(answers, "b" + file, "CNAME", 10L * file + 1, null);
            }));
        }
        String dataset = registerDataset("multi_node", StoragePath.fileUri(dir) + "/*.parquet", Map.of());
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "round_robin").build());
        var request = syncEsqlQueryRequest(
            "FROM " + dataset + " | NESTED_EXPAND answers | STATS c = COUNT(*), t = SUM(answers.ttl) BY answers.type"
        );
        request.pragmas(pragmas);
        request.acceptedPragmaRisks(true);
        request.profile(true);
        try (var response = run(request)) {
            assertThat(externalScanNodeNames(response).size(), greaterThanOrEqualTo(2));
            assertThat(getValuesList(response), containsInAnyOrder(row(4L, 60L, "A"), row(4L, 64L, "CNAME")));
        }
    }

    private record Result(List<List<Object>> rows, List<String> warnings) {}

    /** Runs {@code query} on a random node and returns its rows and the warnings that node's response carried. */
    private Result runWithWarnings(String query) throws Exception {
        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        AtomicReference<Result> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        // The transport client owns the response's ref-count, so it is read here and not closed
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query), ActionListener.wrap(response -> {
            try {
                ThreadContext threadContext = internalCluster().getInstance(TransportService.class, coordinator.getName())
                    .getThreadPool()
                    .getThreadContext();
                result.set(new Result(getValuesList(response), threadContext.getResponseHeaders().getOrDefault("Warning", List.of())));
            } finally {
                latch.countDown();
            }
        }, e -> {
            failure.set(e);
            latch.countDown();
        }));
        assertTrue("query did not complete within timeout", latch.await(30, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw failure.get();
        }
        return result.get();
    }

    private void assertQueryFails(String query, String message) {
        Exception e = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest(query)).close());
        assertThat(query, e.getMessage(), containsString(message));
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
        return write(dir, name, schema, rows, ParquetWriter.DEFAULT_BLOCK_SIZE);
    }

    private Path write(Path dir, String name, MessageType schema, List<Consumer<Group>> rows, int rowGroupSize) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(rowGroupSize)
                .build()
        ) {
            for (Consumer<Group> row : rows) {
                Group g = factory.newGroup();
                row.accept(g);
                writer.write(g);
            }
        }
        Files.createDirectories(dir);
        Path file = dir.resolve(name + ".parquet");
        Files.write(file, baos.toByteArray());
        return file;
    }
}

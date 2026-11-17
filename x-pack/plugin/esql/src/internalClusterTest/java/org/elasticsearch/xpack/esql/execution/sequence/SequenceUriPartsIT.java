/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/** Ordinary ES|QL is the oracle for identical indexed URI cells, including malformed and multivalued inputs. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceUriPartsIT extends AbstractSequenceIntegTestCase {
    private static final String COLUMNS = "id,parsed.domain,parsed.fragment,parsed.path,parsed.extension,parsed.port,"
        + "parsed.query,parsed.scheme,parsed.user_info,parsed.username,parsed.password";
    private static final Pattern EXCEPTION_CLASS = Pattern.compile("(?:[A-Za-z_$][\\w$]*\\.)+[A-Za-z_$][\\w$]*(?:Exception|Error)");
    // Preserve the review table's literals. Its predicted domains are not expected results.
    private static final List<String> URLS = List.of(
        "https://github.com:bad/a.msi",
        "//github.com/a.msi",
        "https://evil_sub.ngrok.io/a.msi",
        "https://a.b_c.workers.dev/",
        "https://a@b@x.limewire.com/a.msi",
        "https://x.limewire.com:abc/",
        "https://x.limewire.com:99999999999/",
        "https://-x.limewire.com/",
        "https://x..limewire.com/",
        "https://evil.example\\@limewire.com/a.msi",
        "[::1]",
        "https://[::1]/a.msi",
        "//[::1]/a.msi",
        "https://example.com:8443/a.msi",
        "https://user:pass@example.com/a.msi",
        "HTTPS://EXAMPLE.COM/a.msi",
        "https://example.com./a.msi",
        "example.com/a.msi",
        "https://example.com//a.msi",
        "https://example.com/a.msi?user=@other.example#@fragment.example",
        ""
    );

    /** An unrelated scalar ID retains every comparison row even if URI parsing returns null. */
    public void testAllColumnsAndWarningClassesMatchOrdinary() {
        String index = "sequence-uri-literals";
        createFixture(index);
        for (int primary : List.of(1, 2)) {
            String phase = primary == 1 ? "A" : "B";
            assertParity(
                observe(stage(index, phase, "") + " | URI_PARTS parsed=url | KEEP " + COLUMNS + " | LIMIT 100"),
                observe(sequence(index, primary, "URI_PARTS parsed=url", "URI_PARTS parsed=url") + " | KEEP " + COLUMNS + " | LIMIT 100"),
                URLS.size() + 4
            );
        }
        // Compare per-input warning classes too, so warning limits or different row order cannot hide one input's warning.
        for (String id : ids()) {
            String predicate = " AND id==\"" + id + "\"";
            var ordinary = observe(stage(index, "A", predicate) + " | URI_PARTS parsed=url | KEEP " + COLUMNS + " | LIMIT 100");
            var nativeResult = observe(
                sequence(index, 1, "WHERE id==\"" + id + "\" | URI_PARTS parsed=url", "") + " | KEEP " + COLUMNS + " | LIMIT 100"
            );
            assertParity(ordinary, nativeResult, 1);
            if (id.equals("multivalue")) {
                assertTrue(ordinary.warningClasses().contains("java.lang.IllegalArgumentException"));
            }
        }
    }

    /** Existing projection, alias and MEMBER paths must retain the compound evaluator's required inputs. */
    public void testPrunedAliasedDroppedAndMemberColumns() {
        String index = "sequence-uri-projections";
        createFixture(index);
        String keep = "id,parsed.domain,parsed.port";
        String pipeline = "EVAL alias=url | URI_PARTS parsed=alias | DROP alias,url";
        assertParity(
            observe(stage(index, "A", "") + " | " + pipeline + " | KEEP " + keep + " | LIMIT 100"),
            observe(sequence(index, 1, pipeline, "KEEP id") + " | KEEP " + keep + " | LIMIT 100"),
            URLS.size() + 4
        );
        String projected = "URI_PARTS parsed=url | KEEP " + keep;
        assertParity(
            observe(stage(index, "A", "") + " | " + projected + " | LIMIT 100"),
            observe(sequence(index, 1, projected, "KEEP id") + " | KEEP " + keep + " | LIMIT 100"),
            URLS.size() + 4
        );
        assertParity(
            observe(
                stage(index, "B", "")
                    + " | URI_PARTS parsed=url | EVAL other_domain=parsed.domain,other_port=parsed.port"
                    + " | KEEP id,other_domain,other_port | LIMIT 100"
            ),
            observe(
                sequence(index, 1, "KEEP id", pipeline)
                    + " | EVAL other_domain=MEMBER(2,parsed.domain),other_port=MEMBER(2,parsed.port)"
                    + " | KEEP id,other_domain,other_port | LIMIT 100"
            ),
            URLS.size() + 4
        );
    }

    /** A URI output can participate in a key and a native cross-member condition without a separate reader. */
    public void testDerivedKeyAndConditionUseOrdinaryUriValues() {
        String index = "sequence-uri-derived";
        createFixture(index);
        String selected = "WHERE id==\"u13\" | URI_PARTS parsed=url";
        var ordinary = observe(
            stage(index, "A", " AND id==\"u13\"") + " | URI_PARTS parsed=url | KEEP id,parsed.domain,parsed.port | LIMIT 100"
        );
        String correlated = sequence(index, 1, selected, selected + " | WHERE parsed.domain==MEMBER(1,parsed.domain)");
        assertParity(ordinary, observe(correlated + " | KEEP id,parsed.domain,parsed.port | LIMIT 100"), 1);
        String keyed = sequence(index, 1, selected, selected).replace("SEQUENCE BY id", "SEQUENCE BY parsed.domain");
        assertParity(ordinary, observe(keyed + " | KEEP id,parsed.domain,parsed.port | LIMIT 100"), 1);
    }

    private static String stage(String index, String phase, String predicate) {
        return "FROM " + index + " | WHERE phase==\"" + phase + "\"" + predicate;
    }

    private static String sequence(String index, int primary, String first, String second) {
        return "SEQUENCE BY id WITHIN 30s PRIMARY "
            + primary
            + " ("
            + stage(index, "A", "")
            + (first.isEmpty() ? "" : " | " + first)
            + "), ("
            + stage(index, "B", "")
            + (second.isEmpty() ? "" : " | " + second)
            + ")";
    }

    private void assertParity(Observed ordinary, Observed sequence, int rows) {
        assertEquals(rows, ordinary.values().size());
        assertEquals(ordinary.columns(), sequence.columns());
        assertEquals(ordinary.types(), sequence.types());
        assertEquals(ordinary.values(), sequence.values());
        assertEquals(ordinary.nativeValues(), sequence.nativeValues());
        assertEquals(ordinary.warningClasses(), sequence.warningClasses());
        assertEquals(ordinary.functionWarnings(), sequence.functionWarnings());
    }

    private Observed observe(String query) {
        var coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        var future = new PlainActionFuture<Observed>();
        var pragmas = new QueryPragmas(Settings.builder().put("sequence_uri_parts", true).put("page_size", 3).build());
        client(coordinator.getName()).execute(
            EsqlQueryAction.INSTANCE,
            syncEsqlQueryRequest(query).pragmas(pragmas),
            ActionListener.wrap(response -> {
                // The transport listener borrows this response; respondAndRelease owns its eventual release.
                // Detach every value and warning before returning, without closing an unacquired reference.
                var context = internalCluster().getInstance(TransportService.class, coordinator.getName())
                    .getThreadPool()
                    .getThreadContext();
                List<String> rawWarnings = context.getResponseHeaders()
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().equalsIgnoreCase("Warning"))
                    .flatMap(entry -> entry.getValue().stream())
                    .toList();
                logger.info("uri_parts_qualification query=[{}] raw_warning_headers=[{}]", query, rawWarnings);
                Set<String> warningClasses = new TreeSet<>();
                Set<String> functionWarnings = new TreeSet<>();
                List<String> uncomparedWarnings = new ArrayList<>();
                for (String header : rawWarnings) {
                    String warning = HeaderWarning.extractWarningValueFromWarningHeader(header, false);
                    var matcher = EXCEPTION_CLASS.matcher(warning);
                    if (warning.startsWith("Line ") && matcher.find()) {
                        // Compare full function exception details, excluding query-source coordinates and the wire envelope.
                        // Distinct details ignore multiplicity; complete raw headers, summaries and node warnings stay retained.
                        functionWarnings.add(warning.substring(matcher.start()));
                        do {
                            warningClasses.add(matcher.group());
                        } while (matcher.find());
                    } else {
                        uncomparedWarnings.add(warning);
                    }
                }
                logger.info(
                    "uri_parts_qualification query=[{}] function_exception_details=[{}] retained_uncompared_warnings=[{}]",
                    query,
                    functionWarnings,
                    uncomparedWarnings
                );
                var values = getValuesList(response);
                future.onResponse(
                    new Observed(
                        response.columns().stream().map(column -> column.name()).toList(),
                        response.columns().stream().map(column -> column.type()).toList(),
                        byId(values, values),
                        byId(values, nativeRows(response)),
                        warningClasses,
                        functionWarnings,
                        rawWarnings,
                        uncomparedWarnings
                    )
                );
            }, future::onFailure)
        );
        return future.actionGet(DEFAULT_REQUEST_TIMEOUT);
    }

    private static Map<String, List<Object>> byId(List<List<Object>> ids, List<List<Object>> rows) {
        Map<String, List<Object>> result = new TreeMap<>();
        for (int row = 0; row < rows.size(); row++) {
            if (result.put((String) ids.get(row).getFirst(), rows.get(row)) != null) {
                throw new IllegalStateException("Fixture emitted a duplicate ID");
            }
        }
        return result;
    }

    private static List<List<Object>> nativeRows(EsqlQueryResponse response) {
        List<List<Object>> rows = new ArrayList<>();
        for (var page : response.pages()) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                List<Object> row = new ArrayList<>();
                for (int column = 0; column < page.getBlockCount(); column++) {
                    Object value = BlockUtils.toJavaObject(page.getBlock(column), position);
                    row.add(value instanceof BytesRef bytes ? BytesRef.deepCopyOf(bytes) : value);
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private static List<String> ids() {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < URLS.size(); i++) {
            ids.add(Strings.format("u%02d", i));
        }
        ids.addAll(List.of("null", "missing", "multivalue", "singleton"));
        return ids;
    }

    private void createFixture(String index) {
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping("@timestamp", "type=date_nanos", "id", "type=keyword", "phase", "type=keyword", "url", "type=keyword")
        );
        var bulk = client().prepareBulk();
        List<String> ids = ids();
        for (String phase : List.of("A", "B")) {
            for (int i = 0; i < ids.size(); i++) {
                Map<String, Object> source = new LinkedHashMap<>();
                source.put("@timestamp", phase.equals("A") ? "2026-11-16T00:00:00.000000001Z" : "2026-11-16T00:00:00.000000002Z");
                source.put("id", ids.get(i));
                source.put("phase", phase);
                if (i < URLS.size()) {
                    source.put("url", URLS.get(i));
                } else {
                    switch (ids.get(i)) {
                        case "null" -> source.put("url", null);
                        case "missing" -> {
                            /* Keep the mapped field absent in this document. */ }
                        case "multivalue" -> source.put("url", List.of(URLS.get(13), URLS.get(14)));
                        case "singleton" -> source.put("url", List.of(URLS.get(13)));
                        default -> throw new AssertionError(ids.get(i));
                    }
                }
                bulk.add(prepareIndex(index).setId(phase + ids.get(i)).setSource(source));
            }
        }
        assertFalse(bulk.get().hasFailures());
        refresh(index);
    }

    /** Every response must release its native blocks and PIT search contexts after successful or failed qualification. */
    @After
    public void assertSequenceResourcesReleased() throws Exception {
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals(0, service.getActiveContexts());
            }
        });
        ensureBlocksReleased();
    }

    /** Values and warnings are detached before the borrowed response returns to its transport owner. */
    private record Observed(
        List<String> columns,
        List<DataType> types,
        Map<String, List<Object>> values,
        Map<String, List<Object>> nativeValues,
        Set<String> warningClasses,
        Set<String> functionWarnings,
        List<String> rawWarnings,
        List<String> uncomparedWarnings
    ) {}
}

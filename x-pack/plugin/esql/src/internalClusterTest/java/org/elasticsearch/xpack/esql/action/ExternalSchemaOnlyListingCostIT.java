/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * What a first look at a dataset costs, per resolution mode and partition setting.
 * <p>
 * {@code LIMIT 0} reads no rows — {@code SkipQueryOnLimitZero} removes the compute and split discovery
 * never runs — so everything measured here is schema resolution. It is also the query a UI issues
 * before any other, against a dataset nobody has touched, which is why only the <em>cold</em> number is
 * reported: each cell gets its own directory and its own dataset, is queried exactly once, and shares
 * no listing-cache entry with any other cell. A warm repeat reads a cached listing and says nothing
 * about what a user waits for.
 * <p>
 * The cells separate the two things a schema answer needs. Data columns come from the declaration when
 * there is one, from one anchor file under {@code first_file_wins}, and from every file under
 * {@code union_by_name} and {@code strict} — the last two by contract, so their cost is honest and they
 * are the control the others are read against. Partition columns are path-derived and, under
 * {@code AUTO}, are typed by folding over every listed path, so a fully declared dataset still pays for
 * them. {@code partition_detection: none} is the config stating that fold has nothing to find; a
 * declared dataset that also sets it has its complete answer before touching storage.
 * <p>
 * A measurement harness, not an assertion of current behaviour: it prints a table and asserts only that
 * every cell answers correctly, so it stays green while the numbers move.
 * <p>
 * These are local-filesystem numbers, and that bounds what they can show. The local provider's iterator walks
 * the whole tree into a list before the first entry is handed out, so a bounded listing here performs the same
 * walk as an unbounded one and the bound caps only what is kept. The listing saving is an object-store effect —
 * a request never issued — and this harness cannot show it. What it does show is the per-file work that follows
 * the listing.
 */
public class ExternalSchemaOnlyListingCostIT extends AbstractExternalDataSourceIT {

    private static final int[] FILE_COUNTS = { 1000, 4000, 16000 };

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        // The shipped 10,000 default refuses the glob outright at the wider counts — the cap applies to
        // schema resolution too, not only to reads. Raised here so the slope can be measured past it.
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ExternalSourceSettings.MAX_DISCOVERED_FILES.getKey(), 200_000)
            .build();
    }

    /** {@code resolution == null} means a fully declared mapping, which bypasses schema_resolution. */
    private record Cell(String label, String resolution, boolean partitionsOff) {
        boolean declared() {
            return resolution == null;
        }
    }

    public void testColdSchemaOnlyCostBySchemaAndPartitionConfig() throws Exception {
        List<Cell> cells = List.of(
            new Cell("declared + partition none", null, true),
            new Cell("declared + partition AUTO", null, false),
            new Cell("first_file_wins + partition none", "first_file_wins", true),
            new Cell("first_file_wins + partition AUTO", "first_file_wins", false),
            new Cell("strict + partition none", "strict", true),
            new Cell("strict + partition AUTO", "strict", false),
            new Cell("union_by_name + partition none", "union_by_name", true),
            new Cell("union_by_name + partition AUTO", "union_by_name", false)
        );

        Map<String, long[]> millisByCell = new LinkedHashMap<>();
        for (Cell cell : cells) {
            millisByCell.put(cell.label(), new long[FILE_COUNTS.length]);
        }

        for (int n = 0; n < FILE_COUNTS.length; n++) {
            int fileCount = FILE_COUNTS[n];
            for (Cell cell : cells) {
                // Its own directory, so this cell's query is the first to list these paths and no other
                // cell can have warmed a listing entry for them.
                Path dir = createTempDir();
                for (int f = 0; f < fileCount; f++) {
                    writeCsv(dir.resolve(String.format(Locale.ROOT, "part-%06d.csv", f)));
                }
                String glob = StoragePath.fileUri(dir) + "/*.csv";

                String name = "cold_" + (cell.declared() ? "decl" : cell.resolution()) + (cell.partitionsOff() ? "_nopart_" : "_auto_") + n;
                Map<String, Object> settings = new LinkedHashMap<>();
                settings.put("format", "csv");
                if (cell.partitionsOff()) {
                    settings.put("partition_detection", "none");
                }
                if (cell.declared() == false) {
                    settings.put("schema_resolution", cell.resolution());
                }

                String dataset;
                if (cell.declared()) {
                    LinkedHashMap<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
                    properties.put("id", new DatasetFieldMapping("long", null));
                    properties.put("v", new DatasetFieldMapping("keyword", null));
                    dataset = registerStrictDataset(name, glob, properties, settings);
                } else {
                    dataset = registerLocalFileDataset(name, glob, settings);
                }

                long startNanos = System.nanoTime();
                try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM " + dataset + " | LIMIT 0"), TIMEOUT)) {
                    millisByCell.get(cell.label())[n] = (System.nanoTime() - startNanos) / 1_000_000L;
                    assertThat("a schema-only query returns no rows", getValuesList(response).size(), equalTo(0));
                    assertThat("the columns are still answered", response.columns().size(), greaterThanOrEqualTo(2));
                }
            }
        }

        StringBuilder table = new StringBuilder("\ncold schema-only (LIMIT 0), local provider, one query per fresh dataset\n");
        table.append(String.format(Locale.ROOT, "%-36s", "cell"));
        for (int fileCount : FILE_COUNTS) {
            table.append(String.format(Locale.ROOT, "%12d files", fileCount));
        }
        table.append(String.format(Locale.ROOT, "%12s", "us/file"));
        table.append('\n');
        for (Map.Entry<String, long[]> e : millisByCell.entrySet()) {
            long[] millis = e.getValue();
            table.append(String.format(Locale.ROOT, "%-36s", e.getKey()));
            for (long value : millis) {
                table.append(String.format(Locale.ROOT, "%18d", value));
            }
            double usPerFile = (millis[millis.length - 1] - millis[0]) * 1000.0 / (FILE_COUNTS[FILE_COUNTS.length - 1] - FILE_COUNTS[0]);
            table.append(String.format(Locale.ROOT, "%12.1f", usPerFile));
            table.append('\n');
        }
        logger.info(table.toString());
    }

    private static void writeCsv(Path target) throws IOException {
        Files.writeString(target, "id,v\n1,a\n");
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.cache.ReadConfigFingerprint;
import org.elasticsearch.xpack.esql.datasources.cache.SchemaCacheEntry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.empty;

/**
 * The census of {@link ReadConfigFingerprint#of} derivation sites, and the agreement each site owes another.
 * <p>
 * The read-configuration fingerprint is ONE identity computed at SEVERAL sites from schema objects that differ by
 * provenance: what the harvest hashes must be what the entry's stamp hashed and what the serve gate expects, or the
 * comparison matches nothing and the warm path dies in silence — going-cold is invisible to every value assertion,
 * because cold recomputes the right answer. Two shipped defects had exactly this shape: the multi-file fold dropped
 * the stamp, and the serve gate hashed a partition-enriched coordinator schema no reader ever parses.
 * <p>
 * No mechanism can PROVE a new derivation site agrees with its partner — agreement depends on which schema object
 * flows in at runtime, which is exactly what {@code ReadConfigSymmetryTests} could not see when it derived both
 * sides from one variable. What CAN be enforced is that a new site cannot land silently: this census fails the
 * build on any new {@code ReadConfigFingerprint.of} call in main sources until the author declares the site below
 * — its role, and which existing site it must agree with — and the declaration's instructions demand a behavioural
 * fixture in the suite that owns the pairing. The declarations are the map; the fixtures are the proof.
 */
public class ReadConfigFingerprintDerivationSitesTests extends ESTestCase {

    private enum Role {
        /** Writes the fingerprint into a cache entry or harvested statistics — the producer side. */
        STAMP,
        /** Computes the fingerprint the data-node harvest attaches to its contributions. */
        HARVEST,
        /** Computes the fingerprint the serve gate compares an entry's stamp against — the consumer side. */
        SERVE_EXPECTATION
    }

    /**
     * One declaration per derivation site. {@code mustAgreeWith} names the partner derivation and the suite that
     * proves the agreement; a site with no live pairing fixture is a site whose agreement is asserted by nobody.
     */
    private record Site(String file, String where, Role role, String mustAgreeWith) {}

    private static final String RESOLVER = "esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java";
    private static final String FILE_SOURCE_FACTORY = "esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSourceFactory.java";

    private static final List<Site> SITES = List.of(
        new Site(
            FILE_SOURCE_FACTORY,
            "readConfigFingerprinter lambda handed to AsyncExternalSourceOperatorFactory; applied per file to "
                + "fileSplit.readSchema() / perFileReadSchema / unifiedReadSchema",
            Role.HARVEST,
            "every STAMP and SERVE_EXPECTATION below; wire half pinned by ReadConfigSymmetryTests, layout half by "
                + "AbstractExternalReadConfigParityIT (hive fixture)"
        ),
        new Site(
            RESOLVER,
            "stampInferredReadConfig — of(entry.toAttributes(), NONE) on the inferred text rail",
            Role.STAMP,
            "the HARVEST over the schema this entry was minted from; entry round-trip half pinned by "
                + "testInferredEntryStampRoundTripsToTheHarvestFingerprint below"
        ),
        new Site(
            RESOLVER,
            "declared-mapping cache seed — of(logicalSchema, declaredReadSpecOf(declaredMapping))",
            Role.STAMP,
            "the HARVEST under the same declaration; end-to-end via AbstractExternalReadConfigParityIT strict-rail pins"
        ),
        new Site(
            RESOLVER,
            "declared serve gate, per-file loop — of(perFile.fileSchema(), declaredReadSpec); same value as the "
                + "harvest BY CONSTRUCTION (folded into the loop that builds the per-file schema)",
            Role.SERVE_EXPECTATION,
            "HARVEST + STAMP; partitioned/shadowed layouts pinned by CsvExternalReadConfigParityIT hive pins"
        ),
        new Site(
            RESOLVER,
            "declared serve gate, defensive fallback — of(dataOnlyUnifiedOverlaid, declaredReadSpec) when the "
                + "schemaMap is empty (documented unreachable for stamped entries)",
            Role.SERVE_EXPECTATION,
            "same pairing as the per-file loop; kept only as a fallback"
        )
    );

    private static final Pattern DERIVATION = Pattern.compile("ReadConfigFingerprint\\s*\\.\\s*of\\s*\\(|ReadConfigFingerprint::of");
    private static final Pattern STATIC_IMPORT = Pattern.compile("import\\s+static\\s+[\\w.]*ReadConfigFingerprint");
    private static final String DEFINITION_FILE = "cache/ReadConfigFingerprint.java";

    /**
     * Scans the main sources of every {@code esql*} module for derivation sites and compares against the
     * declarations. A NEW call site — a new file, or another call in a declared file — fails until a {@link Site}
     * is added naming its role and its agreement partner, and the pairing fixture the declaration demands exists.
     */
    public void testEveryDerivationSiteIsDeclared() throws IOException {
        Path pluginRoot = findPluginRoot();
        Map<String, Integer> found = new TreeMap<>();
        List<String> staticImports = new java.util.ArrayList<>();
        try (Stream<Path> walk = Files.walk(pluginRoot)) {
            for (Path p : (Iterable<Path>) walk::iterator) {
                String rel = pluginRoot.relativize(p).toString().replace('\\', '/');
                if (rel.endsWith(".java") == false || rel.startsWith("esql") == false || rel.contains("/src/main/java/") == false) {
                    continue;
                }
                if (rel.endsWith(DEFINITION_FILE)) {
                    continue;
                }
                String content = Files.readString(p);
                if (STATIC_IMPORT.matcher(content).find()) {
                    staticImports.add(rel);
                }
                int count = 0;
                Matcher m = DERIVATION.matcher(content);
                while (m.find()) {
                    count++;
                }
                if (count > 0) {
                    found.put(rel, count);
                }
            }
        }
        assertThat(
            "static import of ReadConfigFingerprint hides derivation sites from this census — import the class instead",
            staticImports,
            empty()
        );
        Map<String, Integer> declared = new TreeMap<>();
        for (Site site : SITES) {
            declared.merge(site.file(), 1, Integer::sum);
        }
        assertEquals(
            "ReadConfigFingerprint.of derivation sites changed. A fingerprint derived from a schema another site does "
                + "not hash matches nothing and takes the warm path down in silence. For every NEW site add a Site "
                + "declaration (role + which existing site it must agree with) AND a pairing fixture in the suite the "
                + "declaration names; for a REMOVED site delete its declaration. Declared per file: "
                + declared
                + ", found per file: "
                + found,
            declared,
            found
        );
    }

    /**
     * The pairing fixture this class owns: the inferred-rail STAMP derives from {@code entry.toAttributes()}, the
     * HARVEST derives from the schema the coordinator minted the entry FROM — two derivations of one identity that
     * meet only at the serve gate. The entry round trip (attributes → column arrays → fresh attributes) must
     * preserve the fingerprint, or every inferred text entry goes permanently cold. The wire round trip is pinned
     * separately by {@code ReadConfigSymmetryTests}; this is the seam that test derived away.
     */
    public void testInferredEntryStampRoundTripsToTheHarvestFingerprint() {
        List<Attribute> minted = List.of(
            attr("user", DataType.KEYWORD),
            attr("count", DataType.LONG),
            attr("ts", DataType.DATETIME),
            attr("ratio", DataType.DOUBLE)
        );
        SchemaCacheEntry entry = SchemaCacheEntry.from(minted, "csv", "s3://bucket/data.csv", Map.of(), Map.of());

        String harvestSide = ReadConfigFingerprint.of(minted, DeclaredReadSpec.NONE);
        String stampSide = ReadConfigFingerprint.of(entry.toAttributes(), DeclaredReadSpec.NONE);

        assertNotEquals(ReadConfigFingerprint.UNKNOWN, harvestSide);
        assertEquals(
            "the inferred-rail stamp (entry.toAttributes()) and the harvest (the minted schema) must derive the same "
                + "fingerprint — a disagreement serves nothing and re-scans forever",
            harvestSide,
            stampSide
        );
    }

    private static ReferenceAttribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type);
    }

    /**
     * Locates {@code x-pack/plugin} from the test working directory (gradle sets it to
     * {@code <module>/build/testrun/<task>}; IDEs use the module or repo root). Walks up, accepting the level where
     * the esql module's main sources are visible.
     */
    private static Path findPluginRoot() {
        Path cur = PathUtils.get("").toAbsolutePath();
        String probe = "esql/src/main/java/org/elasticsearch/xpack/esql/datasources/cache/ReadConfigFingerprint.java";
        for (int i = 0; i < 12 && cur != null; i++, cur = cur.getParent()) {
            if (Files.exists(cur.resolve(probe))) {
                return cur;
            }
            if (Files.exists(cur.resolve("x-pack/plugin").resolve(probe))) {
                return cur.resolve("x-pack/plugin");
            }
        }
        throw new AssertionError(
            "cannot locate x-pack/plugin from "
                + PathUtils.get("").toAbsolutePath()
                + " — the derivation-site census needs the main sources"
        );
    }
}

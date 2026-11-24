/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Oracle for nested fields on the columnar batch path (Phase 0 of elastic/elasticsearch#167077).
 * <p>
 * Today {@code ShardBatchMapper#resolveMappers} sends every batch to the sequential path when the mapping has a {@code nested}
 * mapper, so the batch path cannot yet be compared with x-content. This class therefore has three parts:
 * <ul>
 *   <li><b>Parity scenarios.</b> They go through {@link #assertNestedParity}, which today checks that the sequential path indexes
 *       the expected number of children for each nested path and that the batch path refuses the documents. Phase 1 step 3 changes
 *       its body to {@link #assertColumnarMatchesXContent}, which compares the block of every document with x-content.</li>
 *   <li><b>Fallback pins.</b> They check that a shape the batch path must not take is refused. They pass today because the whole
 *       mapping falls back, and they must keep passing when the refusal moves into the nested shredder.</li>
 *   <li><b>Row-path probes.</b> They record what the sequential path does with shapes that the plan does not pin down yet, so that
 *       Phase 1 can accept or refuse each one on facts.</li>
 * </ul>
 * Runtime fields have no fallback pin: every strict-columnar index mode rejects them at mapping time, so a runtime field that
 * shadows a child path cannot exist in these modes.
 */
public class NestedColumnarMapperCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final List<IndexMode> STRICT_COLUMNAR_MODES = List.of(
        IndexMode.COLUMNAR,
        IndexMode.LOGSDB_COLUMNAR,
        IndexMode.VECTORDB_COLUMNAR
    );

    /** What the sequential path does with the sources of a fallback pin. */
    private enum RowPath {
        /** It indexes them, which proves that the sources are legal and the refusal is not vacuous. */
        ACCEPTS,
        /** It rejects them with a document error that the batch path cannot report, so the batch path must refuse. */
        REJECTS,
        /** It trips an assertion, see {@link #testProbeDottedKeysIntoNestedTripAnAssertion}, so there is nothing to assert about it. */
        ASSERTS
    }

    /** The child documents that one input document must index: the number of children for each nested path. */
    private record ExpectedChildren(Map<String, Integer> countByPath) {}

    /** An input document and the children that the sequential path indexes for it. */
    private record NestedDoc(Doc doc, ExpectedChildren children) {}

    private record NestedScenario(String name, List<NestedDoc> docs) {

        Batch batch() {
            return new Batch(name, 1L, docs.stream().map(NestedDoc::doc).toList());
        }
    }

    /**
     * An input document with no routing, version {@code 1} and sequence number {@code seqNo}. {@code pathsAndCounts} alternates a
     * nested path and the number of children that path must have, for example {@code "n", 2, "other", 1}; omit it for no children.
     */
    private static NestedDoc nd(String id, long seqNo, String source, Object... pathsAndCounts) {
        final Map<String, Integer> counts = new TreeMap<>();
        for (int i = 0; i < pathsAndCounts.length; i += 2) {
            counts.put((String) pathsAndCounts[i], (Integer) pathsAndCounts[i + 1]);
        }
        return new NestedDoc(doc(id, seqNo, source), new ExpectedChildren(counts));
    }

    private static NestedScenario scenario(String name, NestedDoc... docs) {
        return new NestedScenario(name, List.of(docs));
    }

    private static Settings settings(IndexMode mode) {
        return Settings.builder().put(IndexSettings.MODE.getKey(), mode.getName()).build();
    }

    private static boolean needsTimestamp(IndexMode mode) {
        return mode == IndexMode.LOGSDB_COLUMNAR;
    }

    /** A mapping of {@code fields} that also maps {@code @timestamp} in the modes that need it. */
    private static XContentBuilder mappingFor(IndexMode mode, CheckedConsumer<XContentBuilder, IOException> fields) throws IOException {
        return mapping(b -> {
            if (needsTimestamp(mode)) {
                b.startObject("@timestamp").field("type", "date").endObject();
            }
            fields.accept(b);
        });
    }

    /** A JSON object of {@code members}, which also holds an {@code @timestamp} in the modes that need it. */
    private static String src(IndexMode mode, String members) {
        final List<String> all = new ArrayList<>();
        if (needsTimestamp(mode)) {
            all.add("\"@timestamp\":\"2025-01-01T00:00:00Z\"");
        }
        if (members.isEmpty() == false) {
            all.add(members);
        }
        return "{" + String.join(",", all) + "}";
    }

    /** The standard nested mapping of these tests: a root {@code host} and {@code n} with a keyword {@code k} and a long {@code v}. */
    private static CheckedConsumer<XContentBuilder, IOException> hostAndN() {
        return b -> {
            b.startObject("host").field("type", "keyword").endObject();
            b.startObject("n").field("type", "nested").startObject("properties");
            b.startObject("k").field("type", "keyword").endObject();
            b.startObject("v").field("type", "long").endObject();
            b.endObject().endObject();
        };
    }

    /**
     * Parity check of {@code scenarios} against x-content, in every strict-columnar mode and, where the mode supports it, with
     * {@code columnar_stored} source. {@code sources} is a function of the mode because only some modes need an {@code @timestamp}.
     * <p>
     * Until the batch path maps nested values this is a guard rather than the real comparison; see the class comment.
     */
    private void assertNestedParity(
        CheckedConsumer<XContentBuilder, IOException> fields,
        Map<String, String> extraSettings,
        Function<IndexMode, List<NestedScenario>> scenarios
    ) throws IOException {
        for (IndexMode mode : STRICT_COLUMNAR_MODES) {
            final Settings.Builder base = Settings.builder().put(settings(mode));
            extraSettings.forEach(base::put);
            final List<Settings> variants = new ArrayList<>();
            variants.add(base.build());
            if (mode.supportedSourceModes().contains(SourceFieldMapper.Mode.COLUMNAR_STORED)) {
                variants.add(
                    Settings.builder()
                        .put(base.build())
                        .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.toString())
                        .build()
                );
            }
            for (Settings variant : variants) {
                final MapperService mapperService = createMapperService(variant, mappingFor(mode, fields));
                for (NestedScenario scenario : scenarios.apply(mode)) {
                    for (NestedDoc nestedDoc : scenario.docs()) {
                        assertEquals(
                            "mode [" + mode + "] scenario [" + scenario.name() + "] doc [" + nestedDoc.doc().id() + "]: children per path",
                            nestedDoc.children().countByPath(),
                            childCounts(mapperService, nestedDoc.doc())
                        );
                    }
                    for (SourceEncoder encoder : SourceEncoder.available()) {
                        assertBatchPathRefuses(
                            mapperService,
                            encoder,
                            scenario.docs().stream().map(d -> d.doc().source()).toArray(String[]::new)
                        );
                    }
                }
            }
        }
    }

    /** The number of children that the sequential path indexes for {@code doc}, by the value of their {@code _nested_path}. */
    private static Map<String, Integer> childCounts(MapperService mapperService, Doc doc) throws IOException {
        final ParsedDocument parsed = mapperService.documentMapper()
            .parse(
                new SourceToParse(doc.id(), new BytesArray(doc.source().getBytes(StandardCharsets.UTF_8)), XContentType.JSON, doc.routing())
            );
        final String nestedPathField = NestedPathFieldMapper.name(mapperService.getIndexSettings().getIndexVersionCreated());
        final Map<String, Integer> counts = new TreeMap<>();
        final List<LuceneDocument> documents = parsed.docs();
        for (LuceneDocument child : documents.subList(0, documents.size() - 1)) {
            counts.merge(child.getField(nestedPathField).stringValue(), 1, Integer::sum);
        }
        return counts;
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // Parity scenarios
    // ---------------------------------------------------------------------------------------------------------------------------

    /** The array spelling and the object spelling of one nested element, in separate batches and mixed in one batch. */
    public void testArrayAndObjectSpellings() throws IOException {
        assertNestedParity(
            hostAndN(),
            Map.of(),
            mode -> List.of(
                scenario(
                    "array spelling",
                    nd("a1", 1L, src(mode, "\"host\":\"h1\",\"n\":[{\"k\":\"x\",\"v\":1},{\"k\":\"y\",\"v\":2}]"), "n", 2),
                    nd("a2", 2L, src(mode, "\"host\":\"h2\",\"n\":[{\"k\":\"z\",\"v\":3}]"), "n", 1)
                ),
                scenario("object spelling", nd("o1", 1L, src(mode, "\"host\":\"h1\",\"n\":{\"k\":\"x\",\"v\":1}"), "n", 1)),
                scenario(
                    "both spellings in one batch",
                    nd("m1", 1L, src(mode, "\"host\":\"h1\",\"n\":[{\"k\":\"x\",\"v\":1},{\"k\":\"y\",\"v\":2}]"), "n", 2),
                    nd("m2", 2L, src(mode, "\"host\":\"h2\",\"n\":{\"k\":\"z\",\"v\":3}"), "n", 1),
                    nd("m3", 3L, src(mode, "\"host\":\"h3\"")),
                    nd("m4", 4L, src(mode, "\"host\":\"h4\",\"n\":[{\"k\":\"w\"},{\"v\":4}]"), "n", 2)
                )
            )
        );
    }

    /** Empty values of the nested field: {@code []}, {@code null}, {@code {}} and {@code [{}]}. */
    public void testEmptyValues() throws IOException {
        assertNestedParity(
            hostAndN(),
            Map.of(),
            mode -> List.of(
                scenario(
                    "empty values",
                    nd("e1", 1L, src(mode, "\"host\":\"h1\",\"n\":[]")),
                    nd("e2", 2L, src(mode, "\"host\":\"h2\",\"n\":null")),
                    nd("e3", 3L, src(mode, "\"host\":\"h3\",\"n\":{}"), "n", 1),
                    nd("e4", 4L, src(mode, "\"host\":\"h4\",\"n\":[{}]"), "n", 1),
                    nd("e5", 5L, src(mode, "\"host\":\"h5\",\"n\":[{},{\"k\":\"x\"}]"), "n", 2)
                )
            )
        );
    }

    /** Scalar arrays inside the elements: a multi-valued keyword and a multi-valued long. */
    public void testScalarArraysInElements() throws IOException {
        assertNestedParity(b -> {
            b.startObject("n").field("type", "nested").startObject("properties");
            b.startObject("tags").field("type", "keyword").endObject();
            b.startObject("nums").field("type", "long").endObject();
            b.endObject().endObject();
        },
            Map.of(),
            mode -> List.of(
                scenario(
                    "scalar arrays",
                    nd("s1", 1L, src(mode, "\"n\":[{\"tags\":[\"a\",\"b\"],\"nums\":[1,2,3]},{\"tags\":\"c\",\"nums\":4}]"), "n", 2),
                    nd("s2", 2L, src(mode, "\"n\":[{\"tags\":[],\"nums\":[]}]"), "n", 1),
                    nd("s3", 3L, src(mode, "\"n\":[{\"tags\":[\"d\"]},{\"nums\":[5]}]"), "n", 2)
                )
            )
        );
    }

    /** Child types: keyword, long, double, date, ip and a keyword with a multi-field. */
    public void testChildTypes() throws IOException {
        assertNestedParity(b -> {
            b.startObject("n").field("type", "nested").startObject("properties");
            b.startObject("kw").field("type", "keyword").endObject();
            b.startObject("lg").field("type", "long").endObject();
            b.startObject("db").field("type", "double").endObject();
            b.startObject("dt").field("type", "date").endObject();
            b.startObject("ip").field("type", "ip").endObject();
            b.startObject("mf").field("type", "keyword");
            b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            b.endObject();
            b.endObject().endObject();
        },
            Map.of(),
            mode -> List.of(
                scenario(
                    "child types",
                    nd(
                        "t1",
                        1L,
                        src(
                            mode,
                            "\"n\":[{\"kw\":\"a\",\"lg\":1,\"db\":1.5,\"dt\":\"2025-03-01T00:00:00Z\",\"ip\":\"10.0.0.1\",\"mf\":\"m\"},"
                                + "{\"kw\":\"b\",\"lg\":2,\"db\":2.5,\"dt\":\"2025-03-02T00:00:00Z\",\"ip\":\"::1\",\"mf\":\"n\"}]"
                        ),
                        "n",
                        2
                    ),
                    nd(
                        "t2",
                        2L,
                        src(mode, "\"n\":[{\"kw\":\"c\"},{\"lg\":3},{\"db\":4.5},{\"dt\":1700000000000},{\"ip\":\"192.168.1.1\"}]"),
                        "n",
                        5
                    )
                )
            )
        );
    }

    /**
     * Provided {@code _id}s on the children, with the columnar id mode on and off, because the children carry the identity term
     * and, in the columnar id mode, a doc-values copy of the id.
     */
    public void testChildIds() throws IOException {
        for (boolean columnarId : new boolean[] { true, false }) {
            assertNestedParity(
                hostAndN(),
                Map.of(IndexSettings.USE_COLUMNAR_ID_BY_DEFAULT.getKey(), Boolean.toString(columnarId)),
                mode -> List.of(
                    scenario(
                        "child ids, columnar id mode [" + columnarId + "]",
                        nd("id-1", 1L, src(mode, "\"host\":\"h1\",\"n\":[{\"k\":\"x\"},{\"k\":\"y\"}]"), "n", 2),
                        nd("id-2", 2L, src(mode, "\"host\":\"h2\",\"n\":[{\"k\":\"z\"}]"), "n", 1),
                        nd("id-3", 3L, src(mode, "\"host\":\"h3\""))
                    )
                )
            );
        }
    }

    /** A child keyword value above {@code ignore_above} is not indexed, and the root document records it in {@code _ignored}. */
    public void testIgnoreAboveOnChild() throws IOException {
        assertNestedParity(b -> {
            b.startObject("n").field("type", "nested").startObject("properties");
            b.startObject("kw").field("type", "keyword").field("ignore_above", 3).endObject();
            b.endObject().endObject();
        },
            Map.of(),
            mode -> List.of(
                scenario(
                    "ignore_above",
                    nd("i1", 1L, src(mode, "\"n\":[{\"kw\":\"ok\"},{\"kw\":\"much too long\"}]"), "n", 2),
                    nd("i2", 2L, src(mode, "\"n\":[{\"kw\":\"also too long\"}]"), "n", 1)
                )
            )
        );
    }

    /** A child field with {@code nullability: false} on elements that all have a value. */
    public void testNullabilityFalseOnChild() throws IOException {
        assertNestedParity(b -> {
            b.startObject("n").field("type", "nested").startObject("properties");
            b.startObject("k").field("type", "keyword").field("nullability", false).endObject();
            b.endObject().endObject();
        },
            Map.of(),
            mode -> List.of(
                scenario(
                    "nullability false",
                    nd("r1", 1L, src(mode, "\"n\":[{\"k\":\"x\"},{\"k\":\"y\"}]"), "n", 2),
                    nd("r2", 2L, src(mode, "\"n\":{\"k\":\"z\"}"), "n", 1)
                )
            )
        );
    }

    /** Two nested paths in one mapping, with values for both, for one only, and for neither. */
    public void testTwoNestedPaths() throws IOException {
        assertNestedParity(b -> {
            b.startObject("host").field("type", "keyword").endObject();
            b.startObject("n").field("type", "nested").startObject("properties");
            b.startObject("k").field("type", "keyword").endObject();
            b.endObject().endObject();
            b.startObject("m").field("type", "nested").startObject("properties");
            b.startObject("v").field("type", "long").endObject();
            b.endObject().endObject();
        },
            Map.of(),
            mode -> List.of(
                scenario(
                    "two nested paths",
                    nd("p1", 1L, src(mode, "\"host\":\"h1\",\"n\":[{\"k\":\"x\"},{\"k\":\"y\"}],\"m\":[{\"v\":1}]"), "n", 2, "m", 1),
                    nd("p2", 2L, src(mode, "\"host\":\"h2\",\"m\":[{\"v\":2},{\"v\":3},{\"v\":4}]"), "m", 3),
                    nd("p3", 3L, src(mode, "\"host\":\"h3\",\"n\":{\"k\":\"z\"}"), "n", 1),
                    nd("p4", 4L, src(mode, "\"host\":\"h4\""))
                )
            )
        );
    }

    /** A nested field under a plain object: the nested mapper is a direct root child with the full path {@code meta.comments}. */
    public void testNestedUnderPlainObject() throws IOException {
        assertNestedParity(b -> {
            b.startObject("meta").startObject("properties");
            b.startObject("owner").field("type", "keyword").endObject();
            b.startObject("comments").field("type", "nested").startObject("properties");
            b.startObject("text").field("type", "keyword").endObject();
            b.endObject().endObject();
            b.endObject().endObject();
        },
            Map.of(),
            mode -> List.of(
                scenario(
                    "meta.comments",
                    nd(
                        "c1",
                        1L,
                        src(mode, "\"meta\":{\"owner\":\"o1\",\"comments\":[{\"text\":\"a\"},{\"text\":\"b\"}]}"),
                        "meta.comments",
                        2
                    ),
                    nd("c2", 2L, src(mode, "\"meta\":{\"owner\":\"o2\",\"comments\":{\"text\":\"c\"}}"), "meta.comments", 1),
                    nd("c3", 3L, src(mode, "\"meta\":{\"owner\":\"o3\"}"))
                )
            )
        );
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // Fallback pins
    // ---------------------------------------------------------------------------------------------------------------------------

    /**
     * Asserts that the batch path refuses {@code sources} for a mapping of {@code fields} in every strict-columnar mode, with both
     * source modes where the mode supports them. {@code rowPath} says what the sequential path does with the sources: when it accepts
     * them, the pin also proves that the sources are legal, so that the refusal is not vacuous; when it rejects them, the sequential path
     * must report the document error that the batch path cannot.
     */
    private void assertRefusedInAllModes(
        CheckedConsumer<XContentBuilder, IOException> fields,
        Map<String, String> extraSettings,
        RowPath rowPath,
        Function<IndexMode, String[]> sources
    ) throws IOException {
        for (IndexMode mode : STRICT_COLUMNAR_MODES) {
            final Settings.Builder base = Settings.builder().put(settings(mode));
            extraSettings.forEach(base::put);
            final List<Settings> variants = new ArrayList<>(List.of(base.build()));
            if (mode.supportedSourceModes().contains(SourceFieldMapper.Mode.COLUMNAR_STORED)) {
                variants.add(
                    Settings.builder()
                        .put(base.build())
                        .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.COLUMNAR_STORED.toString())
                        .build()
                );
            }
            for (Settings variant : variants) {
                final MapperService mapperService = createMapperService(variant, mappingFor(mode, fields));
                final String[] documents = sources.apply(mode);
                for (int i = 0; i < documents.length; i++) {
                    final Doc doc = doc("p" + i, i, documents[i]);
                    switch (rowPath) {
                        case ACCEPTS -> childCounts(mapperService, doc);
                        case REJECTS -> expectThrows(DocumentParsingException.class, () -> childCounts(mapperService, doc));
                        case ASSERTS -> {
                        }
                    }
                }
                for (SourceEncoder encoder : SourceEncoder.available()) {
                    assertBatchPathRefuses(mapperService, encoder, documents);
                }
            }
        }
    }

    /**
     * {@code include_in_parent} and {@code include_in_root} copy the children's fields into another document. Strict columnar will
     * reject them at mapping time once elastic/elasticsearch#166393 merges; until then, and in older indices, the batch path refuses.
     */
    public void testIncludeInParentAndRootFallBack() throws IOException {
        for (String include : List.of("include_in_parent", "include_in_root")) {
            assertRefusedInAllModes(b -> {
                b.startObject("n").field("type", "nested").field(include, true).startObject("properties");
                b.startObject("k").field("type", "keyword").endObject();
                b.endObject().endObject();
            }, Map.of(), RowPath.ACCEPTS, mode -> new String[] { src(mode, "\"n\":[{\"k\":\"x\"},{\"k\":\"y\"}]") });
        }
    }

    /** Nested fields in an index that is not strict columnar, here {@code time_series}, which can have {@code index.batch_indexing}. */
    public void testNestedInNonColumnarModeFallsBack() throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            .put(IndexSettings.BATCH_INDEXING.getKey(), true)
            .build();
        final MapperService mapperService = createMapperService(settings, mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("n").field("type", "nested").startObject("properties");
            b.startObject("k").field("type", "keyword").endObject();
            b.endObject().endObject();
        }));
        for (SourceEncoder encoder : SourceEncoder.available()) {
            assertBatchPathRefuses(mapperService, encoder, "{\"@timestamp\":\"2025-01-01T00:00:00Z\",\"dim\":\"d\",\"n\":[{\"k\":\"x\"}]}");
        }
    }

    /**
     * An object value inside a nested element makes an extra level of child documents under {@code subobjects: false}, which is how
     * the columnar modes map objects (see {@code SyntheticVersusColumnarStoredSourceIT#testNestedWithObjectArraySubfield}).
     */
    public void testObjectArrayInsideElementFallsBack() throws IOException {
        assertRefusedInAllModes(b -> {
            b.startObject("n").field("type", "nested").startObject("properties");
            b.startObject("obj").startObject("properties").startObject("val").field("type", "long").endObject().endObject().endObject();
            b.endObject().endObject();
        }, Map.of(), RowPath.ACCEPTS, mode -> new String[] { src(mode, "\"n\":[{\"obj\":[{\"val\":1},{\"val\":2}]}]") });
    }

    /** An element that is not an object. */
    public void testNonObjectElementFallsBack() throws IOException {
        assertRefusedInAllModes(
            hostAndN(),
            Map.of(),
            RowPath.REJECTS,
            mode -> new String[] { src(mode, "\"n\":[\"a\"]"), src(mode, "\"n\":[1]"), src(mode, "\"n\":\"a\"") }
        );
    }

    /** An object inside an element arrives as a {@code KEY_VALUE} blob inside the element. */
    public void testObjectInsideElementFallsBack() throws IOException {
        assertRefusedInAllModes(b -> {
            b.startObject("n").field("type", "nested").startObject("properties");
            b.startObject("o").startObject("properties").startObject("x").field("type", "long").endObject().endObject().endObject();
            b.endObject().endObject();
        }, Map.of(), RowPath.ACCEPTS, mode -> new String[] { src(mode, "\"n\":[{\"o\":{\"x\":1}}]") });
    }

    /** An array inside an element that holds an object. */
    public void testArrayOfObjectsInsideElementFallsBack() throws IOException {
        assertRefusedInAllModes(b -> {
            b.startObject("n").field("type", "nested").startObject("properties");
            b.startObject("a").startObject("properties").startObject("x").field("type", "long").endObject().endObject().endObject();
            b.endObject().endObject();
        }, Map.of(), RowPath.ACCEPTS, mode -> new String[] { src(mode, "\"n\":[{\"a\":[{\"x\":1},{\"x\":2}]}]") });
    }

    /** More children in one row than {@code index.mapping.nested_objects.limit} allows. */
    public void testTooManyChildrenFallBack() throws IOException {
        assertRefusedInAllModes(
            hostAndN(),
            Map.of("index.mapping.nested_objects.limit", "2"),
            RowPath.REJECTS,
            mode -> new String[] { src(mode, "\"n\":[{\"k\":\"a\"},{\"k\":\"b\"},{\"k\":\"c\"}]") }
        );
    }

    /** A dotted key that crosses the nested boundary; see {@link #testProbeDottedKeysIntoNestedTripAnAssertion} for the sequential path. */
    public void testDottedKeyAcrossBoundaryFallsBack() throws IOException {
        assertRefusedInAllModes(hostAndN(), Map.of(), RowPath.ASSERTS, mode -> new String[] { src(mode, "\"n.k\":\"a\"") });
    }

    /** The object spelling with a deeper object: the leaf {@code n.o.x} sits under the non-leaf {@code o}, not directly under {@code n}. */
    public void testDeeperObjectUnderObjectSpellingFallsBack() throws IOException {
        assertRefusedInAllModes(b -> {
            b.startObject("n").field("type", "nested").startObject("properties");
            b.startObject("o").startObject("properties").startObject("x").field("type", "long").endObject().endObject().endObject();
            b.endObject().endObject();
        }, Map.of(), RowPath.ACCEPTS, mode -> new String[] { src(mode, "\"n\":{\"o\":{\"x\":1}}") });
    }

    /** A child leaf that the mapping does not know, under {@code dynamic: false} and under {@code dynamic: strict}. */
    public void testUnmappedChildLeafFallsBack() throws IOException {
        for (String dynamic : List.of("false", "strict")) {
            assertRefusedInAllModes(b -> {
                b.startObject("n").field("type", "nested").field("dynamic", dynamic).startObject("properties");
                b.startObject("k").field("type", "keyword").endObject();
                b.endObject().endObject();
            },
                Map.of(),
                dynamic.equals("false") ? RowPath.ACCEPTS : RowPath.REJECTS,
                mode -> new String[] { src(mode, "\"n\":[{\"k\":\"a\",\"unknown\":\"b\"}]") }
            );
        }
    }

    /** A floating-point number inside an element does not keep its lexical form in the blob, so it cannot go to a keyword child. */
    public void testFloatingPointValueOnKeywordChildFallsBack() throws IOException {
        assertRefusedInAllModes(
            hostAndN(),
            Map.of(),
            RowPath.ACCEPTS,
            mode -> new String[] { src(mode, "\"n\":[{\"k\":1.5}]"), src(mode, "\"n\":[{\"k\":2.50}]") }
        );
    }

    /** A missing or null value for a child with {@code nullability: false} is a document error that only the sequential path reports. */
    public void testNullabilityViolationOnChildFallsBack() throws IOException {
        assertRefusedInAllModes(b -> {
            b.startObject("n").field("type", "nested").startObject("properties");
            b.startObject("k").field("type", "keyword").field("nullability", false).endObject();
            b.endObject().endObject();
        }, Map.of(), RowPath.REJECTS, mode -> new String[] { src(mode, "\"n\":[{\"k\":\"x\"},{}]") });
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // Row-path probes
    // ---------------------------------------------------------------------------------------------------------------------------

    /**
     * A dotted key that crosses the nested boundary, as in {@code {"n.k":"a"}} or {@code {"meta":{"comments.text":"a"}}}, makes the
     * sequential path of the columnar modes trip an assertion in {@code DocumentParser#getLeafMapper}: the root finds no mapper for
     * the dotted name, then finds the child's field type and takes it for a runtime field, which the assertion rejects. Reading the
     * code, a production node, which runs without assertions, takes the no-op mapper for a runtime field and drops the value, so it
     * is not indexed but stays in {@code _source}.
     * <p>
     * Phase 1 must therefore refuse these shapes, which keeps today's behavior exactly, and not map them as the object spelling.
     * This probe fails when the sequential path starts to handle them; revisit the rule "a dotted key across the boundary falls back"
     * then.
     */
    public void testProbeDottedKeysIntoNestedTripAnAssertion() throws IOException {
        for (IndexMode mode : STRICT_COLUMNAR_MODES) {
            final MapperService rootNested = createMapperService(settings(mode), mappingFor(mode, hostAndN()));
            expectThrows(AssertionError.class, () -> childCounts(rootNested, doc("d1", 1L, src(mode, "\"n.k\":\"a\""))));
            final MapperService underObject = createMapperService(settings(mode), mappingFor(mode, b -> {
                b.startObject("meta").startObject("properties");
                b.startObject("comments").field("type", "nested").startObject("properties");
                b.startObject("text").field("type", "keyword").endObject();
                b.endObject().endObject();
                b.endObject().endObject();
            }));
            final String dottedChild = src(mode, "\"meta\":{\"comments.text\":\"a\"}");
            expectThrows(AssertionError.class, () -> childCounts(underObject, doc("d2", 2L, dottedChild)));
        }
    }

    /**
     * The dotted value spelling {@code {"meta.comments":[...]}} of a nested field under a plain object: unlike a dotted key that
     * crosses the boundary, the sequential path indexes it, and gives the same children as the object spelling.
     */
    public void testProbeDottedValueSpelling() throws IOException {
        for (IndexMode mode : STRICT_COLUMNAR_MODES) {
            final MapperService mapperService = createMapperService(settings(mode), mappingFor(mode, b -> {
                b.startObject("meta").startObject("properties");
                b.startObject("comments").field("type", "nested").startObject("properties");
                b.startObject("text").field("type", "keyword").endObject();
                b.endObject().endObject();
                b.endObject().endObject();
            }));
            assertEquals(
                "mode [" + mode + "]",
                Map.of("meta.comments", 2),
                childCounts(mapperService, doc("d1", 1L, src(mode, "\"meta.comments\":[{\"text\":\"a\"},{\"text\":\"b\"}]")))
            );
        }
    }

    /** Null and scalar elements: the sequential path skips a null element and rejects a scalar one. */
    public void testProbeNullAndScalarElements() throws IOException {
        for (IndexMode mode : STRICT_COLUMNAR_MODES) {
            final MapperService mapperService = createMapperService(settings(mode), mappingFor(mode, hostAndN()));
            assertEquals(
                "mode [" + mode + "]: null element",
                Map.of("n", 1),
                childCounts(mapperService, doc("d1", 1L, src(mode, "\"n\":[null,{\"k\":\"a\"}]")))
            );
            assertEquals(
                "mode [" + mode + "]: only a null element",
                Map.of(),
                childCounts(mapperService, doc("d2", 2L, src(mode, "\"n\":[null]")))
            );
            expectThrows(DocumentParsingException.class, () -> childCounts(mapperService, doc("d3", 3L, src(mode, "\"n\":[\"a\"]"))));
            expectThrows(DocumentParsingException.class, () -> childCounts(mapperService, doc("d4", 4L, src(mode, "\"n\":[1]"))));
        }
    }

    /**
     * Both spellings in one row can only be written as a duplicate key, {@code {"n":[...],"n":{...}}}. The sequential path rejects a
     * duplicate key, so the rule "a row uses both spellings" needs a fallback only if the ESCF encoder accepts such a row.
     */
    public void testProbeBothSpellingsInOneRow() throws IOException {
        for (IndexMode mode : STRICT_COLUMNAR_MODES) {
            final MapperService mapperService = createMapperService(settings(mode), mappingFor(mode, hostAndN()));
            final String source = src(mode, "\"n\":[{\"k\":\"a\"}],\"n\":{\"k\":\"b\"}");
            expectThrows(Exception.class, () -> childCounts(mapperService, doc("d1", 1L, source)));
        }
    }
}

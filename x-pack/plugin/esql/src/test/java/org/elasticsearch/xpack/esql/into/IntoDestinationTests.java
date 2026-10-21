/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Where {@code INTO} may write, resolved from hand-built project metadata: indices, aliases, data streams and templates,
 * each with a mapping that may or may not allow the query's columns.
 */
public class IntoDestinationTests extends ESTestCase {

    private static final IntoOptions KEEP = new IntoOptions(IntoOptions.Collisions.KEEP, false, false, false);
    private static final IntoOptions REPLACE = new IntoOptions(IntoOptions.Collisions.REPLACE, false, false, false);

    public void testMissingDestination() {
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT).build();
        var e = expectThrows(VerificationException.class, () -> IntoDestination.resolve(project, "dst"));
        assertThat(e.getMessage(), containsString("[dst] does not exist and no index template matches it"));
    }

    public void testIndex() {
        ProjectMetadata project = project(index("dst", """
            {"properties": {"host": {"type": "keyword"}, "cpu": {"type": "double"}}}
            """));
        IntoDestination destination = IntoDestination.resolve(project, "dst");
        destination.validate(List.of(column("host", DataType.KEYWORD), column("cpu", DataType.DOUBLE)), REPLACE);
    }

    /** An index with no mapping at all maps its first write dynamically, so anything goes. */
    public void testIndexWithoutMapping() {
        ProjectMetadata project = project(index("dst", null));
        IntoDestination.resolve(project, "dst").validate(List.of(column("anything", DataType.LONG)), REPLACE);
    }

    public void testAlias() {
        ProjectMetadata project = project(index("dst-000001", """
            {"properties": {"host": {"type": "keyword"}}}
            """).putAlias(AliasMetadata.builder("dst").writeIndex(true)), index("dst-000002", """
            {"dynamic": "strict", "properties": {"other": {"type": "keyword"}}}
            """).putAlias(AliasMetadata.builder("dst").writeIndex(false)));
        IntoDestination destination = IntoDestination.resolve(project, "dst");
        // The write index's mapping is the one that matters, and it allows dynamic fields.
        destination.validate(List.of(column("host", DataType.KEYWORD), column("new", DataType.LONG)), REPLACE);
    }

    public void testAliasWithoutWriteIndex() {
        ProjectMetadata project = project(
            index("dst-000001", null).putAlias(AliasMetadata.builder("dst")),
            index("dst-000002", null).putAlias(AliasMetadata.builder("dst"))
        );
        var e = expectThrows(VerificationException.class, () -> IntoDestination.resolve(project, "dst"));
        assertThat(e.getMessage(), containsString("[dst] is an alias without a write index"));
    }

    public void testDataStream() {
        IndexMetadata backing = DataStreamTestHelper.createFirstBackingIndex("dst").putMapping("""
            {"properties": {"@timestamp": {"type": "date"}, "host": {"type": "keyword"}}}
            """).build();
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(backing, false)
            .put(DataStreamTestHelper.newInstance("dst", List.of(backing.getIndex())))
            .build();
        IntoDestination destination = IntoDestination.resolve(project, "dst");
        assertTrue(destination.isDataStream());
        destination.validate(List.of(column("host", DataType.KEYWORD)), KEEP);
        // Nothing a query names can be replaced in a data stream, since no id column reaches one.
        var replace = expectThrows(
            VerificationException.class,
            () -> destination.validate(List.of(column("host", DataType.KEYWORD)), REPLACE)
        );
        assertThat(
            replace.getMessage(),
            containsString(
                "[dst] is a data stream, which only takes new documents, so INTO cannot replace in it; "
                    + "use \"collisions\": \"keep\" or \"collisions\": \"abort\""
            )
        );
        List<Attribute> withId = List.of(column("host", DataType.KEYWORD), column(IntoPageWriter.ID_COLUMN, DataType.KEYWORD));
        // The replace refusal above comes first, so the id column is checked with the modes a data stream accepts.
        var e = expectThrows(VerificationException.class, () -> destination.validate(withId, randomFrom(KEEP, IntoOptions.DEFAULT)));
        assertThat(
            e.getMessage(),
            containsString(
                "[_id] cannot be written to [dst]: a data stream generates its own document ids; drop the column with DROP or KEEP"
            )
        );
        // Writing straight to a backing index is a plain index write.
        IntoDestination backingIndex = IntoDestination.resolve(project, backing.getIndex().getName());
        assertFalse(backingIndex.isDataStream());
        backingIndex.validate(withId, REPLACE);
    }

    /**
     * A time series data stream derives its ids on the shard, so rows can collide there without an id column; like any
     * data stream it cannot be replaced into, while a standalone time series index is not append-only and can be.
     */
    public void testTimeSeriesDataStreamCannotBeReplacedInto() {
        IndexMetadata backing = DataStreamTestHelper.createFirstBackingIndex("tsds")
            .settings(timeSeriesSettings())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexMetadata standalone = IndexMetadata.builder("tsdb")
            .settings(timeSeriesSettings())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(backing, false)
            .put(standalone, false)
            .put(DataStreamTestHelper.newInstance("tsds", List.of(backing.getIndex())))
            .build();
        IntoDestination stream = IntoDestination.resolve(project, "tsds");
        assertTrue(stream.isDataStream());
        stream.validate(List.of(column("host", DataType.KEYWORD)), KEEP);
        var e = expectThrows(VerificationException.class, () -> stream.validate(List.of(column("host", DataType.KEYWORD)), REPLACE));
        assertThat(e.getMessage(), containsString("[tsds] is a data stream, which only takes new documents, so INTO cannot replace in it"));
        IntoDestination index = IntoDestination.resolve(project, "tsdb");
        assertFalse(index.isDataStream());
        index.validate(List.of(column("host", DataType.KEYWORD)), REPLACE);
    }

    private static Settings.Builder timeSeriesSettings() {
        return indexSettings(IndexVersion.current(), 1, 0).put("index.mode", "time_series").put("index.routing_path", "host");
    }

    /** A destination that does not exist yet is fine when a template says what it will look like. */
    public void testTemplate() throws IOException {
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(
                "dst-template",
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of("dst-*"))
                    .template(Template.builder().mappings(new CompressedXContent("""
                        {"_doc": {"dynamic": "strict", "properties": {"host": {"type": "keyword"}}}}
                        """)).build())
                    .build()
            )
            .build();
        IntoDestination destination = IntoDestination.resolve(project, "dst-1");
        destination.validate(List.of(column("host", DataType.KEYWORD)), REPLACE);
        var e = expectThrows(VerificationException.class, () -> destination.validate(List.of(column("cpu", DataType.DOUBLE)), REPLACE));
        assertThat(
            e.getMessage(),
            containsString("[cpu] is not mapped in [dst-1] and its mapping does not allow dynamic fields (dynamic=strict)")
        );
    }

    public void testDataStreamTemplate() throws IOException {
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(
                "dst-template",
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of("dst-*"))
                    .template(Template.builder().mappings(new CompressedXContent("""
                        {"properties": {"host": {"type": "keyword"}}}
                        """)).build())
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                    .build()
            )
            .build();
        IntoDestination destination = IntoDestination.resolve(project, "dst-1");
        assertTrue(destination.isDataStream());
        destination.validate(List.of(column("host", DataType.KEYWORD), column("cpu", DataType.DOUBLE)), KEEP);
        var e = expectThrows(
            VerificationException.class,
            () -> destination.validate(List.of(column("host", DataType.KEYWORD), column("cpu", DataType.DOUBLE)), REPLACE)
        );
        assertThat(
            e.getMessage(),
            containsString("[dst-1] is a data stream, which only takes new documents, so INTO cannot replace in it")
        );
    }

    /** A time series stream that does not exist yet is a data stream by its template, and refuses replace like any other. */
    public void testTimeSeriesDataStreamTemplate() throws IOException {
        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(
                "tsds-template",
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of("tsds-*"))
                    .template(
                        Template.builder()
                            .settings(Settings.builder().put("index.mode", "time_series").put("index.routing_path", "host"))
                            .mappings(new CompressedXContent("""
                                {"properties": {"host": {"type": "keyword", "time_series_dimension": true}}}
                                """))
                            .build()
                    )
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                    .build()
            )
            .build();
        IntoDestination destination = IntoDestination.resolve(project, "tsds-1");
        assertTrue(destination.isDataStream());
        destination.validate(List.of(column("host", DataType.KEYWORD)), KEEP);
        var e = expectThrows(VerificationException.class, () -> destination.validate(List.of(column("host", DataType.KEYWORD)), REPLACE));
        assertThat(
            e.getMessage(),
            containsString("[tsds-1] is a data stream, which only takes new documents, so INTO cannot replace in it")
        );
    }

    public void testUnmappedColumnNeedsDynamicMapping() {
        for (String dynamic : new String[] { "true", "runtime" }) {
            IntoDestination destination = IntoDestination.resolve(project(index("dst", Strings.format("""
                {"dynamic": "%s", "properties": {"host": {"type": "keyword"}}}
                """, dynamic))), "dst");
            destination.validate(List.of(column("host", DataType.KEYWORD), column("cpu", DataType.DOUBLE)), REPLACE);
        }
        for (String dynamic : new String[] { "false", "strict" }) {
            IntoDestination destination = IntoDestination.resolve(project(index("dst", Strings.format("""
                {"dynamic": "%s", "properties": {"host": {"type": "keyword"}}}
                """, dynamic))), "dst");
            destination.validate(List.of(column("host", DataType.KEYWORD)), REPLACE);
            var e = expectThrows(
                VerificationException.class,
                () -> destination.validate(List.of(column("host", DataType.KEYWORD), column("cpu", DataType.DOUBLE)), REPLACE)
            );
            assertThat(
                e.getMessage(),
                containsString("[cpu] is not mapped in [dst] and its mapping does not allow dynamic fields (dynamic=" + dynamic + ")")
            );
        }
    }

    /** The nearest enclosing object decides, so a strict object inside a dynamic root refuses, and the reverse allows. */
    public void testNestedObjectsDecideForTheirFields() {
        IntoDestination strictInside = IntoDestination.resolve(project(index("dst", """
            {"properties": {"meta": {"dynamic": "strict", "properties": {"host": {"type": "keyword"}}}}}
            """)), "dst");
        strictInside.validate(List.of(column("meta.host", DataType.KEYWORD), column("top", DataType.LONG)), REPLACE);
        var e = expectThrows(
            VerificationException.class,
            () -> strictInside.validate(List.of(column("meta.cpu", DataType.DOUBLE)), REPLACE)
        );
        assertThat(e.getMessage(), containsString("[meta.cpu] is not mapped in [dst]"));

        IntoDestination dynamicInside = IntoDestination.resolve(project(index("dst", """
            {"dynamic": "strict", "properties": {"meta": {"dynamic": true, "properties": {"host": {"type": "keyword"}}}}}
            """)), "dst");
        dynamicInside.validate(List.of(column("meta.cpu", DataType.DOUBLE)), REPLACE);
        e = expectThrows(VerificationException.class, () -> dynamicInside.validate(List.of(column("top", DataType.LONG)), REPLACE));
        assertThat(e.getMessage(), containsString("[top] is not mapped in [dst]"));
    }

    public void testIdColumn() {
        IntoDestination destination = IntoDestination.resolve(project(index("dst", """
            {"dynamic": "strict", "properties": {"host": {"type": "keyword"}}}
            """)), "dst");
        // The id is not a field, so it need not be mapped, but it has to be a string.
        destination.validate(List.of(column("_id", DataType.KEYWORD), column("host", DataType.KEYWORD)), REPLACE);
        destination.validate(List.of(column("_id", DataType.TEXT)), REPLACE);
        var e = expectThrows(VerificationException.class, () -> destination.validate(List.of(column("_id", DataType.LONG)), REPLACE));
        assertThat(e.getMessage(), containsString("[_id] is used as the document id, so it must be a keyword or text column, not [long]"));
    }

    public void testOtherMetadataColumnsAreRefused() {
        IntoDestination destination = IntoDestination.resolve(project(index("dst", null)), "dst");
        for (Attribute attribute : List.of(
            new MetadataAttribute(Source.EMPTY, MetadataAttribute.INDEX, DataType.KEYWORD, true),
            new MetadataAttribute(Source.EMPTY, MetadataAttribute.SCORE, DataType.DOUBLE, false),
            column("_version", DataType.LONG)
        )) {
            var e = expectThrows(VerificationException.class, () -> destination.validate(List.of(attribute), REPLACE));
            assertThat(
                e.getMessage(),
                containsString("metadata column [" + attribute.name() + "] cannot be written by INTO; drop it with DROP or KEEP")
            );
        }
    }

    public void testUnsupportedTypeIsRefused() {
        IntoDestination destination = IntoDestination.resolve(project(index("dst", null)), "dst");
        var e = expectThrows(
            VerificationException.class,
            () -> destination.validate(List.of(column("odd", DataType.UNSUPPORTED)), REPLACE)
        );
        assertThat(e.getMessage(), equalTo("[odd] has an unsupported type and cannot be written by INTO"));
    }

    private static ProjectMetadata project(IndexMetadata.Builder... indices) {
        ProjectMetadata.Builder project = ProjectMetadata.builder(ProjectId.DEFAULT);
        for (IndexMetadata.Builder index : indices) {
            project.put(index);
        }
        return project.build();
    }

    private static IndexMetadata.Builder index(String name, String mapping) {
        IndexMetadata.Builder builder = IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), 1, 0));
        if (mapping != null) {
            builder.putMapping(mapping);
        }
        return builder;
    }

    private static Attribute column(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Scenarios for {@code _dimension_group} on the row path: the value of the configured dimension field is copied for keyword
 * values and only for those, the field only exists on time series indices with the grouped sort, and it cannot be set from source.
 */
public class DimensionGroupFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected String fieldName() {
        return DimensionGroupFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        // There aren't any parameters
    }

    @Override
    protected Settings getIndexSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.name())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim.*")
            .put(IndexSettings.TIME_SERIES_DIMENSION_GROUP_FIELD.getKey(), "dim.host")
            .build();
    }

    private DocumentMapper createMapper(XContentBuilder mappings) throws IOException {
        return createMapper(getVersion(), getIndexSettingsBuilder(), mappings);
    }

    private DocumentMapper createMapper(IndexVersion version, Settings.Builder settings, XContentBuilder mappings) throws IOException {
        return createMapperService(version, settings.build(), () -> true, mappings).documentMapper();
    }

    private static XContentBuilder hostAndRegionMapping() throws IOException {
        return mapping(b -> {
            b.startObject("dim").startObject("properties");
            b.startObject("host").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("region").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
        });
    }

    private ParsedDocument parseDocument(DocumentMapper docMapper, CheckedConsumer<XContentBuilder, IOException> f) throws IOException {
        // the routing hash is computed on the coordinating node and travels as the routing of the index request
        return docMapper.parse(source(null, b -> {
            f.accept(b);
            b.field("@timestamp", "2021-10-01");
        }, TimeSeriesRoutingHashFieldMapper.encode(randomInt())));
    }

    private static BytesRef dimensionGroup(ParsedDocument doc) {
        return doc.rootDoc().getBinaryValue(DimensionGroupFieldMapper.NAME);
    }

    public void testCopiesKeywordDimensionValue() throws Exception {
        DocumentMapper docMapper = createMapper(hostAndRegionMapping());
        assertThat(docMapper.metadataMapper(DimensionGroupFieldMapper.class), is(DimensionGroupFieldMapper.INSTANCE));

        ParsedDocument doc = parseDocument(docMapper, b -> b.startObject("dim").field("host", "h1").field("region", "eu").endObject());
        assertThat(dimensionGroup(doc), equalTo(new BytesRef("h1")));

        // the group only depends on the group field
        ParsedDocument sameHost = parseDocument(docMapper, b -> b.startObject("dim").field("host", "h1").field("region", "us").endObject());
        assertThat(dimensionGroup(sameHost), equalTo(new BytesRef("h1")));
        ParsedDocument otherHost = parseDocument(
            docMapper,
            b -> b.startObject("dim").field("host", "h2").field("region", "eu").endObject()
        );
        assertThat(dimensionGroup(otherHost), equalTo(new BytesRef("h2")));
    }

    public void testMissingGroupField() throws Exception {
        DocumentMapper docMapper = createMapper(hostAndRegionMapping());
        ParsedDocument doc = parseDocument(docMapper, b -> b.startObject("dim").field("region", "eu").endObject());
        assertThat(dimensionGroup(doc), nullValue());
    }

    /** Only the first value of an array-valued group field is copied, like the first value drives the tsid value similarity. */
    public void testArrayGroupFieldUsesFirstValue() throws Exception {
        DocumentMapper docMapper = createMapper(hostAndRegionMapping());
        ParsedDocument doc = parseDocument(docMapper, b -> b.startObject("dim").array("host", "h2", "h1").endObject());
        assertThat(dimensionGroup(doc), equalTo(new BytesRef("h2")));
    }

    /** Numeric dimensions are not copied: the group is meant for keyword-like entities such as host names. */
    public void testNumericGroupFieldIsIgnored() throws Exception {
        DocumentMapper docMapper = createMapper(mapping(b -> {
            b.startObject("dim").startObject("properties");
            b.startObject("host").field("type", "long").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
        }));
        ParsedDocument doc = parseDocument(docMapper, b -> b.startObject("dim").field("host", 42).endObject());
        assertThat(dimensionGroup(doc), nullValue());
    }

    /** The default group field is an OTel resource attribute, which lives under a dynamic passthrough object. */
    public void testDefaultGroupFieldUnderPassthrough() throws Exception {
        Settings.Builder settings = getIndexSettingsBuilder().put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "resource.attributes.*");
        settings.remove(IndexSettings.TIME_SERIES_DIMENSION_GROUP_FIELD.getKey());
        DocumentMapper docMapper = createMapper(getVersion(), settings, mapping(b -> {
            b.startObject("resource").startObject("properties");
            b.startObject("attributes").field("type", "passthrough").field("priority", 1).field("time_series_dimension", true).endObject();
            b.endObject().endObject();
        }));
        ParsedDocument doc = parseDocument(
            docMapper,
            b -> b.startObject("resource")
                .startObject("attributes")
                .field("host.name", "host-a")
                .field("service.name", "svc")
                .endObject()
                .endObject()
        );
        assertThat(doc.rootDoc().getField("resource.attributes.host.name").binaryValue(), equalTo(new BytesRef("host-a")));
        assertThat(dimensionGroup(doc), equalTo(new BytesRef("host-a")));
    }

    public void testDisabledWithEmptyGroupField() throws Exception {
        Settings.Builder settings = getIndexSettingsBuilder().put(IndexSettings.TIME_SERIES_DIMENSION_GROUP_FIELD.getKey(), "");
        DocumentMapper docMapper = createMapper(getVersion(), settings, hostAndRegionMapping());
        assertThat(docMapper.metadataMapper(DimensionGroupFieldMapper.class), is(nullValue()));
        ParsedDocument doc = parseDocument(docMapper, b -> b.startObject("dim").field("host", "h1").endObject());
        assertThat(dimensionGroup(doc), nullValue());
    }

    public void testEnabledWithSyntheticId() throws Exception {
        Settings.Builder settings = getIndexSettingsBuilder().put(IndexSettings.SYNTHETIC_ID.getKey(), true);
        DocumentMapper docMapper = createMapper(getVersion(), settings, hostAndRegionMapping());
        assertThat(docMapper.metadataMapper(DimensionGroupFieldMapper.class), is(notNullValue()));
        ParsedDocument doc = parseDocument(docMapper, b -> b.startObject("dim").field("host", "h1").endObject());
        assertThat(dimensionGroup(doc), equalTo(new BytesRef("h1")));
    }

    /**
     * The sort keys ahead of the tsid have to be functions of the tsid, which keeps the documents of a tsid contiguous in a segment
     * for the synthetic _id lookup. So a group field that is not a dimension is not copied.
     */
    public void testGroupFieldNotADimension() throws Exception {
        Settings.Builder settings = getIndexSettingsBuilder().put(IndexSettings.TIME_SERIES_DIMENSION_GROUP_FIELD.getKey(), "label");
        DocumentMapper docMapper = createMapper(getVersion(), settings, mapping(b -> {
            b.startObject("dim").startObject("properties");
            b.startObject("host").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
            b.startObject("label").field("type", "keyword").endObject();
        }));
        assertThat(docMapper.metadataMapper(DimensionGroupFieldMapper.class), is(notNullValue()));
        ParsedDocument doc = parseDocument(docMapper, b -> b.startObject("dim").field("host", "h1").endObject().field("label", "l1"));
        assertThat(doc.rootDoc().getField("label"), is(notNullValue()));
        assertThat(dimensionGroup(doc), nullValue());
    }

    public void testDisabledBeforeVersion() throws Exception {
        IndexVersion version = IndexVersionUtils.randomVersionBetween(
            IndexVersions.TSID_SINGLE_PREFIX_BYTE,
            IndexVersionUtils.getPreviousVersion(IndexVersions.TIME_SERIES_DIMENSION_GROUP_SORT)
        );
        DocumentMapper docMapper = createMapper(version, getIndexSettingsBuilder(), hostAndRegionMapping());
        assertThat(docMapper.metadataMapper(DimensionGroupFieldMapper.class), is(nullValue()));
        ParsedDocument doc = parseDocument(docMapper, b -> b.startObject("dim").field("host", "h1").endObject());
        assertThat(dimensionGroup(doc), nullValue());
    }

    public void testDisabledInStandardMode() throws Exception {
        Settings.Builder builder = getIndexSettingsBuilder();
        builder.put(IndexSettings.MODE.getKey(), IndexMode.STANDARD.name());
        builder.remove(IndexMetadata.INDEX_ROUTING_PATH.getKey());
        DocumentMapper docMapper = createMapperService(builder.build(), mapping(b -> {})).documentMapper();
        assertThat(docMapper.metadataMapper(DimensionGroupFieldMapper.class), is(nullValue()));
    }

    public void testIncludeInDocumentNotAllowed() throws Exception {
        DocumentMapper docMapper = createMapper(hostAndRegionMapping());
        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> parseDocument(docMapper, b -> b.startObject("dim").field("host", "h1").endObject().field("_dimension_group", "foo"))
        );
        assertThat(
            e.getCause().getMessage(),
            containsString("Field [_dimension_group] is a metadata field and cannot be added inside a document")
        );
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A metadata fieldmapper that stores the source of a document as binary doc values.
 * This is a trade off for faster read performance in exchange for higher disk usage compared
 * to {@link org.elasticsearch.index.mapper.SourceFieldMapper}.
 *
 * This metadata field mapper is internal and meant to be only used by the enrich plugin.
 */
public final class EnrichSourceFieldMapper extends MetadataFieldMapper {

    static final String NAME = "_enrich_source";

    private static final MappedFieldType DEFAULT = new FieldType();
    private static final boolean DEFAULT_ENABLED = false;

    static class FieldType extends MappedFieldType {

        FieldType() {
            setName(NAME);
            setIndexOptions(IndexOptions.NONE);
            setStored(false);
            setHasDocValues(true);
            setDocValuesType(DocValuesType.BINARY);
        }

        FieldType(MappedFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new FieldType(this);
        }

        @Override
        public String typeName() {
            return NAME;
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            return DocValueFormat.BINARY;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new EnrichSourceIndexFieldData.Builder();
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            failIfNoDocValues();
            return new DocValuesFieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "[" + NAME + "] does not support searching");
        }

    }

    // This exists only to make sure that we can retrieve the source of documents from enrich indices via
    // the search api using doc value fields. This is useful for debugging purposes.
    static final class EnrichSourceIndexFieldData extends DocValuesIndexFieldData
        implements IndexFieldData<EnrichSourceIndexFieldData.AtomicFD> {

        EnrichSourceIndexFieldData(Index index, String fieldName) {
            super(index, fieldName);
        }

        @Override
        public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode,
                                   XFieldComparatorSource.Nested nested, boolean reverse) {
            throw new IllegalArgumentException("can't sort on binary field");
        }

        @Override
        public AtomicFD load(LeafReaderContext context) {
            try {
                return new AtomicFD(DocValues.getBinary(context.reader(), fieldName));
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public AtomicFD loadDirect(LeafReaderContext context) throws Exception {
            return load(context);
        }

        static class Builder implements IndexFieldData.Builder {

            @Override
            public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                                           CircuitBreakerService breakerService, MapperService mapperService) {
                // Ignore breaker
                final String fieldName = fieldType.name();
                return new EnrichSourceIndexFieldData(indexSettings.getIndex(), fieldName);
            }

        }

        static final class AtomicFD implements AtomicFieldData {

            private final BinaryDocValues values;

            AtomicFD(BinaryDocValues values) {
                super();
                this.values = values;
            }

            @Override
            public long ramBytesUsed() {
                return 0; // not exposed by Lucene
            }

            @Override
            public Collection<Accountable> getChildResources() {
                return Collections.emptyList();
            }

            @Override
            public SortedBinaryDocValues getBytesValues() {
                return FieldData.singleton(values);
            }

            @Override
            public ScriptDocValues<BytesRef> getScriptValues() {
                return new ScriptDocValues.BytesRefs(getBytesValues());
            }

            @Override
            public void close() {
                // no-op
            }

        }
    }

    static final class TypeParser implements MetadataFieldMapper.TypeParser {
        @Override
        public MetadataFieldMapper.Builder<?, ?> parse(String name,
                                                       Map<String, Object> node,
                                                       ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder();
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                if ("enabled".equals(entry.getKey())) {
                    builder.setEnabled(XContentMapValues.nodeBooleanValue(entry.getValue(), name + ".enabled"));
                    iterator.remove();
                }
            }
            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext parserContext) {
            Settings indexSettings = parserContext.mapperService().getIndexSettings().getSettings();
            return new EnrichSourceFieldMapper(indexSettings, DEFAULT, DEFAULT.clone(), false);
        }

    }

    static final class Builder extends MetadataFieldMapper.Builder<Builder, EnrichSourceFieldMapper> {

        private boolean enabled = false;

        Builder() {
            super(NAME, DEFAULT, DEFAULT.clone());
        }

        void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        @Override
        public EnrichSourceFieldMapper build(BuilderContext context) {
            if (enabled) {
                String indexName = context.indexSettings().get(IndexMetaData.SETTING_INDEX_PROVIDED_NAME);
                if (indexName.startsWith(EnrichPolicy.ENRICH_INDEX_NAME_BASE) == false) {
                    throw new IllegalArgumentException("only enrich indices can use the [" + NAME + "] meta field");
                }
            }
            setupFieldType(context);
            return new EnrichSourceFieldMapper(context.indexSettings(), fieldType, defaultFieldType, enabled);
        }
    }

    private EnrichSourceFieldMapper(Settings indexSettings, MappedFieldType fieldType, MappedFieldType defaultFieldType, boolean enabled) {
        super(NAME, fieldType, defaultFieldType, indexSettings);
        this.enabled = enabled;
    }

    private final boolean enabled;

    @Override
    public void preParse(ParseContext context) throws IOException {
        super.parse(context);
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        // nothing to do here, we will call it in pre parse
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        BytesReference source = context.sourceToParse().source();
        if (enabled && source != null) {
            XContentType contentType = context.sourceToParse().getXContentType();
            fields.add(createEnrichSourceField(source, contentType));
        }
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        if (includeDefaults || enabled != DEFAULT_ENABLED) {
            builder.startObject(contentType());
            builder.field("enabled", enabled);
            builder.endObject();
        }
        return builder;
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        EnrichSourceFieldMapper sourceMergeWith = (EnrichSourceFieldMapper) mergeWith;
        if (this.enabled != sourceMergeWith.enabled) {
            throw new IllegalArgumentException("Cannot update enabled setting for [_enrich_source]");
        }
    }

    static IndexableField createEnrichSourceField(BytesReference source, XContentType contentType) throws IOException {
        if (contentType != XContentType.SMILE) {
            throw new IllegalArgumentException("unsupported xcontent type [" + contentType + "], only SMILE is supported");
        }

        return new BinaryDocValuesField(EnrichSourceFieldMapper.NAME, source.toBytesRef());
    }
}

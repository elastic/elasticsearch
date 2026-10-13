/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.BatchMappingContext;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * The implicit {@code _unmapped} sink that absorbs unmapped fields on strict columnar indices as full dotted keys. It is a metadata field
 * so that its name is reserved like any other {@code _}-prefixed name: a user mapping named {@code _unmapped} fails the duplicate-name
 * check and a document key {@code _unmapped} is rejected by {@link MetadataFieldMapper#parseCreateField}. All flattened behavior comes
 * from a wrapped {@link FlattenedFieldMapper} built with no user parameters; the sink itself is never serialized.
 */
public final class UnmappedSinkFieldMapper extends MetadataFieldMapper {

    public static final TypeParser PARSER = new FixedTypeParser(
        c -> c.getIndexSettings().isFlattenedUnmappedFieldsEnabled() ? new UnmappedSinkFieldMapper(buildDelegate(c)) : null
    );

    private final FlattenedFieldMapper delegate;

    private UnmappedSinkFieldMapper(FlattenedFieldMapper delegate) {
        super(delegate.fieldType());
        this.delegate = delegate;
    }

    private static FlattenedFieldMapper buildDelegate(MappingParserContext context) {
        SourceFieldMapper.Mode sourceMode = context.getIndexSettings().getIndexMappingSourceMode();
        boolean isSourceSynthetic = sourceMode == SourceFieldMapper.Mode.SYNTHETIC || sourceMode == SourceFieldMapper.Mode.COLUMNAR_STORED;
        return (FlattenedFieldMapper) FlattenedFieldMapper.PARSER.parse(FlattenedFieldMapper.UNMAPPED_SINK_NAME, new HashMap<>(), context)
            .build(MapperBuilderContext.root(isSourceSynthetic, false));
    }

    @Override
    protected String contentType() {
        return FlattenedFieldMapper.UNMAPPED_SINK_NAME;
    }

    /**
     * {@code true} so that under {@code subobjects: false} a document object {@code {"_unmapped": {..}}} reaches
     * {@link MetadataFieldMapper#parseCreateField} and is rejected, rather than being flattened into {@code _unmapped.*} leaves.
     */
    @Override
    protected boolean supportsParsingObject() {
        return true;
    }

    /**
     * {@code true} so that a document array {@code {"_unmapped": [..]}} reaches {@link MetadataFieldMapper#parseCreateField} and is
     * rejected even when empty, instead of being silently accepted as an array with no elements to parse.
     */
    @Override
    public boolean parsesArrayValue() {
        return true;
    }

    /**
     * Absorbs the current parser value of the unmapped field {@code name} into the sink, keyed by its full dotted path.
     * See {@link FlattenedFieldMapper#indexValueAtPath}.
     */
    public static void absorb(DocumentParserContext context, String name) throws IOException {
        UnmappedSinkFieldMapper sink = context.mappingLookup().getMapping().getMetadataMapperByClass(UnmappedSinkFieldMapper.class);
        sink.delegate.indexValueAtPath(context, context.path().pathAsText(name));
    }

    /**
     * The sink takes part in the columnar batch path whenever its wrapped flattened mapper can batch the absorbed leaves. Its own
     * {@code doSupportsColumnarParse} is deliberately left at the inherited {@code false}: a document key {@code _unmapped} resolves to the
     * sink as a column of its own, and must fall back to the sequential parser, which rejects it, rather than be mapped as a column.
     */
    @Override
    public boolean supportsColumnarMetadataParse(IndexSettings indexSettings) {
        return delegate.supportsColumnarParse(indexSettings);
    }

    /**
     * Absorbed leaves are routed here explicitly by {@code ShardBatchMapper} under {@code dynamic: flattened}. {@code resolvesColumnGroup}
     * is deliberately left at the inherited {@code false}, so a document leaf under {@code _unmapped.} resolves as a conflict and falls
     * back to the sequential parser, which rejects it, instead of being silently written into the sink.
     */
    @Override
    public void mapColumnGroupBatch(BatchMappingContext ctx, EscfColumn[] columns, String[] relativeKeys) {
        delegate.mapColumnGroupBatch(ctx, columns, relativeKeys);
    }

    /**
     * Rejects every user field whose full path is under {@code _unmapped.}: the sink reserves its name as a prefix, in mappings as well as
     * in documents (see {@code DocumentParser.RootDocumentParserContext#getMapper}).
     */
    @Override
    protected void doValidate(MappingLookup lookup) {
        String prefix = fullPath() + ".";
        Iterator<? extends Mapper> mappers = Iterators.concat(lookup.fieldMappers().iterator(), lookup.objectMappers().values().iterator());
        while (mappers.hasNext()) {
            String path = mappers.next().fullPath();
            if (path.startsWith(prefix)) {
                throw new MapperParsingException("Field [" + path + "] is reserved by the metadata field [" + fullPath() + "]");
            }
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport.Native(delegate::syntheticFieldLoader);
    }
}

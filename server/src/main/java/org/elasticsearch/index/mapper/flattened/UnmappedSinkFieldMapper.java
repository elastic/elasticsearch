/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

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
        UnmappedSinkFieldMapper sink = (UnmappedSinkFieldMapper) context.mappingLookup().getMapper(FlattenedFieldMapper.UNMAPPED_SINK_NAME);
        sink.delegate.indexValueAtPath(context, context.path().pathAsText(name));
    }

    @Override
    protected boolean doSupportsColumnarParse(IndexSettings indexSettings) {
        // Flattened is a delegate and it supports columnar parse.
        return true;
    }

    /**
     * {@code false} although the sink consumes column groups: absorbed leaves are routed to it explicitly by {@code ShardBatchMapper}
     * under {@code dynamic: flattened}. Returning {@code false} makes a document leaf under {@code _unmapped.} resolve as a conflict and
     * fall back to the sequential parser, exactly as for any other metadata field, instead of being silently written into the sink.
     */
    @Override
    public boolean resolvesColumnGroup() {
        // TODO: the resolution to this depends on how we want to handle fields that share the _unmapped name
        // Context: without false here, we would a in _unmapped.a into the sink, meanwhile the sequential path currently rejects that.
        // The problem with absorbing a is that a could also be a mapped field, so during source synthesis, we'll have two instances of a.
        return false;
    }

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
        for (Mapper mapper : lookup.fieldMappers()) {
            rejectIfUnderSink(mapper, prefix);
        }
        for (Mapper mapper : lookup.objectMappers().values()) {
            rejectIfUnderSink(mapper, prefix);
        }
    }

    private void rejectIfUnderSink(Mapper mapper, String prefix) {
        if (mapper.fullPath().startsWith(prefix)) {
            throw new MapperParsingException("Field [" + mapper.fullPath() + "] is reserved by the metadata field [" + fullPath() + "]");
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport.Native(delegate::syntheticFieldLoader);
    }
}

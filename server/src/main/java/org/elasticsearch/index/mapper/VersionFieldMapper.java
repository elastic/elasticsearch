/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.VersionDocValuesField;
import org.elasticsearch.sourcebatch.MappedColumns;

import java.util.Collections;

/** Mapper for the _version field.
 *
 *  This is the field mapper for the monotonically increasing document version.  If you are looking for the field that stores semver style
 *  strings in a sortable binary format, you want VersionStringFieldMapper in the xpack VersionField plugin
 */
public class VersionFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_version";
    public static final String CONTENT_TYPE = "_version";

    public static final VersionFieldMapper INSTANCE = new VersionFieldMapper();

    public static final TypeParser PARSER = new FixedTypeParser(c -> INSTANCE);

    static final class VersionFieldType extends MappedFieldType {

        public static final VersionFieldType INSTANCE = new VersionFieldType();

        private VersionFieldType() {
            super(NAME, IndexType.docValuesOnly(), false, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isSearchable() {
            return false;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new QueryShardException(context, "The _version field is not searchable");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new DocValueFetcher(docValueFormat(format, null), context.getForField(this, FielddataOperation.SEARCH));
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            return new LongsBlockLoader(name());
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return new SortedNumericIndexFieldData.Builder(name(), NumericType.LONG, VersionDocValuesField::new, indexType);
        }
    }

    private VersionFieldMapper() {
        super(VersionFieldType.INSTANCE);
    }

    @Override
    public void preParse(DocumentParserContext context) {
        final Field version = versionField();
        context.version(version);
        context.doc().add(version);
    }

    public static Field versionField() {
        // see InternalEngine.updateVersion to see where the real version value is set
        return new NumericDocValuesField(NAME, -1L);
    }

    @Override
    public void postParse(DocumentParserContext context) {
        // In the case of nested docs, let's fill nested docs with version=1 so that Lucene doesn't write a Bitset for documents
        // that don't have the field. This is consistent with the default value for efficiency.
        Field version = context.version();
        assert version != null;
        for (LuceneDocument doc : context.nonRootDocuments()) {
            doc.add(version);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    private static final IndexableFieldType VERSION_COLUMN_FIELD_TYPE = buildVersionColumnFieldType();

    private static IndexableFieldType buildVersionColumnFieldType() {
        FieldType ft = new FieldType();
        ft.setDocValuesType(DocValuesType.NUMERIC);
        ft.freeze();
        return ft;
    }

    @Override
    public boolean supportsColumnarParse(IndexSettings indexSettings) {
        return true;
    }

    @Override
    public void preColumnarParse(BatchMappingContext context) {
        // TODO: Look at moving this and the above to postColumnarParse
        // Engine-assigned: register an array-backed column over the context's mutable version
        // array; the engine fills the real per-document value (see InternalEngine) after mapping,
        // just before requesting the ColumnBatch.
        context.addColumn(MappedColumns.longColumn(context.versions(), NAME, VERSION_COLUMN_FIELD_TYPE, LongColumn.NumericKind.LONG));
    }
}

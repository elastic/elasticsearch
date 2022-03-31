/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collections;
import java.util.List;

/**
 * A mapper that indexes the field names of a document under <code>_field_names</code>. This mapper is typically useful in order
 * to have fast <code>exists</code> and <code>missing</code> queries/filters.
 *
 * Added in Elasticsearch 1.3.
 */
public class FieldNamesFieldMapper extends MetadataFieldMapper {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(FieldNamesFieldMapper.class);

    public static final String NAME = "_field_names";

    public static final String CONTENT_TYPE = "_field_names";

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(createdOnOrAfterV8).init(this);
    }

    public static class Defaults {
        public static final String NAME = FieldNamesFieldMapper.NAME;

        public static final Explicit<Boolean> ENABLED = Explicit.IMPLICIT_TRUE;
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    private static FieldNamesFieldMapper toType(FieldMapper in) {
        return (FieldNamesFieldMapper) in;
    }

    public static final String ENABLED_DEPRECATION_MESSAGE =
        "Disabling _field_names is not necessary because it no longer carries a large index overhead. Support for the `enabled` "
            + "setting will be removed in a future major version. Please remove it from your mappings and templates.";

    static class Builder extends MetadataFieldMapper.Builder {

        private final Parameter<Explicit<Boolean>> enabled = updateableBoolParam(
            "enabled",
            m -> toType(m).enabled,
            Defaults.ENABLED.value()
        );

        private final boolean createdOnOrAfterV8;

        Builder(Version indexVersionCreated) {
            this(indexVersionCreated.onOrAfter(Version.V_8_0_0));
        }

        Builder(boolean createdOnOrAfterV8) {
            super(Defaults.NAME);
            this.createdOnOrAfterV8 = createdOnOrAfterV8;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(enabled);
        }

        @Override
        public FieldNamesFieldMapper build() {
            if (enabled.getValue().explicit()) {
                if (createdOnOrAfterV8) {
                    throw new MapperParsingException(
                        "The `enabled` setting for the `_field_names` field has been deprecated and "
                            + "removed. Please remove it from your mappings and templates."
                    );
                } else {
                    deprecationLogger.warn(DeprecationCategory.TEMPLATES, "field_names_enabled_parameter", ENABLED_DEPRECATION_MESSAGE);
                }
            }
            if (Defaults.ENABLED.equals(enabled.getValue())) {
                return createdOnOrAfterV8 ? DEFAULT : DEFAULT_OLD;
            }
            return new FieldNamesFieldMapper(enabled.getValue(), createdOnOrAfterV8);
        }
    }

    private static final FieldNamesFieldMapper DEFAULT = new FieldNamesFieldMapper(Defaults.ENABLED, true);

    private static final FieldNamesFieldMapper DEFAULT_OLD = new FieldNamesFieldMapper(Defaults.ENABLED, false);

    public static final TypeParser PARSER = new ConfigurableTypeParser(
        c -> c.indexVersionCreated().onOrAfter(Version.V_8_0_0) ? DEFAULT : DEFAULT_OLD,
        c -> new Builder(c.indexVersionCreated())
    );

    public static final class FieldNamesFieldType extends TermBasedFieldType {

        private static final FieldNamesFieldType ENABLED = new FieldNamesFieldType(true);

        private static final FieldNamesFieldType DISABLED = new FieldNamesFieldType(false);

        private final boolean enabled;

        public static FieldNamesFieldType get(boolean enabled) {
            return enabled ? ENABLED : DISABLED;
        }

        private FieldNamesFieldType(boolean enabled) {
            super(Defaults.NAME, true, false, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
            this.enabled = enabled;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public boolean isEnabled() {
            return enabled;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            throw new UnsupportedOperationException("Cannot run exists query on _field_names");
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            if (isEnabled() == false) {
                throw new IllegalStateException("Cannot run [exists] queries if the [_field_names] field is disabled");
            }
            deprecationLogger.warn(
                DeprecationCategory.MAPPINGS,
                "terms_query_on_field_names",
                "terms query on the _field_names field is deprecated and will be removed, use exists query instead"
            );
            return super.termQuery(value, context);
        }
    }

    private final Explicit<Boolean> enabled;
    private final boolean createdOnOrAfterV8;

    private FieldNamesFieldMapper(Explicit<Boolean> enabled, boolean createdOnOrAfterV8) {
        super(FieldNamesFieldType.get(enabled.value()));
        this.enabled = enabled;
        this.createdOnOrAfterV8 = createdOnOrAfterV8;
    }

    @Override
    public FieldNamesFieldType fieldType() {
        return (FieldNamesFieldType) super.fieldType();
    }

    public void addFieldNames(DocumentParserContext context, String field) {
        if (enabled.value() == false) {
            return;
        }
        assert noDocValues(field, context) : "Field " + field + " should not have docvalues";
        context.doc().add(new Field(NAME, field, Defaults.FIELD_TYPE));
    }

    private static boolean noDocValues(String field, DocumentParserContext context) {
        MappedFieldType ft = context.mappingLookup().getFieldType(field);
        return ft == null || ft.hasDocValues() == false;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
